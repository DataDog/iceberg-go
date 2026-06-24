// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package engine

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table/internal"
	"github.com/DataDog/iceberg-go/table/substrait"
	tbl "github.com/DataDog/iceberg-go/table"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// AppendTable appends the rows of an Arrow table to the transaction.
func AppendTable(ctx context.Context, t *Transaction, table arrow.Table, batchSize int64, snapshotProps iceberg.Properties) error {
	rdr := array.NewTableReader(table, batchSize)
	defer rdr.Release()

	return Append(ctx, t, rdr, snapshotProps)
}

// Append appends the records from rdr to the transaction.
func Append(ctx context.Context, t *Transaction, rdr array.RecordReader, snapshotProps iceberg.Properties) error {
	fs, err := t.TableFS(ctx)
	if err != nil {
		return err
	}
	appendFiles := t.AppendProducer(fs, snapshotProps)
	commitUUID := appendFiles.CommitUUID()
	itr := recordsToDataFiles(ctx, t.Table().Location(), t.CurrentMeta(), recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        fs.(iceio.WriteFileIO),
		writeUUID: &commitUUID,
	})

	for df, err := range itr {
		if err != nil {
			return err
		}
		appendFiles.AppendDataFile(df)
	}

	updates, reqs, err := appendFiles.Commit(ctx)
	if err != nil {
		return err
	}

	return t.ApplyUpdates(updates, reqs)
}

// ReplaceDataFiles replaces the data files at the given paths with new files.
func ReplaceDataFiles(ctx context.Context, t *Transaction, filesToDelete, filesToAdd []string, snapshotProps iceberg.Properties) error {
	if len(filesToDelete) == 0 {
		if len(filesToAdd) > 0 {
			return AddFiles(ctx, t, filesToAdd, snapshotProps, false)
		}
	}

	var (
		setToDelete = make(map[string]struct{})
		setToAdd    = make(map[string]struct{})
	)

	for _, f := range filesToDelete {
		setToDelete[f] = struct{}{}
	}
	for _, f := range filesToAdd {
		setToAdd[f] = struct{}{}
	}

	if len(setToDelete) != len(filesToDelete) {
		return errors.New("delete file paths must be unique for ReplaceDataFiles")
	}
	if len(setToAdd) != len(filesToAdd) {
		return errors.New("add file paths must be unique for ReplaceDataFiles")
	}

	meta := t.CurrentMeta()
	s := meta.CurrentSnapshotBuilding()
	if s == nil {
		return fmt.Errorf("%w: cannot replace files in a table without an existing snapshot", tbl.ErrInvalidOperation)
	}

	fs, err := t.TableFS(ctx)
	if err != nil {
		return err
	}
	markedForDeletion := make([]iceberg.DataFile, 0, len(setToDelete))
	for df, err := range s.AllDataFiles(fs) {
		if err != nil {
			return err
		}
		if _, ok := setToDelete[df.FilePath()]; ok {
			markedForDeletion = append(markedForDeletion, df)
		}
		if _, ok := setToAdd[df.FilePath()]; ok {
			return fmt.Errorf("cannot add files that are already referenced by table, files: %s", df.FilePath())
		}
	}

	if len(markedForDeletion) != len(setToDelete) {
		return errors.New("cannot delete files that do not belong to the table")
	}

	if err := t.EnsureNameMapping(); err != nil {
		return err
	}

	commitUUID := uuid.New()
	updater := t.OverwriteProducer(fs, snapshotProps, OpOverwrite, &commitUUID, nil)

	for _, df := range markedForDeletion {
		updater.DeleteDataFile(df)
	}

	dataFiles, err := filesToDataFiles(ctx, fs, meta, filesToAdd, 1)
	if err != nil {
		return err
	}
	for _, dataFile := range dataFiles {
		updater.AppendDataFile(dataFile)
	}

	updates, reqs, err := updater.Commit(ctx)
	if err != nil {
		return err
	}

	return t.ApplyUpdates(updates, reqs)
}

// AddFilesOption configures AddFiles.
type AddFilesOption func(addFilesOp *addFilesOperation)

type addFilesOperation struct {
	concurrency int
}

// WithAddFilesConcurrency overrides the default concurrency for AddFiles.
func WithAddFilesConcurrency(concurrency int) AddFilesOption {
	return func(op *addFilesOperation) {
		if concurrency > 0 {
			op.concurrency = concurrency
		}
	}
}

// AddFiles scans the given file paths from storage and adds them to the table.
func AddFiles(ctx context.Context, t *Transaction, filePaths []string, snapshotProps iceberg.Properties, ignoreDuplicates bool, opts ...AddFilesOption) error {
	addFilesOp := addFilesOperation{concurrency: runtime.GOMAXPROCS(0)}
	for _, apply := range opts {
		apply(&addFilesOp)
	}

	set := make(map[string]struct{}, len(filePaths))
	for _, filePath := range filePaths {
		if _, ok := set[filePath]; ok {
			return errors.New("file paths must be unique for AddFiles")
		}
		set[filePath] = struct{}{}
	}

	meta := t.CurrentMeta()
	if !ignoreDuplicates {
		if s := meta.CurrentSnapshotBuilding(); s != nil {
			referenced := make([]string, 0)
			fs, err := t.TableFS(ctx)
			if err != nil {
				return err
			}
			for df, err := range s.AllDataFiles(fs) {
				if err != nil {
					return err
				}
				if _, ok := set[df.FilePath()]; ok {
					referenced = append(referenced, df.FilePath())
				}
			}
			if len(referenced) > 0 {
				return fmt.Errorf("cannot add files that are already referenced by table, files: %v", referenced)
			}
		}
	}

	if err := t.EnsureNameMapping(); err != nil {
		return err
	}

	fs, err := t.TableFS(ctx)
	if err != nil {
		return err
	}

	updater := t.AppendProducer(fs, snapshotProps)

	dataFiles, err := filesToDataFiles(ctx, fs, meta, filePaths, addFilesOp.concurrency)
	if err != nil {
		return err
	}
	for _, dataFile := range dataFiles {
		updater.AppendDataFile(dataFile)
	}

	updates, reqs, err := updater.Commit(ctx)
	if err != nil {
		return err
	}

	return t.ApplyUpdates(updates, reqs)
}

type overwriteOperation struct {
	concurrency   int
	filter        iceberg.BooleanExpression
	caseSensitive bool
}

// OverwriteOption applies options to overwrite operations.
type OverwriteOption func(op *overwriteOperation)

// WithOverwriteConcurrency overrides the default overwrite concurrency.
func WithOverwriteConcurrency(concurrency int) OverwriteOption {
	return func(op *overwriteOperation) {
		if concurrency <= 0 {
			op.concurrency = runtime.GOMAXPROCS(0)

			return
		}
		op.concurrency = concurrency
	}
}

// WithOverwriteFilter overrides the default deletion filter on overwrite.
func WithOverwriteFilter(filter iceberg.BooleanExpression) OverwriteOption {
	return func(op *overwriteOperation) {
		op.filter = filter
	}
}

// WithOverwriteCaseInsensitive binds the filter case-insensitively.
func WithOverwriteCaseInsensitive() OverwriteOption {
	return func(op *overwriteOperation) {
		op.caseSensitive = false
	}
}

// OverwriteTable overwrites the table data using an Arrow table.
func OverwriteTable(ctx context.Context, t *Transaction, table arrow.Table, batchSize int64, snapshotProps iceberg.Properties, opts ...OverwriteOption) error {
	rdr := array.NewTableReader(table, batchSize)
	defer rdr.Release()

	return Overwrite(ctx, t, rdr, snapshotProps, opts...)
}

// Overwrite overwrites the table data using a RecordReader.
func Overwrite(ctx context.Context, t *Transaction, rdr array.RecordReader, snapshotProps iceberg.Properties, opts ...OverwriteOption) error {
	overwrite := overwriteOperation{
		concurrency:   runtime.GOMAXPROCS(0),
		filter:        iceberg.AlwaysTrue{},
		caseSensitive: true,
	}
	for _, apply := range opts {
		apply(&overwrite)
	}

	updater, err := performCopyOnWriteDeletion(ctx, t, OpOverwrite, snapshotProps, overwrite.filter, overwrite.caseSensitive, overwrite.concurrency)
	if err != nil {
		return err
	}

	fs, err := t.TableFS(ctx)
	if err != nil {
		return err
	}
	commitUUID := updater.CommitUUID()
	itr := recordsToDataFiles(ctx, t.Table().Location(), t.CurrentMeta(), recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        fs.(iceio.WriteFileIO),
		writeUUID: &commitUUID,
	})

	for df, err := range itr {
		if err != nil {
			return err
		}
		updater.AppendDataFile(df)
	}

	var addedRows, deletedRows int64
	for _, df := range updater.AddedFiles() {
		addedRows += df.Count()
	}
	for _, df := range updater.DeletedFiles() {
		deletedRows += df.Count()
	}
	if deletedRows > 0 && addedRows < deletedRows {
		slog.Warn("Overwrite produced fewer rows than deleted",
			"added_rows", addedRows,
			"deleted_rows", deletedRows,
			"delta", addedRows-deletedRows)
	}

	updates, reqs, err := updater.Commit(ctx)
	if err != nil {
		return err
	}

	return t.ApplyUpdates(updates, reqs)
}

func performCopyOnWriteDeletion(ctx context.Context, t *Transaction, operation Operation, snapshotProps iceberg.Properties, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (*SnapshotProducer, error) {
	fs, err := t.TableFS(ctx)
	if err != nil {
		return nil, err
	}

	if err := t.EnsureNameMapping(); err != nil {
		return nil, err
	}

	commitUUID := uuid.New()
	updater := t.OverwriteProducer(fs, snapshotProps, operation, &commitUUID, filter)

	filesToDelete, filesToRewrite, err := classifyFilesForDeletions(ctx, t, fs, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, err
	}

	for _, df := range filesToDelete {
		updater.DeleteDataFile(df)
	}

	if len(filesToRewrite) > 0 {
		if err := rewriteFilesWithFilter(ctx, t, fs, updater, filesToRewrite, filter, caseSensitive, concurrency); err != nil {
			return nil, err
		}
	}

	return updater, nil
}

func performMergeOnReadDeletion(ctx context.Context, t *Transaction, snapshotProps iceberg.Properties, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (*SnapshotProducer, error) {
	fs, err := t.TableFS(ctx)
	if err != nil {
		return nil, err
	}

	if err := t.EnsureNameMapping(); err != nil {
		return nil, err
	}

	commitUUID := uuid.New()
	updater := t.OverwriteProducer(fs, snapshotProps, OpDelete, &commitUUID, filter)

	filesToDelete, withPartialDeletions, err := classifyFilesForDeletions(ctx, t, fs, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, err
	}

	for _, df := range filesToDelete {
		updater.DeleteDataFile(df)
	}

	if len(withPartialDeletions) > 0 {
		if err := writePositionDeletesForFiles(ctx, t, fs, updater, withPartialDeletions, filter, caseSensitive, concurrency, commitUUID); err != nil {
			return nil, err
		}
	}

	return updater, nil
}

type deleteOperation struct {
	caseSensitive bool
	concurrency   int
}

// DeleteOption applies options to delete operations.
type DeleteOption func(deleteOp *deleteOperation)

// WithDeleteConcurrency overrides the default delete concurrency.
func WithDeleteConcurrency(concurrency int) DeleteOption {
	return func(op *deleteOperation) {
		if concurrency <= 0 {
			op.concurrency = runtime.GOMAXPROCS(0)

			return
		}
		op.concurrency = concurrency
	}
}

// WithDeleteCaseInsensitive binds the filter case-insensitively.
func WithDeleteCaseInsensitive() DeleteOption {
	return func(deleteOp *deleteOperation) {
		deleteOp.caseSensitive = false
	}
}

// Delete deletes records matching the provided filter.
func Delete(ctx context.Context, t *Transaction, filter iceberg.BooleanExpression, snapshotProps iceberg.Properties, opts ...DeleteOption) (err error) {
	deleteOp := deleteOperation{
		concurrency:   runtime.GOMAXPROCS(0),
		caseSensitive: true,
	}
	for _, apply := range opts {
		apply(&deleteOp)
	}

	meta := t.CurrentMeta()
	var updater *SnapshotProducer
	writeDeleteMode := WriteDeleteModeDefault
	if meta.FormatVersion() > 1 {
		writeDeleteMode = meta.Props().Get(WriteDeleteModeKey, WriteDeleteModeDefault)
	}
	switch writeDeleteMode {
	case WriteModeCopyOnWrite:
		updater, err = performCopyOnWriteDeletion(ctx, t, OpDelete, snapshotProps, filter, deleteOp.caseSensitive, deleteOp.concurrency)
		if err != nil {
			return err
		}
	case WriteModeMergeOnRead:
		updater, err = performMergeOnReadDeletion(ctx, t, snapshotProps, filter, deleteOp.caseSensitive, deleteOp.concurrency)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported write mode: '%s'", writeDeleteMode)
	}

	updates, reqs, err := updater.Commit(ctx)
	if err != nil {
		return err
	}

	return t.ApplyUpdates(updates, reqs)
}

// classifyFilesForDeletions classifies existing data files based on the filter.
func classifyFilesForDeletions(ctx context.Context, t *Transaction, fs iceio.IO, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (filesToDelete, filesWithPartialDeletions []iceberg.DataFile, err error) {
	meta := t.CurrentMeta()
	s := meta.CurrentSnapshotBuilding()
	if s == nil {
		return nil, nil, nil
	}

	if filter == nil || filter.Equals(iceberg.AlwaysTrue{}) {
		for df, err := range s.AllDataFiles(fs) {
			if err != nil {
				return nil, nil, err
			}
			if df.ContentType() == iceberg.EntryContentData {
				filesToDelete = append(filesToDelete, df)
			}
		}

		return filesToDelete, filesWithPartialDeletions, nil
	}

	return classifyFilesForFilteredDeletions(ctx, t, fs, filter, caseSensitive, concurrency)
}

func classifyFilesForFilteredDeletions(ctx context.Context, t *Transaction, fs iceio.IO, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (filesToDelete, filesWithPartialDeletes []iceberg.DataFile, err error) {
	builder := t.CurrentMeta()
	schema := builder.CurrentSchema()
	meta, err := builder.Build()
	if err != nil {
		return nil, nil, err
	}

	inclusiveEvaluator, err := tbl.NewInclusiveMetricsEvaluator(schema, filter, caseSensitive, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create inclusive metrics evaluator: %w", err)
	}

	strictEvaluator, err := tbl.NewStrictMetricsEvaluator(schema, filter, caseSensitive, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create strict metrics evaluator: %w", err)
	}

	var (
		mu               sync.Mutex
		manifestEvalsMu  sync.Mutex
		manifestEvalsMap = map[int]func(iceberg.ManifestFile) (bool, error){}
	)
	getManifestEval := func(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
		manifestEvalsMu.Lock()
		defer manifestEvalsMu.Unlock()
		if e, ok := manifestEvalsMap[specID]; ok {
			return e, nil
		}
		e, err := tbl.BuildManifestEvaluatorForSpec(specID, meta, filter, caseSensitive)
		if err != nil {
			return nil, err
		}
		manifestEvalsMap[specID] = e

		return e, nil
	}

	s := builder.CurrentSnapshotBuilding()
	var manifests []iceberg.ManifestFile
	if s != nil {
		manifests, err = s.Manifests(fs)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get manifests: %w", err)
		}
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(min(concurrency, max(1, len(manifests))))

	for _, manifest := range manifests {
		manifest := manifest
		g.Go(func() error {
			manifestEval, err := getManifestEval(int(manifest.PartitionSpecID()))
			if err != nil {
				return err
			}
			if manifestEval != nil {
				match, err := manifestEval(manifest)
				if err != nil {
					return fmt.Errorf("failed to evaluate manifest %s: %w", manifest.FilePath(), err)
				}
				if !match {
					return nil
				}
			}

			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				return fmt.Errorf("failed to fetch manifest entries: %w", err)
			}

			localDelete := make([]iceberg.DataFile, 0)
			localRewrite := make([]iceberg.DataFile, 0)

			for _, entry := range entries {
				if entry.Status() == iceberg.EntryStatusDELETED {
					continue
				}

				df := entry.DataFile()
				if df.ContentType() != iceberg.EntryContentData {
					continue
				}

				inclusive, err := inclusiveEvaluator(df)
				if err != nil {
					return fmt.Errorf("failed to evaluate data file %s with inclusive evaluator: %w", df.FilePath(), err)
				}
				if !inclusive {
					continue
				}

				strict, err := strictEvaluator(df)
				if err != nil {
					return fmt.Errorf("failed to evaluate data file %s with strict evaluator: %w", df.FilePath(), err)
				}

				if strict {
					localDelete = append(localDelete, df)
				} else {
					localRewrite = append(localRewrite, df)
				}
			}

			if len(localDelete) > 0 || len(localRewrite) > 0 {
				mu.Lock()
				filesToDelete = append(filesToDelete, localDelete...)
				filesWithPartialDeletes = append(filesWithPartialDeletes, localRewrite...)
				mu.Unlock()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return filesToDelete, filesWithPartialDeletes, nil
}

func rewriteFilesWithFilter(ctx context.Context, t *Transaction, fs iceio.IO, updater *SnapshotProducer, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) error {
	complementFilter := iceberg.NewNot(filter)

	for _, originalFile := range files {
		rewriteUUID := uuid.New()
		rewrittenFiles, err := rewriteSingleFile(ctx, t, fs, originalFile, complementFilter, caseSensitive, rewriteUUID, concurrency)
		if err != nil {
			return fmt.Errorf("failed to rewrite file %s: %w", originalFile.FilePath(), err)
		}

		updater.DeleteDataFile(originalFile)
		for _, rewrittenFile := range rewrittenFiles {
			updater.AppendDataFile(rewrittenFile)
		}
	}

	return nil
}

func rewriteSingleFile(ctx context.Context, t *Transaction, fs iceio.IO, originalFile iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, commitUUID uuid.UUID, concurrency int) ([]iceberg.DataFile, error) {
	builder := t.CurrentMeta()
	scanTask := &FileScanTask{
		File:   originalFile,
		Start:  0,
		Length: originalFile.FileSizeBytes(),
	}

	boundFilter, err := iceberg.BindExpr(builder.CurrentSchema(), filter, caseSensitive)
	if err != nil {
		return nil, fmt.Errorf("failed to bind filter: %w", err)
	}

	meta, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build metadata: %w", err)
	}

	scanner := &arrowScan{
		metadata:        meta,
		fs:              fs,
		projectedSchema: builder.CurrentSchema(),
		boundRowFilter:  boundFilter,
		caseSensitive:   caseSensitive,
		rowLimit:        -1,
		concurrency:     concurrency,
	}

	arrowSchema, recordIter, err := scanner.GetRecords(ctx, []FileScanTask{*scanTask})
	if err != nil {
		return nil, fmt.Errorf("failed to get records from original file: %w", err)
	}

	releaseIter := func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range recordIter {
			if err != nil {
				yield(nil, err)

				return
			}
			if !yield(rec, nil) {
				rec.Release()

				return
			}
			rec.Release()
		}
	}

	var result []iceberg.DataFile
	itr := recordsToDataFiles(ctx, t.Table().Location(), builder, recordWritingArgs{
		sc:        arrowSchema,
		itr:       releaseIter,
		fs:        fs.(iceio.WriteFileIO),
		writeUUID: &commitUUID,
	})

	for df, err := range itr {
		if err != nil {
			return nil, err
		}
		result = append(result, df)
	}

	return result, nil
}

func writePositionDeletesForFiles(ctx context.Context, t *Transaction, fs iceio.IO, updater *SnapshotProducer, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int, commitUUID uuid.UUID) error {
	posDeleteRecIter, err := makePositionDeleteRecordsForFilter(ctx, t, fs, files, filter, caseSensitive, concurrency)
	if err != nil {
		return err
	}

	partitionContextByFilePath := make(map[string]partitionContext, len(files))
	for _, df := range files {
		partitionContextByFilePath[df.FilePath()] = partitionContext{partitionData: df.Partition(), specID: df.SpecID()}
	}

	posDeleteFiles := positionDeleteRecordsToDataFiles(ctx, t.Table().Location(), t.CurrentMeta(), partitionContextByFilePath, recordWritingArgs{
		sc:        PositionalDeleteArrowSchema,
		itr:       posDeleteRecIter,
		writeUUID: &commitUUID,
		fs:        fs.(iceio.WriteFileIO),
	})

	for f, err := range posDeleteFiles {
		if err != nil {
			return err
		}
		updater.AppendDeleteFile(f)
	}

	return nil
}

func makePositionDeleteRecordsForFilter(ctx context.Context, t *Transaction, fs iceio.IO, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (seq2 iter.Seq2[arrow.RecordBatch, error], err error) {
	builder := t.CurrentMeta()
	tasks := make([]FileScanTask, 0, len(files))
	for _, f := range files {
		tasks = append(tasks, FileScanTask{
			File:   f,
			Start:  0,
			Length: f.FileSizeBytes(),
		})
	}

	boundFilter, err := iceberg.BindExpr(builder.CurrentSchema(), filter, caseSensitive)
	if err != nil {
		return nil, fmt.Errorf("failed to bind filter: %w", err)
	}

	meta, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build metadata: %w", err)
	}

	scanner := &arrowScan{
		metadata:        meta,
		fs:              fs,
		projectedSchema: builder.CurrentSchema(),
		boundRowFilter:  boundFilter,
		caseSensitive:   caseSensitive,
		rowLimit:        -1,
		concurrency:     concurrency,
	}

	deletesPerFile, err := readAllDeleteFiles(ctx, fs, tasks, concurrency)
	if err != nil {
		return nil, err
	}

	extSet := substrait.NewExtensionSet()

	ctx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan internal.Enumerated[FileScanTask], len(tasks))

	numWorkers := min(concurrency, len(tasks))
	records := make(chan enumeratedRecord, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}

					if err := scanner.producePosDeletesFromTask(ctx, task, deletesPerFile[task.Value.File.FilePath()], records); err != nil {
						cancel(err)

						return
					}
				}
			}
		}()
	}

	go func() {
		for i, t := range tasks {
			taskChan <- internal.Enumerated[FileScanTask]{
				Value: t, Index: i, Last: i == len(tasks)-1,
			}
		}
		close(taskChan)

		wg.Wait()
		close(records)
	}()

	return createIterator(ctx, uint(numWorkers), records, deletesPerFile, cancel, scanner.rowLimit), nil
}
