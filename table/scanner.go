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

package table

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/io"
	"golang.org/x/sync/errgroup"
)

const ScanNoLimit = -1

// set is a generic set built on a map. It is shared by the scanner,
// snapshot file iteration, and (in the engine package) the arrow readers.
type set[T comparable] map[T]struct{}

type keyDefaultMap[K comparable, V any] struct {
	defaultFactory func(K) V
	data           map[K]V

	mx sync.RWMutex
}

func (k *keyDefaultMap[K, V]) Get(key K) V {
	k.mx.RLock()
	if v, ok := k.data[key]; ok {
		k.mx.RUnlock()

		return v
	}

	k.mx.RUnlock()
	k.mx.Lock()
	defer k.mx.Unlock()

	// race check between RLock and Lock
	if v, ok := k.data[key]; ok {
		return v
	}

	v := k.defaultFactory(key)
	k.data[key] = v

	return v
}

func newKeyDefaultMap[K comparable, V any](factory func(K) V) *keyDefaultMap[K, V] {
	return &keyDefaultMap[K, V]{
		data:           make(map[K]V),
		defaultFactory: factory,
	}
}

func newKeyDefaultMapWrapErr[K comparable, V any](factory func(K) (V, error)) *keyDefaultMap[K, V] {
	return &keyDefaultMap[K, V]{
		data: make(map[K]V),
		defaultFactory: func(k K) V {
			v, err := factory(k)
			if err != nil {
				panic(err)
			}

			return v
		},
	}
}

type partitionRecord []any

func (p partitionRecord) Size() int            { return len(p) }
func (p partitionRecord) Get(pos int) any      { return p[pos] }
func (p partitionRecord) Set(pos int, val any) { p[pos] = val }

// manifestEntries holds the data, positional delete, and equality delete
// entries read from manifests.
type manifestEntries struct {
	dataEntries             []iceberg.ManifestEntry
	positionalDeleteEntries []iceberg.ManifestEntry
	equalityDeleteEntries   []iceberg.ManifestEntry
	dvEntries               []iceberg.ManifestEntry
	mu                      sync.Mutex
}

func newManifestEntries() *manifestEntries {
	return &manifestEntries{
		dataEntries:             make([]iceberg.ManifestEntry, 0),
		positionalDeleteEntries: make([]iceberg.ManifestEntry, 0),
		equalityDeleteEntries:   make([]iceberg.ManifestEntry, 0),
		dvEntries:               make([]iceberg.ManifestEntry, 0),
	}
}

func (m *manifestEntries) addDataEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataEntries = append(m.dataEntries, e)
}

func (m *manifestEntries) addPositionalDeleteEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.positionalDeleteEntries = append(m.positionalDeleteEntries, e)
}

func (m *manifestEntries) addEqualityDeleteEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.equalityDeleteEntries = append(m.equalityDeleteEntries, e)
}

func (m *manifestEntries) addDVEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dvEntries = append(m.dvEntries, e)
}

func newPartitionRecord(partitionData map[int]any, partitionType *iceberg.StructType) partitionRecord {
	out := make(partitionRecord, len(partitionType.FieldList))
	for i, f := range partitionType.FieldList {
		out[i] = partitionData[f.ID]
	}

	return out
}

// GetPartitionRecord converts a DataFile's partition map into a positional
// record ordered by the fields of the given partition struct type.
func GetPartitionRecord(dataFile iceberg.DataFile, partitionType *iceberg.StructType) iceberg.StructLike {
	return newPartitionRecord(dataFile.Partition(), partitionType)
}

func openManifest(io io.IO, manifest iceberg.ManifestFile,
	partitionFilter, metricsEval func(iceberg.DataFile) (bool, error),
) ([]iceberg.ManifestEntry, error) {
	entries, err := manifest.FetchEntries(io, true)
	if err != nil {
		return nil, err
	}

	out := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		p, err := partitionFilter(entry.DataFile())
		if err != nil {
			return nil, err
		}

		m, err := metricsEval(entry.DataFile())
		if err != nil {
			return nil, err
		}

		if p && m {
			out = append(out, entry)
		}
	}

	return out, nil
}

func isDeletionVector(df iceberg.DataFile) bool {
	return df.ReferencedDataFile() != nil
}

type Scan struct {
	metadata       Metadata
	ioF            FSysF
	rowFilter      iceberg.BooleanExpression
	selectedFields []string
	caseSensitive  bool
	snapshotID     *int64
	asOfTimestamp  *int64
	options        iceberg.Properties
	limit          int64

	partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression]
	concurrency      int
}

func (scan *Scan) UseRowLimit(n int64) *Scan {
	out := *scan
	out.limit = n

	return &out
}

func (scan *Scan) UseRef(name string) (*Scan, error) {
	if scan.snapshotID != nil {
		return nil, fmt.Errorf("%w: cannot override ref, already set snapshot id %d",
			iceberg.ErrInvalidArgument, *scan.snapshotID)
	}

	if snap := scan.metadata.SnapshotByName(name); snap != nil {
		out := *scan
		out.snapshotID = &snap.SnapshotID
		out.partitionFilters = newKeyDefaultMapWrapErr(out.buildPartitionProjection)

		return &out, nil
	}

	return nil, fmt.Errorf("%w: cannot scan unknown ref=%s", iceberg.ErrInvalidArgument, name)
}

func (scan *Scan) Snapshot() *Snapshot {
	if scan.snapshotID != nil {
		return scan.metadata.SnapshotByID(*scan.snapshotID)
	}

	if scan.asOfTimestamp != nil {
		entries := slices.Collect(scan.metadata.SnapshotLogs())
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if entry.TimestampMs <= *scan.asOfTimestamp {
				return scan.metadata.SnapshotByID(entry.SnapshotID)
			}
		}
	}

	return scan.metadata.CurrentSnapshot()
}

func (scan *Scan) Projection() (*iceberg.Schema, error) {
	curSchema := scan.metadata.CurrentSchema()
	if scan.snapshotID != nil {
		snap := scan.metadata.SnapshotByID(*scan.snapshotID)
		if snap == nil {
			return nil, fmt.Errorf("%w: snapshot not found: %d", ErrInvalidOperation, *scan.snapshotID)
		}

		if snap.SchemaID != nil {
			for _, schema := range scan.metadata.Schemas() {
				if schema.ID == *snap.SchemaID {
					curSchema = schema

					break
				}
			}
		}
	}

	if slices.Contains(scan.selectedFields, "*") {
		return curSchema, nil
	}

	caseSensitive := scan.caseSensitive
	curVersion := scan.metadata.Version()
	selectedFieldsMeta := metaFieldsFromSelectedFields(scan.selectedFields, caseSensitive)
	schemaMeta := metaFieldsFromSchema(curSchema)
	synthesisMeta := synthesizeMeta(selectedFieldsMeta, schemaMeta)
	if len(synthesisMeta) > 0 && curVersion >= minFormatVersionRowLineage {
		removedMetaSlice, missingMetaFields := removeMetadataFromSelectedFields(scan.selectedFields, synthesisMeta)
		sch, err := curSchema.Select(caseSensitive, removedMetaSlice...)
		if err != nil {
			return nil, err
		}

		return iceberg.NewSchemaWithIdentifiers(sch.ID, sch.IdentifierFieldIDs, append(sch.Fields(), missingMetaFields...)...), nil
	}

	return curSchema.Select(scan.caseSensitive, scan.selectedFields...)
}

func removeMetadataFromSelectedFields(selectedFields []string, metaFields []string) ([]string, []iceberg.NestedField) {
	filteredFields := []string{}
	meta := []iceberg.NestedField{}

	for _, field := range selectedFields {
		if slices.Contains(metaFields, strings.ToLower(field)) {
			switch strings.ToLower(field) {
			case iceberg.LastUpdatedSequenceNumberColumnName:
				meta = append(meta, iceberg.LastUpdatedSequenceNumber())
			case iceberg.RowIDColumnName:
				meta = append(meta, iceberg.RowID())
			}

			continue
		}

		filteredFields = append(filteredFields, field)
	}

	return filteredFields, meta
}

func metaFieldsFromSelectedFields(selectedFields []string, caseSensitive bool) []string {
	meta := []string{}
	if !caseSensitive {
		for _, field := range selectedFields {
			if strings.EqualFold(field, iceberg.RowIDColumnName) || strings.EqualFold(field, iceberg.LastUpdatedSequenceNumberColumnName) {
				meta = append(meta, strings.ToLower(field))
			}
		}

		return meta
	}

	for _, field := range selectedFields {
		if field == iceberg.RowIDColumnName || field == iceberg.LastUpdatedSequenceNumberColumnName {
			meta = append(meta, strings.ToLower(field))
		}
	}

	return meta
}

func metaFieldsFromSchema(sch *iceberg.Schema) []string {
	meta := []string{}
	_, hasRowIDMeta := sch.FindFieldByName(iceberg.RowIDColumnName)
	_, hasSeqMeta := sch.FindFieldByName(iceberg.LastUpdatedSequenceNumberColumnName)

	if hasRowIDMeta {
		meta = append(meta, iceberg.RowIDColumnName)
	}
	if hasSeqMeta {
		meta = append(meta, iceberg.LastUpdatedSequenceNumberColumnName)
	}

	return meta
}

func synthesizeMeta(selectedFieldsMeta []string, schemaMeta []string) []string {
	synthesis := []string{}

	for _, f := range selectedFieldsMeta {
		if !slices.Contains(schemaMeta, f) {
			synthesis = append(synthesis, f)
		}
	}

	return synthesis
}

func (scan *Scan) buildPartitionProjection(specID int) (iceberg.BooleanExpression, error) {
	return buildPartitionProjection(specID, scan.metadata, scan.rowFilter, scan.caseSensitive)
}

func buildPartitionProjection(specID int, meta Metadata, rowFilter iceberg.BooleanExpression, caseSensitive bool) (iceberg.BooleanExpression, error) {
	spec := meta.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}
	project := newInclusiveProjection(meta.CurrentSchema(), *spec, caseSensitive)

	return project(rowFilter)
}

func (scan *Scan) buildManifestEvaluator(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
	return buildManifestEvaluator(specID, scan.metadata, scan.partitionFilters, scan.caseSensitive)
}

func buildManifestEvaluator(specID int, metadata Metadata, partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression], caseSensitive bool) (func(iceberg.ManifestFile) (bool, error), error) {
	spec := metadata.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}

	return newManifestEvaluator(*spec, metadata.CurrentSchema(),
		partitionFilters.Get(specID), caseSensitive)
}

func (scan *Scan) buildPartitionEvaluator(specID int) (func(iceberg.DataFile) (bool, error), error) {
	return buildPartitionEvaluator(specID, scan.metadata, scan.partitionFilters, scan.caseSensitive)
}

func buildPartitionEvaluator(specID int, metadata Metadata, partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression], caseSensitive bool) (func(iceberg.DataFile) (bool, error), error) {
	spec := metadata.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}
	partType := spec.PartitionType(metadata.CurrentSchema())
	partSchema := iceberg.NewSchema(0, partType.FieldList...)

	fn, err := iceberg.ExpressionEvaluator(partSchema, partitionFilters.Get(specID), caseSensitive)
	if err != nil {
		return nil, err
	}

	return func(d iceberg.DataFile) (bool, error) {
		return fn(GetPartitionRecord(d, partType))
	}, nil
}

// BuildManifestEvaluatorForSpec returns a manifest-pruning predicate for the
// given partition spec id, projecting rowFilter onto the spec. Exported for
// the engine package (which has no access to the unexported memoizer the
// scanner uses internally).
func BuildManifestEvaluatorForSpec(specID int, meta Metadata, rowFilter iceberg.BooleanExpression, caseSensitive bool) (func(iceberg.ManifestFile) (bool, error), error) {
	partFilter, err := buildPartitionProjection(specID, meta, rowFilter, caseSensitive)
	if err != nil {
		return nil, err
	}
	spec := meta.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}

	return newManifestEvaluator(*spec, meta.CurrentSchema(), partFilter, caseSensitive)
}

func (scan *Scan) checkSequenceNumber(minSeqNum int64, manifest iceberg.ManifestFile) bool {
	return manifest.ManifestContent() == iceberg.ManifestContentData ||
		(manifest.ManifestContent() == iceberg.ManifestContentDeletes &&
			manifest.SequenceNum() >= minSeqNum)
}

func minSequenceNum(manifests []iceberg.ManifestFile) int64 {
	var n int64 = math.MaxInt64
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData {
			n = min(n, m.MinSequenceNum())
		}
	}
	if n == math.MaxInt64 {
		return 0
	}

	return n
}

func matchDeletesToData(entry iceberg.ManifestEntry, positionalDeletes []iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	idx, _ := slices.BinarySearchFunc(positionalDeletes, entry, func(me1, me2 iceberg.ManifestEntry) int {
		return cmp.Compare(me1.SequenceNum(), me2.SequenceNum())
	})

	evaluator, err := newInclusiveMetricsEvaluator(iceberg.PositionalDeleteSchema,
		iceberg.EqualTo(iceberg.Reference("file_path"), entry.DataFile().FilePath()), true, false)
	if err != nil {
		return nil, err
	}

	out := make([]iceberg.DataFile, 0)
	for _, relevant := range positionalDeletes[idx:] {
		df := relevant.DataFile()
		ok, err := evaluator(df)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, df)
		}
	}

	return out, nil
}

// matchEqualityDeletesToData returns the equality delete files that apply to
// the given data entry. An equality delete applies when:
//   - it has a strictly greater sequence number than the data file
//   - it shares the same partition (for partitioned tables)
//
// The "strictly greater" rule ensures that data files committed in the same
// snapshot as the equality deletes are not affected — this is how RowDelta
// atomically adds new rows alongside deletes for old rows.
func matchEqualityDeletesToData(dataEntry iceberg.ManifestEntry, eqDeleteEntries []iceberg.ManifestEntry) []iceberg.DataFile {
	dataSeqNum := dataEntry.SequenceNum()
	dataPartition := dataEntry.DataFile().Partition()

	out := make([]iceberg.DataFile, 0)
	for _, del := range eqDeleteEntries {
		// Equality deletes only apply to data files with a strictly lower
		// sequence number.
		if del.SequenceNum() <= dataSeqNum {
			continue
		}

		// For partitioned tables, equality deletes must share the same
		// partition as the data file. Unpartitioned deletes (nil/empty
		// partition) apply globally.
		delPartition := del.DataFile().Partition()
		if len(delPartition) > 0 && len(dataPartition) > 0 {
			if !partitionsMatch(dataPartition, delPartition) {
				continue
			}
		}

		out = append(out, del.DataFile())
	}

	return out
}

func partitionsMatch(a, b map[int]any) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}

	return true
}

// fetchPartitionSpecFilteredManifests retrieves the table's current snapshot,
// fetches its manifest files, and applies partition-spec filters to remove irrelevant manifests.
func (scan *Scan) fetchPartitionSpecFilteredManifests(ctx context.Context) ([]iceberg.ManifestFile, error) {
	snap := scan.Snapshot()
	if snap == nil {
		return nil, nil
	}

	afs, err := scan.ioF(ctx)
	if err != nil {
		return nil, err
	}
	// Fetch all manifests for the current snapshot.
	manifestList, err := snap.Manifests(afs)
	if err != nil {
		return nil, err
	}

	// Build per-spec manifest evaluators and filter out irrelevant manifests.
	manifestEvaluators := newKeyDefaultMapWrapErr(scan.buildManifestEvaluator)
	manifestList = slices.DeleteFunc(manifestList, func(mf iceberg.ManifestFile) bool {
		eval := manifestEvaluators.Get(int(mf.PartitionSpecID()))
		use, err := eval(mf)

		return !use || err != nil
	})

	return manifestList, nil
}

// collectManifestEntries concurrently opens manifests, applies partition and metrics
// filters, and accumulates both data entries and positional-delete entries.
func (scan *Scan) collectManifestEntries(
	ctx context.Context,
	manifestList []iceberg.ManifestFile,
) (*manifestEntries, error) {
	metricsEval, err := newInclusiveMetricsEvaluator(
		scan.metadata.CurrentSchema(),
		scan.rowFilter,
		scan.caseSensitive,
		scan.options["include_empty_files"] == "true",
	)
	if err != nil {
		return nil, err
	}

	minSeqNum := minSequenceNum(manifestList)
	concurrencyLimit := min(scan.concurrency, len(manifestList))

	entries := newManifestEntries()
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(concurrencyLimit)

	partitionEvaluators := newKeyDefaultMapWrapErr(scan.buildPartitionEvaluator)

	for _, mf := range manifestList {
		if !scan.checkSequenceNumber(minSeqNum, mf) {
			continue
		}

		g.Go(func() error {
			fs, err := scan.ioF(ctx)
			if err != nil {
				return err
			}
			partEval := partitionEvaluators.Get(int(mf.PartitionSpecID()))
			manifestEntries, err := openManifest(fs, mf, partEval, metricsEval)
			if err != nil {
				return err
			}

			for _, e := range manifestEntries {
				df := e.DataFile()
				switch df.ContentType() {
				case iceberg.EntryContentData:
					entries.addDataEntry(e)
				case iceberg.EntryContentPosDeletes:
					if isDeletionVector(e.DataFile()) {
						entries.addDVEntry(e)
					} else {
						entries.addPositionalDeleteEntry(e)
					}
				case iceberg.EntryContentEqDeletes:
					entries.addEqualityDeleteEntry(e)
				default:
					return fmt.Errorf("%w: unknown DataFileContent type (%s): %s",
						ErrInvalidMetadata, df.ContentType(), e)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return entries, nil
}

// PlanFiles orchestrates the fetching and filtering of manifests, and then
// building a list of FileScanTasks that match the current Scan criteria.
func (scan *Scan) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
	if scan.asOfTimestamp != nil {
		var snapshot *Snapshot
		entries := slices.Collect(scan.metadata.SnapshotLogs())
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if entry.TimestampMs <= *scan.asOfTimestamp {
				snapshot = scan.metadata.SnapshotByID(entry.SnapshotID)

				break
			}
		}
		if snapshot == nil {
			return nil, fmt.Errorf("no snapshot found for timestamp %d", *scan.asOfTimestamp)
		}
		scan.snapshotID = &snapshot.SnapshotID
		scan.asOfTimestamp = nil
	}
	// Step 1: Retrieve filtered manifests based on snapshot and partition specs.
	manifestList, err := scan.fetchPartitionSpecFilteredManifests(ctx)
	if err != nil || len(manifestList) == 0 {
		return nil, err
	}

	// Step 2: Read manifest entries concurrently, accumulating data and positional deletes.
	entries, err := scan.collectManifestEntries(ctx, manifestList)
	if err != nil {
		return nil, err
	}

	// Step 3: Sort positional deletes and match them to data files.
	slices.SortFunc(entries.positionalDeleteEntries, func(a, b iceberg.ManifestEntry) int {
		return cmp.Compare(a.SequenceNum(), b.SequenceNum())
	})

	// Index DVs by referenced data file path for O(1) lookup.
	dvIndex := make(map[string][]iceberg.DataFile, len(entries.dvEntries))
	for _, del := range entries.dvEntries {
		if ref := del.DataFile().ReferencedDataFile(); ref != nil {
			dvIndex[*ref] = append(dvIndex[*ref], del.DataFile())
		}
	}

	results := make([]FileScanTask, 0, len(entries.dataEntries))
	for _, e := range entries.dataEntries {
		deleteFiles, err := matchDeletesToData(e, entries.positionalDeleteEntries)
		if err != nil {
			return nil, err
		}
		eqDeleteFiles := matchEqualityDeletesToData(e, entries.equalityDeleteEntries)

		task := FileScanTask{
			File:                e.DataFile(),
			DeleteFiles:         deleteFiles,
			EqualityDeleteFiles: eqDeleteFiles,
			DeletionVectorFiles: dvIndex[e.DataFile().FilePath()],
			Start:               0,
			Length:              e.DataFile().FileSizeBytes(),
		}
		// Row lineage constants: readers use these to synthesize _row_id and
		// _last_updated_sequence_number when requested.
		task.FirstRowID = e.DataFile().FirstRowID()
		if fseq := e.FileSequenceNum(); fseq != nil {
			task.DataSequenceNumber = fseq
		}
		results = append(results, task)
	}

	return results, nil
}

type FileScanTask struct {
	File                iceberg.DataFile
	DeleteFiles         []iceberg.DataFile // positional delete files
	EqualityDeleteFiles []iceberg.DataFile // equality delete files
	DeletionVectorFiles []iceberg.DataFile // deletion vectors (puffin files)
	Start, Length       int64

	// Row lineage (v3): constants used when reading to synthesize _row_id and _last_updated_sequence_number.
	// FirstRowID is the effective first_row_id for this file (from manifest entry, after inheritance).
	// DataSequenceNumber is the data sequence number of the file's manifest entry.
	FirstRowID         *int64
	DataSequenceNumber *int64
}

// ScanFS resolves the scan's filesystem. Exposed for the engine package's
// arrow record readers.
func (scan *Scan) ScanFS(ctx context.Context) (io.IO, error) { return scan.ioF(ctx) }

// ScanMetadata returns the metadata the scan is reading against.
func (scan *Scan) ScanMetadata() Metadata { return scan.metadata }

// ScanRowFilter returns the scan's row filter expression.
func (scan *Scan) ScanRowFilter() iceberg.BooleanExpression { return scan.rowFilter }

// ScanCaseSensitive reports whether field-name lookups are case-sensitive.
func (scan *Scan) ScanCaseSensitive() bool { return scan.caseSensitive }

// ScanLimit returns the scan's row limit ([ScanNoLimit] when unset).
func (scan *Scan) ScanLimit() int64 { return scan.limit }

// ScanOptions returns the scan's options properties.
func (scan *Scan) ScanOptions() iceberg.Properties { return scan.options }

// ScanConcurrency returns the scan's configured concurrency.
func (scan *Scan) ScanConcurrency() int { return scan.concurrency }

// CollectSafePositionDeletes returns position delete files from the given tasks
// that are safe to remove during compaction. A pos-delete file is safe when it
// was matched to a data file that is being rewritten: the reader will apply the
// deletes, so the new files will not contain those rows. Duplicates are removed.
// Only EntryContentPosDeletes files are included; equality deletes are skipped.
func CollectSafePositionDeletes(tasks []FileScanTask) []iceberg.DataFile {
	seen := make(map[string]bool)
	var safe []iceberg.DataFile

	for _, task := range tasks {
		for _, df := range task.DeleteFiles {
			if df.ContentType() != iceberg.EntryContentPosDeletes {
				continue
			}

			path := df.FilePath()
			if seen[path] {
				continue
			}
			seen[path] = true
			safe = append(safe, df)
		}
	}

	return safe
}
