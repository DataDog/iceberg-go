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
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/io"
	"github.com/google/uuid"
)

type snapshotUpdate struct {
	txn           *Transaction
	io            io.WriteFileIO
	snapshotProps iceberg.Properties
	operation     Operation
}

func (s snapshotUpdate) fastAppend() *snapshotProducer {
	return newFastAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

// mergeOverwrite builds an overwrite producer. filter is the
// row-level predicate the caller declared (typically the same one
// used to plan the deletion); pass nil for a full-table overwrite.
// validate() reads it to decide between filter-bounded and full
// checks.
func (s snapshotUpdate) mergeOverwrite(commitUUID *uuid.UUID, filter iceberg.BooleanExpression) *snapshotProducer {
	op := s.operation
	if s.operation == OpOverwrite && s.txn.meta.currentSnapshot() == nil {
		op = OpAppend
	}
	prod := newOverwriteFilesProducer(op, s.txn, s.io, commitUUID, s.snapshotProps)
	if filter != nil {
		prod.producerImpl.(*overwriteFiles).filter = filter
	}

	return prod
}

func (s snapshotUpdate) mergeAppend() *snapshotProducer {
	return newMergeAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

type Transaction struct {
	tbl    *Table
	meta   *MetadataBuilder
	branch string

	reqs []Requirement

	// validators collects per-producer conflict checks registered
	// during this transaction's lifetime. doCommit runs them against
	// the current catalog state before sending CommitTable so
	// producers can reject commits whose semantics are violated by
	// concurrent peers (partition-filter overlap, referenced-file
	// removal). Fast/merge-append producers do not register a
	// validator (they are safe under any isolation).
	validators []conflictValidatorFunc

	mx        sync.Mutex
	committed bool
}

func (t *Transaction) apply(updates []Update, reqs []Requirement) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return errors.New("transaction has already been committed")
	}

	current, err := t.meta.Build()
	if err != nil {
		return err
	}

	for _, r := range reqs {
		if err := r.Validate(current); err != nil {
			return err
		}
	}

	existing := map[string]struct{}{}
	for _, r := range t.reqs {
		existing[r.GetType()] = struct{}{}
	}

	for _, r := range reqs {
		if _, ok := existing[r.GetType()]; !ok {
			t.reqs = append(t.reqs, r)
		}
	}

	prevUpdates, prevLastUpdated := len(t.meta.updates), t.meta.lastUpdatedMS
	for _, u := range updates {
		if err := u.Apply(t.meta); err != nil {
			return err
		}
	}

	// u.Apply will add updates to t.meta.updates if they are not no-ops
	// and actually perform changes. So let's check if we actually had any
	// changes added and thus need to update the lastupdated value.
	if prevUpdates < len(t.meta.updates) {
		if prevLastUpdated == t.meta.lastUpdatedMS {
			t.meta.lastUpdatedMS = time.Now().UnixMilli()
		}
	}

	return nil
}

func (t *Transaction) appendSnapshotProducer(afs io.IO, props iceberg.Properties) *snapshotProducer {
	manifestMerge := t.meta.props.GetBool(ManifestMergeEnabledKey, ManifestMergeEnabledDefault)
	updateSnapshot := t.updateSnapshot(afs, props, OpAppend)
	if manifestMerge {
		return updateSnapshot.mergeAppend()
	}

	return updateSnapshot.fastAppend()
}

func (t *Transaction) updateSnapshot(fs io.IO, props iceberg.Properties, operation Operation) snapshotUpdate {
	return snapshotUpdate{
		txn:           t,
		io:            fs.(io.WriteFileIO),
		snapshotProps: props,
		operation:     operation,
	}
}

func (t *Transaction) SetProperties(props iceberg.Properties) error {
	if len(props) > 0 {
		return t.apply([]Update{NewSetPropertiesUpdate(props)}, nil)
	}

	return nil
}

// UpgradeFormatVersion upgrades the table to the given format version. Downgrading
// is not allowed. If the table is already at the given version, this is a no-op.
func (t *Transaction) UpgradeFormatVersion(version int) error {
	return t.apply([]Update{NewUpgradeFormatVersionUpdate(version)}, nil)
}

func (t *Transaction) RollbackToSnapshot(snapshotID int64) error {
	cs := t.meta.currentSnapshot()
	if cs == nil {
		return errors.New("cannot rollback: table has no current snapshot")
	}

	lookup := func(id int64) *Snapshot {
		s, _ := t.meta.SnapshotByID(id)

		return s
	}

	if !IsAncestorOf(cs.SnapshotID, snapshotID, lookup) {
		return fmt.Errorf("snapshot %d is not an ancestor of current snapshot %d",
			snapshotID, cs.SnapshotID)
	}

	update := NewSetSnapshotRefUpdate(MainBranch, snapshotID, BranchRef, 0, 0, 0)
	req := AssertRefSnapshotID(MainBranch, &cs.SnapshotID)

	return t.apply([]Update{update}, []Requirement{req})
}

func (t *Transaction) UpdateSpec(caseSensitive bool) *UpdateSpec {
	return NewUpdateSpec(t, caseSensitive)
}

// UpdateSchema creates a new UpdateSchema instance for managing schema changes
// within this transaction.
//
// Parameters:
//   - caseSensitive: If true, field name lookups are case-sensitive; if false,
//     field names are matched case-insensitively.
//   - allowIncompatibleChanges: If true, allows schema changes that would normally
//     be rejected for being incompatible (e.g., adding required fields without
//     default values, changing field types in non-promotable ways, or changing
//     column nullability from optional to required).
//   - opts: Optional configuration functions to customize the UpdateSchema behavior.
//
// Returns an UpdateSchema instance that can be used to build and apply schema changes.
func (t *Transaction) UpdateSchema(caseSensitive bool, allowIncompatibleChanges bool, opts ...UpdateSchemaOption) *UpdateSchema {
	return NewUpdateSchema(t, caseSensitive, allowIncompatibleChanges, opts...)
}

type expireSnapshotsCfg struct {
	minSnapshotsToKeep *int
	maxSnapshotAgeMs   *int64
	postCommit         bool
}

type ExpireSnapshotsOpt func(*expireSnapshotsCfg)

func WithRetainLast(n int) ExpireSnapshotsOpt {
	return func(cfg *expireSnapshotsCfg) {
		cfg.minSnapshotsToKeep = &n
	}
}

func WithOlderThan(t time.Duration) ExpireSnapshotsOpt {
	return func(cfg *expireSnapshotsCfg) {
		n := t.Milliseconds()
		cfg.maxSnapshotAgeMs = &n
	}
}

// WithPostCommit controls whether orphaned files (manifests, manifest lists,
// data files) are deleted immediately after expiring snapshots. Defaults to true.
// Set to false to defer file deletion to a separate maintenance job, avoiding
// conflicts with in-flight queries that may still reference those files.
func WithPostCommit(postCommit bool) ExpireSnapshotsOpt {
	return func(cfg *expireSnapshotsCfg) {
		cfg.postCommit = postCommit
	}
}

func (t *Transaction) ExpireSnapshots(opts ...ExpireSnapshotsOpt) error {
	var (
		cfg         = expireSnapshotsCfg{postCommit: true}
		updates     []Update
		reqs        []Requirement
		snapsToKeep = make(map[int64]struct{})
		nowMs       = time.Now().UnixMilli()
	)

	for _, opt := range opts {
		opt(&cfg)
	}

	// Read table-level retention properties as the last-resort defaults,
	// mirroring the Java implementation. When neither the ref nor the
	// caller provides a value, fall back to the table property; when the
	// table property is also absent use the constant default (math.MaxInt,
	// meaning "keep everything").
	propMaxRefAgeMs := int64(t.meta.props.GetInt(MaxRefAgeMsKey, MaxRefAgeMsDefault))
	propMinSnapshotsToKeep := t.meta.props.GetInt(MinSnapshotsToKeepKey, MinSnapshotsToKeepDefault)
	propMaxSnapshotAgeMs := int64(t.meta.props.GetInt(MaxSnapshotAgeMsKey, MaxSnapshotAgeMsDefault))

	for refName, ref := range t.meta.refs {
		// Assert that this ref's snapshot ID hasn't changed concurrently.
		// This ensures we don't accidentally expire snapshots that are now
		// referenced by updated refs.
		snapshotID := ref.SnapshotID
		reqs = append(reqs, AssertRefSnapshotID(refName, &snapshotID))

		if refName == MainBranch {
			snapsToKeep[ref.SnapshotID] = struct{}{}
		}

		snap, err := t.meta.SnapshotByID(ref.SnapshotID)
		if err != nil {
			return err
		}

		maxRefAgeMs := cmp.Or(ref.MaxRefAgeMs, cfg.maxSnapshotAgeMs, &propMaxRefAgeMs)

		refAge := nowMs - snap.TimestampMs
		if refAge > *maxRefAgeMs && refName != MainBranch {
			updates = append(updates, NewRemoveSnapshotRefUpdate(refName))

			continue
		}

		var (
			minSnapshotsToKeep = cmp.Or(ref.MinSnapshotsToKeep, cfg.minSnapshotsToKeep, &propMinSnapshotsToKeep)
			maxSnapshotAgeMs   = cmp.Or(ref.MaxSnapshotAgeMs, cfg.maxSnapshotAgeMs, &propMaxSnapshotAgeMs)
		)

		if ref.SnapshotRefType != BranchRef {
			snapsToKeep[ref.SnapshotID] = struct{}{}

			continue
		}

		var (
			numSnapshots int
			snapId       = ref.SnapshotID
		)

		for {
			snap, err := t.meta.SnapshotByID(snapId)
			if err != nil {
				// Parent snapshot may have been removed by a previous expiration.
				// Treat missing parent as end of chain - this is expected behavior.
				break
			}

			snapAge := time.Now().UnixMilli() - snap.TimestampMs
			if (snapAge > *maxSnapshotAgeMs) && (numSnapshots >= *minSnapshotsToKeep) {
				break
			}

			snapsToKeep[snap.SnapshotID] = struct{}{}

			if snap.ParentSnapshotID == nil {
				break
			}

			snapId = *snap.ParentSnapshotID
			numSnapshots++
		}
	}

	var snapsToDelete []int64

	for _, snap := range t.meta.snapshotList {
		if _, found := snapsToKeep[snap.SnapshotID]; !found {
			snapsToDelete = append(snapsToDelete, snap.SnapshotID)
		}
	}

	// Only add the update if there are actually snapshots to delete
	if len(snapsToDelete) > 0 {
		updates = append(updates, NewRemoveSnapshotsUpdate(snapsToDelete, cfg.postCommit))
	}

	return t.apply(updates, reqs)
}

// validateDataFilePartitionData verifies that DataFile partition values match
// the current partition spec fields by ID without reading file contents.
func validateDataFilePartitionData(df iceberg.DataFile, spec *iceberg.PartitionSpec) error {
	partitionData := df.Partition()

	expectedFieldIDs := make(map[int]string)
	for _, field := range spec.Fields() {
		expectedFieldIDs[field.FieldID] = field.Name
		if _, ok := partitionData[field.FieldID]; !ok {
			return fmt.Errorf("missing partition value for field id %d (%s)", field.FieldID, field.Name)
		}
	}

	for fieldID := range partitionData {
		if _, ok := expectedFieldIDs[fieldID]; !ok {
			return fmt.Errorf("unknown partition field id %d for spec id %d", fieldID, spec.ID())
		}
	}

	return nil
}

// validateDataFilesToAdd performs metadata-only validation for caller-provided
// DataFiles and returns a set of paths that passed validation.
func (t *Transaction) validateDataFilesToAdd(dataFiles []iceberg.DataFile, operation string) (map[string]struct{}, error) {
	currentSpec, err := t.meta.CurrentSpec()
	if err != nil {
		return nil, fmt.Errorf("could not get current partition spec: %w", err)
	}
	if currentSpec == nil {
		return nil, errors.New("could not get current partition spec: no current partition spec found")
	}

	expectedSpecID := int32(currentSpec.ID())
	setToAdd := make(map[string]struct{}, len(dataFiles))

	for i, df := range dataFiles {
		if df == nil {
			return nil, fmt.Errorf("nil data file at index %d for %s", i, operation)
		}

		path := df.FilePath()
		if path == "" {
			return nil, fmt.Errorf("data file path cannot be empty for %s", operation)
		}

		if _, ok := setToAdd[path]; ok {
			return nil, fmt.Errorf("add data file paths must be unique for %s", operation)
		}
		setToAdd[path] = struct{}{}

		if df.ContentType() != iceberg.EntryContentData {
			return nil, fmt.Errorf("adding files other than data files is not yet implemented: file %s has content type %s for %s", path, df.ContentType(), operation)
		}

		switch df.FileFormat() {
		case iceberg.ParquetFile, iceberg.OrcFile, iceberg.AvroFile:
		default:
			return nil, fmt.Errorf("data file %s has invalid file format %s for %s", path, df.FileFormat(), operation)
		}

		if df.SpecID() != expectedSpecID {
			return nil, fmt.Errorf("data file %s has invalid partition spec id %d for %s: expected %d",
				path, df.SpecID(), operation, expectedSpecID)
		}

		if err := validateDataFilePartitionData(df, currentSpec); err != nil {
			return nil, fmt.Errorf("data file %s has invalid partition data for %s: %w", path, operation, err)
		}
	}

	return setToAdd, nil
}

// WriteOption is an option for methods that operate on pre-built DataFile objects.
type WriteOption func(*dataFileCfg)

type dataFileCfg struct {
	skipAutoNameMapping bool
	skipDuplicateCheck  bool
	rewriteSemantics    bool
}

// withRewriteSemantics marks an overwrite/replace operation as a
// rewrite (compaction) rather than a user-facing overwrite. The
// overwrite producer's default pre-commit conflict validator is
// bypassed; the caller registers a rewrite-specific validator on the
// transaction separately via validateNoNewDeletesForRewrittenFiles.
// Unexported: only RewriteDataFiles passes this; there is no public
// surface for user code to bypass overwrite isolation.
func withRewriteSemantics() WriteOption {
	return func(cfg *dataFileCfg) {
		cfg.rewriteSemantics = true
	}
}

// WithoutAutoNameMapping disables the automatic setting of the schema name
// mapping in table properties. By default, methods like [Transaction.AddDataFiles]
// and [Transaction.ReplaceDataFilesWithDataFiles] will set the name mapping if
// one does not already exist. This option is useful when working with catalogs
// (such as Databricks Unity Catalog) that reject the name mapping property.
func WithoutAutoNameMapping() WriteOption {
	return func(cfg *dataFileCfg) {
		cfg.skipAutoNameMapping = true
	}
}

// WithoutDuplicateCheck disables the duplicate file path check against
// existing data files in the current snapshot. By default, [Transaction.AddDataFiles]
// scans all manifests to ensure no file being added already exists in the
// table. For tables with many manifests this scan can be expensive because
// each manifest must be read from storage. Use this option when the caller
// can guarantee that the files being added are not already in the table.
func WithoutDuplicateCheck() WriteOption {
	return func(cfg *dataFileCfg) {
		cfg.skipDuplicateCheck = true
	}
}

// ensureNameMapping sets the schema name mapping in table properties if one
// does not already exist. This is extracted as a helper so it can be called
// from any method that accepts WriteOption.
func (t *Transaction) ensureNameMapping() error {
	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return err
		}

		return t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
	}

	return nil
}

// AddDataFiles adds pre-built DataFiles to the table without scanning them from storage.
// This is useful for clients who have already constructed DataFile objects with metadata,
// avoiding the need to read files to extract schema and statistics.
//
// Unlike AddFiles, this method does not read files from storage. It validates only metadata
// that can be checked without opening files (for example spec-id and partition field IDs).
//
// By default this method automatically sets the schema name mapping in table
// properties if one does not already exist. Pass [WithoutAutoNameMapping] to
// disable this behavior, for example when working with catalogs that reject
// the name mapping property.
//
// Callers are responsible for ensuring each DataFile is valid and consistent with the table.
// Supplying incorrect DataFile metadata can produce an invalid snapshot and break reads.
func (t *Transaction) AddDataFiles(ctx context.Context, dataFiles []iceberg.DataFile, snapshotProps iceberg.Properties, opts ...WriteOption) error {
	if len(dataFiles) == 0 {
		return nil
	}

	var cfg dataFileCfg
	for _, o := range opts {
		o(&cfg)
	}

	setToAdd, err := t.validateDataFilesToAdd(dataFiles, "AddDataFiles")
	if err != nil {
		return err
	}

	if !cfg.skipAutoNameMapping {
		if err := t.ensureNameMapping(); err != nil {
			return err
		}
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	if !cfg.skipDuplicateCheck {
		if s := t.meta.currentSnapshot(); s != nil {
			referenced := make([]string, 0)
			for df, err := range s.dataFiles(fs, nil) {
				if err != nil {
					return err
				}

				if _, ok := setToAdd[df.FilePath()]; ok {
					referenced = append(referenced, df.FilePath())
				}
			}

			if len(referenced) > 0 {
				return fmt.Errorf("cannot add files that are already referenced by table, files: %v", referenced)
			}
		}
	}

	appendFiles := t.appendSnapshotProducer(fs, snapshotProps)
	for _, df := range dataFiles {
		appendFiles.appendDataFile(df)
	}

	updates, reqs, err := appendFiles.commit(ctx)
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// ReplaceDataFilesWithDataFiles replaces files using pre-built DataFile objects.
// This avoids scanning files to extract schema and statistics - the caller provides
// DataFile objects directly with all required metadata.
//
// For the files to add, use iceberg.NewDataFileBuilder to construct DataFile objects
// with the appropriate metadata (path, record count, file size, partition values).
//
// This method does not open files. It validates only metadata that can be checked
// without reading file contents.
//
// By default this method automatically sets the schema name mapping in table
// properties if one does not already exist. Pass [WithoutAutoNameMapping] to
// disable this behavior, for example when working with catalogs that reject
// the name mapping property.
//
// Callers are responsible for ensuring each DataFile is valid and consistent with the table.
// Supplying incorrect DataFile metadata can produce an invalid snapshot and break reads.
//
// This is useful when:
//   - Files are written via a separate I/O path and metadata is already known
//   - Avoiding file scanning improves performance or reliability
//   - Working with storage systems where immediate file reads may be unreliable
func (t *Transaction) ReplaceDataFilesWithDataFiles(ctx context.Context, filesToDelete, filesToAdd []iceberg.DataFile, snapshotProps iceberg.Properties, opts ...WriteOption) error {
	if len(filesToDelete) == 0 {
		if len(filesToAdd) > 0 {
			return t.AddDataFiles(ctx, filesToAdd, snapshotProps, opts...)
		}

		return nil
	}

	var cfg dataFileCfg
	for _, o := range opts {
		o(&cfg)
	}

	setToAdd, err := t.validateDataFilesToAdd(filesToAdd, "ReplaceDataFilesWithDataFiles")
	if err != nil {
		return err
	}

	setToDelete := make(map[string]struct{}, len(filesToDelete))
	for i, df := range filesToDelete {
		if df == nil {
			return fmt.Errorf("nil data file at index %d for ReplaceDataFilesWithDataFiles", i)
		}

		path := df.FilePath()
		if path == "" {
			return errors.New("delete data file paths must be non-empty for ReplaceDataFilesWithDataFiles")
		}

		if _, ok := setToDelete[path]; ok {
			return errors.New("delete data file paths must be unique for ReplaceDataFilesWithDataFiles")
		}
		setToDelete[path] = struct{}{}
	}

	s := t.meta.currentSnapshot()
	if s == nil {
		return fmt.Errorf("%w: cannot replace files in a table without an existing snapshot", ErrInvalidOperation)
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	markedForDeletion := make([]iceberg.DataFile, 0, len(setToDelete))
	for df, err := range s.dataFiles(fs, nil) {
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

	if !cfg.skipAutoNameMapping {
		if err := t.ensureNameMapping(); err != nil {
			return err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(fs, snapshotProps, OpOverwrite).mergeOverwrite(&commitUUID, nil)
	if cfg.rewriteSemantics {
		// mergeOverwrite guarantees an *overwriteFiles producerImpl.
		updater.producerImpl.(*overwriteFiles).skipDefaultValidator = true
	}

	for _, df := range markedForDeletion {
		updater.deleteDataFile(df)
	}

	for _, df := range filesToAdd {
		updater.appendDataFile(df)
	}

	updates, reqs, err := updater.commit(ctx)
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// ReplaceFiles atomically replaces data files and removes associated delete files
// in a single snapshot. This is the commit primitive for compaction: old data files
// are replaced with new (compacted) data files, and delete files that are fully
// applied are removed.
func (t *Transaction) ReplaceFiles(ctx context.Context, dataFilesToDelete, dataFilesToAdd, deleteFilesToRemove []iceberg.DataFile, snapshotProps iceberg.Properties, opts ...WriteOption) error {
	// Delegate data file replacement to existing logic.
	if len(deleteFilesToRemove) == 0 {
		return t.ReplaceDataFilesWithDataFiles(ctx, dataFilesToDelete, dataFilesToAdd, snapshotProps, opts...)
	}

	var cfg dataFileCfg
	for _, o := range opts {
		o(&cfg)
	}

	setToAdd, err := t.validateDataFilesToAdd(dataFilesToAdd, "ReplaceFiles")
	if err != nil {
		return err
	}

	setToDelete := make(map[string]struct{}, len(dataFilesToDelete))
	for i, df := range dataFilesToDelete {
		if df == nil {
			return fmt.Errorf("nil data file at index %d for ReplaceFiles", i)
		}
		path := df.FilePath()
		if path == "" {
			return errors.New("delete data file paths must be non-empty for ReplaceFiles")
		}
		if _, ok := setToDelete[path]; ok {
			return errors.New("delete data file paths must be unique for ReplaceFiles")
		}
		setToDelete[path] = struct{}{}
	}

	setDeleteFilesToRemove := make(map[string]struct{}, len(deleteFilesToRemove))
	for i, df := range deleteFilesToRemove {
		if df == nil {
			return fmt.Errorf("nil delete file at index %d for ReplaceFiles", i)
		}
		path := df.FilePath()
		if path == "" {
			return errors.New("delete file paths must be non-empty for ReplaceFiles")
		}
		if _, ok := setDeleteFilesToRemove[path]; ok {
			return errors.New("delete file paths must be unique for ReplaceFiles")
		}
		setDeleteFilesToRemove[path] = struct{}{}
	}

	s := t.meta.currentSnapshot()
	if s == nil {
		return fmt.Errorf("%w: cannot replace files in a table without an existing snapshot", ErrInvalidOperation)
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// Scan all entries (data + delete files) in a single pass to validate
	// that all files to delete/remove actually exist in the table.
	markedDataForDeletion := make([]iceberg.DataFile, 0, len(setToDelete))
	markedDeleteForRemoval := make([]iceberg.DataFile, 0, len(setDeleteFilesToRemove))
	for df, err := range s.dataFiles(fs, nil) {
		if err != nil {
			return err
		}
		path := df.FilePath()
		isData := df.ContentType() == iceberg.EntryContentData
		if _, ok := setToDelete[path]; ok && isData {
			markedDataForDeletion = append(markedDataForDeletion, df)
		}
		if _, ok := setDeleteFilesToRemove[path]; ok && !isData {
			markedDeleteForRemoval = append(markedDeleteForRemoval, df)
		}
		if _, ok := setToAdd[path]; ok {
			return fmt.Errorf("cannot add files that are already referenced by table, files: %s", path)
		}
	}

	if len(markedDataForDeletion) != len(setToDelete) {
		return errors.New("cannot delete data files that do not belong to the table")
	}
	if len(markedDeleteForRemoval) != len(setDeleteFilesToRemove) {
		return errors.New("cannot remove delete files that do not belong to the table")
	}

	if !cfg.skipAutoNameMapping {
		if err := t.ensureNameMapping(); err != nil {
			return err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(fs, snapshotProps, OpOverwrite).mergeOverwrite(&commitUUID, nil)
	if cfg.rewriteSemantics {
		// mergeOverwrite guarantees an *overwriteFiles producerImpl.
		updater.producerImpl.(*overwriteFiles).skipDefaultValidator = true
	}

	for _, df := range markedDataForDeletion {
		updater.deleteDataFile(df)
	}
	for _, df := range dataFilesToAdd {
		updater.appendDataFile(df)
	}
	for _, df := range markedDeleteForRemoval {
		updater.removeDeleteFile(df)
	}

	updates, reqs, err := updater.commit(ctx)
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

func (t *Transaction) Scan(opts ...ScanOption) (*Scan, error) {
	updatedMeta, err := t.meta.Build()
	if err != nil {
		return nil, err
	}

	s := &Scan{
		metadata:       updatedMeta,
		ioF:            t.tbl.fsF,
		rowFilter:      iceberg.AlwaysTrue{},
		selectedFields: []string{"*"},
		caseSensitive:  true,
		limit:          ScanNoLimit,
		concurrency:    runtime.GOMAXPROCS(0),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.partitionFilters = newKeyDefaultMapWrapErr(s.buildPartitionProjection)

	return s, nil
}

func (t *Transaction) StagedTable() (*StagedTable, error) {
	updatedMeta, err := t.meta.Build()
	if err != nil {
		return nil, err
	}

	return &StagedTable{
		Table: New(
			t.tbl.identifier,
			updatedMeta,
			updatedMeta.Location(),
			t.tbl.fsF,
			t.tbl.cat,
		),
	}, nil
}

func (t *Transaction) Commit(ctx context.Context) (*Table, error) {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return nil, errors.New("transaction has already been committed")
	}

	t.committed = true

	if len(t.meta.updates) > 0 {
		t.reqs = append(t.reqs, AssertTableUUID(t.meta.uuid))
		tbl, err := t.tbl.doCommit(ctx, t.meta.updates, t.reqs,
			withCommitBranch(t.branch),
			withCommitValidators(t.validators...),
		)
		if err != nil {
			return tbl, err
		}

		for _, u := range t.meta.updates {
			if perr := u.PostCommit(ctx, t.tbl, tbl); perr != nil {
				err = errors.Join(err, perr)
			}
		}

		return tbl, err
	}

	return t.tbl, nil
}

type StagedTable struct {
	*Table
}

func (s *StagedTable) Refresh(ctx context.Context) (*Table, error) {
	return nil, fmt.Errorf("%w: cannot refresh a staged table", ErrInvalidOperation)
}

