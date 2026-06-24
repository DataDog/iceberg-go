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

// This file exposes an exported bridge surface so that the arrow-heavy
// query/write engine can live in the table/engine subpackage without
// the table package itself importing arrow-go. None of the declarations
// here import arrow.

import (
	"context"
	"iter"

	"github.com/DataDog/iceberg-go"
	icebergio "github.com/DataDog/iceberg-go/io"
	"github.com/google/uuid"
)

// AllDataFiles iterates every data and delete file referenced by the
// snapshot. Exposed for the engine package's file-add/replace paths.
func (s Snapshot) AllDataFiles(fio icebergio.IO) iter.Seq2[iceberg.DataFile, error] {
	return s.dataFiles(fio, nil)
}

// SnapshotProducer is the exported handle the engine package uses to
// stage data/delete file changes into a snapshot. It aliases the
// internal snapshotProducer so internal callers and the engine share
// one implementation.
type SnapshotProducer = snapshotProducer

// AppendDataFile stages a data file to be added in this snapshot.
func (sp *snapshotProducer) AppendDataFile(df iceberg.DataFile) *snapshotProducer {
	return sp.appendDataFile(df)
}

// AppendDeleteFile stages a delete file to be added in this snapshot.
func (sp *snapshotProducer) AppendDeleteFile(df iceberg.DataFile) *snapshotProducer {
	return sp.appendDeleteFile(df)
}

// DeleteDataFile stages a data file to be removed in this snapshot.
func (sp *snapshotProducer) DeleteDataFile(df iceberg.DataFile) *snapshotProducer {
	return sp.deleteDataFile(df)
}

// RemoveDeleteFile stages a delete file to be removed in this snapshot.
func (sp *snapshotProducer) RemoveDeleteFile(df iceberg.DataFile) *snapshotProducer {
	return sp.removeDeleteFile(df)
}

// CommitUUID returns the producer's write UUID used to name files.
func (sp *snapshotProducer) CommitUUID() uuid.UUID { return sp.commitUuid }

// AddedFiles returns the data files staged for addition.
func (sp *snapshotProducer) AddedFiles() []iceberg.DataFile { return sp.addedFiles }

// DeletedFiles returns the data files staged for deletion.
func (sp *snapshotProducer) DeletedFiles() map[string]iceberg.DataFile { return sp.deletedFiles }

// Commit builds the updates and requirements for this producer's snapshot.
func (sp *snapshotProducer) Commit(ctx context.Context) ([]Update, []Requirement, error) {
	return sp.commit(ctx)
}

// --- Transaction bridge -------------------------------------------------

// CurrentMeta returns the transaction's in-progress metadata builder.
func (t *Transaction) CurrentMeta() *MetadataBuilder { return t.meta }

// Table returns the transaction's underlying table.
func (t *Transaction) Table() *Table { return t.tbl }

// TableFS resolves the table's filesystem.
func (t *Transaction) TableFS(ctx context.Context) (icebergio.IO, error) {
	return t.tbl.fsF(ctx)
}

// ApplyUpdates applies the given updates and requirements to the transaction.
func (t *Transaction) ApplyUpdates(updates []Update, reqs []Requirement) error {
	return t.apply(updates, reqs)
}

// AppendProducer returns a snapshot producer configured for appends,
// honoring the table's manifest-merge property.
func (t *Transaction) AppendProducer(fs icebergio.IO, props iceberg.Properties) *snapshotProducer {
	return t.appendSnapshotProducer(fs, props)
}

// OverwriteProducer returns an overwrite snapshot producer for the given
// operation and optional row-level filter (nil for a full overwrite).
func (t *Transaction) OverwriteProducer(fs icebergio.IO, props iceberg.Properties, op Operation, commitUUID *uuid.UUID, filter iceberg.BooleanExpression) *snapshotProducer {
	return t.updateSnapshot(fs, props, op).mergeOverwrite(commitUUID, filter)
}

// EnsureNameMapping sets the schema name mapping in table properties if one
// does not already exist.
func (t *Transaction) EnsureNameMapping() error { return t.ensureNameMapping() }

// AppendValidator registers a conflict validator on the transaction.
func (t *Transaction) AppendValidator(v ConflictValidatorFunc) {
	t.validators = append(t.validators, conflictValidatorFunc(v))
}

// RewriteValidatorFor returns a conflict validator that rejects the commit
// if a concurrent snapshot added delete files pointing at any of the
// rewritten data-file paths.
func RewriteValidatorFor(rewrittenPaths []string) ConflictValidatorFunc {
	return func(cc *conflictContext) error {
		if cc == nil {
			return nil
		}

		return validateNoNewDeletesForRewrittenFiles(cc, rewrittenPaths)
	}
}

// rewriteValidator is the unexported alias used by internal tests.
func rewriteValidator(rewrittenPaths []string) conflictValidatorFunc {
	return RewriteValidatorFor(rewrittenPaths)
}

// WithRewriteSemantics marks an overwrite/replace as a rewrite (compaction),
// bypassing the overwrite producer's default isolation validator.
func WithRewriteSemantics() WriteOption { return withRewriteSemantics() }

// ConflictValidatorFunc is the exported alias for a pre-commit conflict
// validator used by rewrite-style operations.
type ConflictValidatorFunc = conflictValidatorFunc

// --- MetadataBuilder bridge --------------------------------------------

// Props returns the metadata builder's properties.
func (b *MetadataBuilder) Props() iceberg.Properties { return b.props }

// FormatVersion returns the metadata builder's format version.
func (b *MetadataBuilder) FormatVersion() int { return b.formatVersion }

// DefaultSortOrderID returns the metadata builder's default sort order id.
func (b *MetadataBuilder) DefaultSortOrderID() int { return b.defaultSortOrderID }

// CurrentSnapshotBuilding returns the current snapshot of the in-progress
// metadata builder (nil when there is none).
func (b *MetadataBuilder) CurrentSnapshotBuilding() *Snapshot { return b.currentSnapshot() }

// SetProp sets a single property on the metadata builder.
func (b *MetadataBuilder) SetProp(key, value string) {
	if b.props == nil {
		b.props = make(iceberg.Properties)
	}
	b.props[key] = value
}
