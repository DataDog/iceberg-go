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

package engine_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table"
	"github.com/DataDog/iceberg-go/table/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orphanInMemoryCatalog is a minimal catalog for the orphan-cleanup test.
type orphanInMemoryCatalog struct {
	metadata table.Metadata
}

func (c *orphanInMemoryCatalog) CommitTable(
	ctx context.Context,
	ident table.Identifier,
	reqs []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

func (c *orphanInMemoryCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, nil
}

func TestGetReferencedFiles_OverwriteThenExpireExcludesTombstones(t *testing.T) {
	ctx := context.Background()
	tableLocation := t.TempDir()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	spec := *iceberg.UnpartitionedSpec

	meta, err := engine.NewMetadata(schema, &spec, engine.UnsortedSortOrder, tableLocation,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	fs := iceio.LocalFS{}
	tbl := engine.New(
		table.Identifier{"db", "tbl"},
		meta,
		tableLocation+"/metadata/v0.metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil },
		&orphanInMemoryCatalog{meta},
	)

	// Step 1: append id=1. Produces snapshot 1 with one ADDED entry for fileA.
	arrA, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema, []string{`[{"id": 1}]`})
	require.NoError(t, err)
	defer arrA.Release()
	tbl, err = engine.TableAppendTable(ctx, tbl, arrA, 1, nil)
	require.NoError(t, err)

	snap1 := tbl.CurrentSnapshot()
	require.NotNil(t, snap1)
	pathA := orphanDataFilePathsFromSnapshot(t, snap1, fs, iceberg.EntryStatusADDED)
	require.Len(t, pathA, 1, "expected one ADDED data file after append")
	fileA := pathA[0]

	// Step 2: overwrite with id=2. Produces snapshot 2 whose manifest list
	// contains [added-fileB-manifest, deleted-fileA-manifest]. fileA still
	// lives in snapshot 1's manifest as ADDED at this point.
	arrB, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema, []string{`[{"id": 2}]`})
	require.NoError(t, err)
	defer arrB.Release()
	tbl, err = engine.TableOverwriteTable(ctx, tbl, arrB, 1, nil)
	require.NoError(t, err)
	require.Len(t, tbl.Metadata().Snapshots(), 2, "expected two snapshots after overwrite")

	pathB := orphanDataFilePathsFromSnapshot(t, tbl.CurrentSnapshot(), fs, iceberg.EntryStatusADDED)
	require.Len(t, pathB, 1, "expected one ADDED data file after overwrite")
	fileB := pathB[0]

	// Step 3: expire snapshot 1, keeping only the overwrite snapshot.
	// WithPostCommit(false) keeps fileA on disk so the test only exercises
	// metadata reachability, not the side-effect of file removal.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.ExpireSnapshots(
		table.WithRetainLast(1),
		table.WithOlderThan(0),
		table.WithPostCommit(false),
	))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Len(t, tbl.Metadata().Snapshots(), 1,
		"only the overwrite snapshot should remain after expiration")

	// fileA is now referenced only via a DELETED entry in the surviving
	// snapshot's tombstone manifest. The fix must exclude it.
	refs, err := tbl.GetReferencedFiles(fs)
	require.NoError(t, err)

	assert.True(t, refs[fileB],
		"new live file (ADDED in surviving snapshot) must be in reference set")
	assert.False(t, refs[fileA],
		"overwritten file (only present as DELETED tombstone) must NOT be in reference set")
}

// orphanDataFilePathsFromSnapshot returns the data-file paths referenced by the
// given snapshot's manifests, filtered to entries matching wantStatus.
func orphanDataFilePathsFromSnapshot(
	t *testing.T,
	snap *table.Snapshot,
	fs iceio.IO,
	wantStatus iceberg.ManifestEntryStatus,
) []string {
	t.Helper()
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	var paths []string
	for _, m := range manifests {
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == wantStatus {
				paths = append(paths, e.DataFile().FilePath())
			}
		}
	}

	return paths
}
