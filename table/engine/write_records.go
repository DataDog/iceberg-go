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
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/internal"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/google/uuid"
)

// WriteRecordOption configures the behavior of WriteRecords.
type WriteRecordOption func(*writeRecordConfig)

type writeRecordConfig struct {
	targetFileSize  int64
	writeUUID       *uuid.UUID
	maxWriteWorkers int
	clustered       bool
}

// WithTargetFileSize overrides the table's default target file size.
func WithTargetFileSize(size int64) WriteRecordOption {
	return func(c *writeRecordConfig) {
		c.targetFileSize = size
	}
}

// WithWriteUUID sets a specific UUID for file naming.
func WithWriteUUID(id uuid.UUID) WriteRecordOption {
	return func(c *writeRecordConfig) {
		c.writeUUID = &id
	}
}

// WithMaxWriteWorkers overrides the default number of fanout workers used for
// partitioned writes. A value of 0 uses the default. Incompatible with
// [WithClusteredWrite].
func WithMaxWriteWorkers(n int) WriteRecordOption {
	return func(c *writeRecordConfig) {
		c.maxWriteWorkers = n
	}
}

// WithClusteredWrite enables the memory-efficient clustered write path for
// partitioned tables. The input must be clustered by partition across batches.
// Incompatible with [WithMaxWriteWorkers].
func WithClusteredWrite() WriteRecordOption {
	return func(c *writeRecordConfig) {
		c.clustered = true
	}
}

// WriteRecords writes Arrow record batches to Parquet data files for the given
// table, returning an iterator of the resulting DataFile objects.
//
// The provided Arrow schema must be compatible with the table's current Iceberg
// schema: each field in the Arrow schema is matched to the table schema by
// field ID (or by name via the table's name mapping if field IDs are absent).
// The Arrow schema may be a subset of the table schema (projection), but every
// field present must have a type that is promotable to the corresponding table
// field type.
//
// WriteRecords releases each RecordBatch it consumes. If the caller needs a
// batch to remain valid after it has been yielded, it must call Retain before
// yielding and is then responsible for the corresponding Release.
func WriteRecords(ctx context.Context, tbl *Table,
	schema *arrow.Schema,
	records iter.Seq2[arrow.RecordBatch, error],
	opts ...WriteRecordOption,
) iter.Seq2[iceberg.DataFile, error] {
	if err := checkArrowSchemaCompat(tbl.Schema(), schema, false); err != nil {
		return internal.SingleErrorIter[iceberg.DataFile](
			fmt.Errorf("arrow schema is not compatible with the table schema: %w", err))
	}

	cfg := writeRecordConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.clustered && cfg.maxWriteWorkers > 0 {
		return internal.SingleErrorIter[iceberg.DataFile](
			errors.New("WithClusteredWrite and WithMaxWriteWorkers are incompatible: the clustered write path is single-threaded"))
	}

	fs, err := tbl.FS(ctx)
	if err != nil {
		return internal.SingleErrorIter[iceberg.DataFile](err)
	}

	writeFS, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return internal.SingleErrorIter[iceberg.DataFile](fmt.Errorf("%w: filesystem does not support writing", iceberg.ErrNotImplemented))
	}

	meta, err := MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
	if err != nil {
		return internal.SingleErrorIter[iceberg.DataFile](fmt.Errorf("failed to build metadata: %w", err))
	}

	if cfg.targetFileSize > 0 {
		meta.SetProp(WriteTargetFileSizeBytesKey, strconv.FormatInt(cfg.targetFileSize, 10))
	}

	releasing := func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range records {
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

	args := recordWritingArgs{
		sc:              schema,
		itr:             releasing,
		fs:              writeFS,
		writeUUID:       cfg.writeUUID,
		maxWriteWorkers: cfg.maxWriteWorkers,
		clustered:       cfg.clustered,
	}

	return recordsToDataFiles(ctx, tbl.Location(), meta, args)
}
