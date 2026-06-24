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
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/DataDog/iceberg-go"
)

// ToArrowRecords returns the arrow schema of the expected records and an
// iterator over the scan's records. The schema is returned up front so the
// projection is known even when no rows match.
func ToArrowRecords(ctx context.Context, scan *Scan) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}

	return ReadTasks(ctx, scan, tasks)
}

// ReadTasks reads Arrow records from a specific set of FileScanTasks, applying
// the scan's projection, row filters, and delete handling.
func ReadTasks(ctx context.Context, scan *Scan, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	var (
		boundFilter iceberg.BooleanExpression
		err         error
	)

	meta := scan.ScanMetadata()
	if rf := scan.ScanRowFilter(); rf != nil {
		boundFilter, err = iceberg.BindExpr(meta.CurrentSchema(), rf, scan.ScanCaseSensitive())
		if err != nil {
			return nil, nil, err
		}
	}

	schema, err := scan.Projection()
	if err != nil {
		return nil, nil, err
	}

	fs, err := scan.ScanFS(ctx)
	if err != nil {
		return nil, nil, err
	}

	return (&arrowScan{
		metadata:        meta,
		fs:              fs,
		projectedSchema: schema,
		boundRowFilter:  boundFilter,
		caseSensitive:   scan.ScanCaseSensitive(),
		rowLimit:        scan.ScanLimit(),
		options:         scan.ScanOptions(),
		concurrency:     scan.ScanConcurrency(),
	}).GetRecords(ctx, tasks)
}

// ToArrowTable reads the scan fully into an in-memory arrow.Table.
func ToArrowTable(ctx context.Context, scan *Scan) (arrow.Table, error) {
	schema, itr, err := ToArrowRecords(ctx, scan)
	if err != nil {
		return nil, err
	}

	records := make([]arrow.RecordBatch, 0)
	for rec, err := range itr {
		if err != nil {
			return nil, err
		}

		defer rec.Release()
		records = append(records, rec)
	}

	return array.NewTableFromRecords(schema, records), nil
}
