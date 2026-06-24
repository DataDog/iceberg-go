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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/DataDog/iceberg-go"
)

// TableAppendTable appends an Arrow table to t in a single committed transaction.
func TableAppendTable(ctx context.Context, t *Table, table arrow.Table, batchSize int64, snapshotProps iceberg.Properties) (*Table, error) {
	txn := t.NewTransaction()
	if err := AppendTable(ctx, txn, table, batchSize, snapshotProps); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// TableAppend appends records to t in a single committed transaction.
func TableAppend(ctx context.Context, t *Table, rdr array.RecordReader, snapshotProps iceberg.Properties) (*Table, error) {
	txn := t.NewTransaction()
	if err := Append(ctx, txn, rdr, snapshotProps); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// TableOverwriteTable overwrites t with an Arrow table in a single committed transaction.
func TableOverwriteTable(ctx context.Context, t *Table, table arrow.Table, batchSize int64, snapshotProps iceberg.Properties, opts ...OverwriteOption) (*Table, error) {
	txn := t.NewTransaction()
	if err := OverwriteTable(ctx, txn, table, batchSize, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// TableOverwrite overwrites t with a RecordReader in a single committed transaction.
func TableOverwrite(ctx context.Context, t *Table, rdr array.RecordReader, snapshotProps iceberg.Properties, opts ...OverwriteOption) (*Table, error) {
	txn := t.NewTransaction()
	if err := Overwrite(ctx, txn, rdr, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// TableDelete deletes rows matching filter from t in a single committed transaction.
func TableDelete(ctx context.Context, t *Table, filter iceberg.BooleanExpression, snapshotProps iceberg.Properties, opts ...DeleteOption) (*Table, error) {
	txn := t.NewTransaction()
	if err := Delete(ctx, txn, filter, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}
