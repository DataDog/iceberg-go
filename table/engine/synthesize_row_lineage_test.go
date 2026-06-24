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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSynthesizeRowLineageColumns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)
	firstRowID := int64(1000)
	dataSeqNum := int64(5)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &dataSeqNum}
	rowOffset := int64(0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: iceberg.LastUpdatedSequenceNumberColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		nil,
	)
	const nrows = 3
	xBldr := array.NewInt64Builder(mem)
	defer xBldr.Release()
	xBldr.AppendValues([]int64{1, 2, 3}, nil)
	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendNulls(nrows)
	seqBldr := array.NewInt64Builder(mem)
	defer seqBldr.Release()
	seqBldr.AppendNulls(nrows)

	xArr := xBldr.NewArray()
	rowIDArr := rowIDBldr.NewArray()
	seqArr := seqBldr.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{xArr, rowIDArr, seqArr}, nrows)
	xArr.Release()
	rowIDArr.Release()
	seqArr.Release()
	defer batch.Release()

	out, err := synthesizeRowLineageColumns(ctx, &rowOffset, task, batch)
	require.NoError(t, err)
	defer out.Release()

	rowIDCol := out.Column(1).(*array.Int64)
	require.Equal(t, nrows, rowIDCol.Len())
	for i := 0; i < nrows; i++ {
		assert.False(t, rowIDCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 1000+int64(i), rowIDCol.Value(i), "row %d", i)
	}
	seqCol := out.Column(2).(*array.Int64)
	for i := 0; i < nrows; i++ {
		assert.False(t, seqCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 5, seqCol.Value(i), "row %d", i)
	}
	assert.EqualValues(t, 3, rowOffset)
}
