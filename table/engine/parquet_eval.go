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
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/DataDog/iceberg-go"
	topinternal "github.com/DataDog/iceberg-go/internal"
	tbl "github.com/DataDog/iceberg-go/table"
)

// newParquetRowGroupStatsEvaluator adapts the arrow-free inclusive metrics
// row-group evaluator (in the table package) to the Parquet metadata the
// reader provides. It extracts per-column statistics from each row group and
// feeds them to the table evaluator.
func newParquetRowGroupStatsEvaluator(fileSchema *iceberg.Schema, expr iceberg.BooleanExpression,
	includeEmptyFiles bool,
) (func(*metadata.RowGroupMetaData, []int) (bool, error), error) {
	eval, err := tbl.NewRowGroupStatsEvaluator(fileSchema, expr, includeEmptyFiles)
	if err != nil {
		return nil, err
	}

	return func(rgmeta *metadata.RowGroupMetaData, colIndices []int) (bool, error) {
		stats := RowGroupStats{
			NumRows:     rgmeta.NumRows(),
			ValueCounts: make(map[int]int64),
			NullCounts:  make(map[int]int64),
			LowerBounds: make(map[int][]byte),
			UpperBounds: make(map[int][]byte),
		}

		for _, c := range colIndices {
			colMeta, err := rgmeta.ColumnChunk(c)
			if err != nil {
				return false, err
			}

			if ok, err := colMeta.StatsSet(); !ok || err != nil {
				continue
			}

			colStats, err := colMeta.Statistics()
			if err != nil {
				return false, err
			}

			if colStats == nil {
				continue
			}

			fieldID := int(colStats.Descr().SchemaNode().FieldID())
			stats.ValueCounts[fieldID] = colStats.NumValues()
			if colStats.HasNullCount() {
				stats.NullCounts[fieldID] = colStats.NullCount()
			}
			if colStats.HasMinMax() {
				stats.LowerBounds[fieldID] = colStats.EncodeMin()
				stats.UpperBounds[fieldID] = colStats.EncodeMax()
			}
		}

		return eval(stats)
	}, nil
}

func newBloomFilterPredicates(expr iceberg.BooleanExpression) ([]topinternal.RowGroupBloomPred, error) {
	return tbl.NewBloomFilterPredicates(expr)
}
