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

import "github.com/DataDog/iceberg-go"

// mockDataFile is a minimal iceberg.DataFile implementation used in tests.
type mockDataFile struct {
	path        string
	contentType iceberg.ManifestEntryContent
	format      iceberg.FileFormat
	partition   map[int]any
	count       int64
	columnSizes map[int]int64
	filesize    int64
	valueCounts map[int]int64
	nullCounts  map[int]int64
	nanCounts   map[int]int64
	lowerBounds map[int][]byte
	upperBounds map[int][]byte

	specid      int32
	sortOrderID *int
}

func (m *mockDataFile) ContentType() iceberg.ManifestEntryContent { return m.contentType }
func (m *mockDataFile) FilePath() string                          { return m.path }
func (m *mockDataFile) FileFormat() iceberg.FileFormat            { return m.format }
func (m *mockDataFile) Partition() map[int]any                    { return m.partition }
func (m *mockDataFile) Count() int64                              { return m.count }
func (m *mockDataFile) FileSizeBytes() int64                      { return m.filesize }
func (m *mockDataFile) ColumnSizes() map[int]int64                { return m.columnSizes }
func (m *mockDataFile) ValueCounts() map[int]int64                { return m.valueCounts }
func (m *mockDataFile) NullValueCounts() map[int]int64            { return m.nullCounts }
func (m *mockDataFile) NaNValueCounts() map[int]int64             { return m.nanCounts }
func (*mockDataFile) DistinctValueCounts() map[int]int64          { return nil }
func (m *mockDataFile) LowerBoundValues() map[int][]byte          { return m.lowerBounds }
func (m *mockDataFile) UpperBoundValues() map[int][]byte          { return m.upperBounds }
func (*mockDataFile) KeyMetadata() []byte                         { return nil }
func (*mockDataFile) SplitOffsets() []int64                       { return nil }
func (*mockDataFile) EqualityFieldIDs() []int                     { return nil }
func (m *mockDataFile) SortOrderID() *int                         { return m.sortOrderID }
func (m *mockDataFile) SpecID() int32                             { return m.specid }
func (*mockDataFile) FirstRowID() *int64                          { return nil }
func (*mockDataFile) ReferencedDataFile() *string                 { return nil }
func (*mockDataFile) ContentOffset() *int64                       { return nil }
func (*mockDataFile) ContentSizeInBytes() *int64                  { return nil }
