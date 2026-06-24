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

// This file re-exports the table-package symbols the engine depends on so
// the moved arrow-heavy code can keep referencing them unqualified. The
// table package itself stays free of arrow-go; the engine pulls arrow in.

import tbl "github.com/DataDog/iceberg-go/table"

// Re-exported types.
type (
	Metadata         = tbl.Metadata
	MetadataBuilder  = tbl.MetadataBuilder
	FileScanTask     = tbl.FileScanTask
	LocationProvider = tbl.LocationProvider
	Snapshot         = tbl.Snapshot
	Summary          = tbl.Summary
	Operation        = tbl.Operation
	Update           = tbl.Update
	Requirement      = tbl.Requirement
	Table            = tbl.Table
	Transaction      = tbl.Transaction
	StagedTable      = tbl.StagedTable
	Scan             = tbl.Scan
	ScanOption       = tbl.ScanOption
	WriteOption      = tbl.WriteOption
	FSysF            = tbl.FSysF
	RowGroupStats    = tbl.RowGroupStats

	SnapshotProducer     = tbl.SnapshotProducer
	ConflictValidatorFunc = tbl.ConflictValidatorFunc
)

// Re-exported functions and constructors.
var (
	LoadLocationProvider    = tbl.LoadLocationProvider
	MetadataBuilderFromBase = tbl.MetadataBuilderFromBase
	New                     = tbl.New
	GetPartitionRecord      = tbl.GetPartitionRecord
	NewRowGroupStatsEvaluator = tbl.NewRowGroupStatsEvaluator
	WithRewriteSemantics    = tbl.WithRewriteSemantics
	RewriteValidatorFor     = tbl.RewriteValidatorFor

	// Scan options.
	WithSelectedFields = tbl.WithSelectedFields
	WithRowFilter      = tbl.WithRowFilter
	WithSnapshotID     = tbl.WithSnapshotID
	WithSnapshotAsOf   = tbl.WithSnapshotAsOf
	WithCaseSensitive  = tbl.WithCaseSensitive
	WithLimit          = tbl.WithLimit
	WitMaxConcurrency  = tbl.WitMaxConcurrency
	WithOptions        = tbl.WithOptions
)

// Re-exported constants.
const (
	ScanNoLimit = tbl.ScanNoLimit

	OpAppend    = tbl.OpAppend
	OpOverwrite = tbl.OpOverwrite
	OpDelete    = tbl.OpDelete
	OpReplace   = tbl.OpReplace

	MainBranch = tbl.MainBranch

	WriteTargetFileSizeBytesKey     = tbl.WriteTargetFileSizeBytesKey
	WriteTargetFileSizeBytesDefault = tbl.WriteTargetFileSizeBytesDefault
	WriteFormatDefaultKey           = tbl.WriteFormatDefaultKey
	WriteFormatDefaultDefault       = tbl.WriteFormatDefaultDefault
	WriteDataPathKey                = tbl.WriteDataPathKey

	MetricsModeColumnConfPrefix    = tbl.MetricsModeColumnConfPrefix
	DefaultWriteMetricsModeKey     = tbl.DefaultWriteMetricsModeKey
	DefaultWriteMetricsModeDefault = tbl.DefaultWriteMetricsModeDefault

	DefaultNameMappingKey = tbl.DefaultNameMappingKey

	WriteDeleteModeKey     = tbl.WriteDeleteModeKey
	WriteDeleteModeDefault = tbl.WriteDeleteModeDefault
	WriteModeCopyOnWrite   = tbl.WriteModeCopyOnWrite
	WriteModeMergeOnRead   = tbl.WriteModeMergeOnRead

	WritePartitionSummaryLimitKey     = tbl.WritePartitionSummaryLimitKey
	WritePartitionSummaryLimitDefault = tbl.WritePartitionSummaryLimitDefault

	ManifestTargetSizeBytesKey     = tbl.ManifestTargetSizeBytesKey
	ManifestTargetSizeBytesDefault = tbl.ManifestTargetSizeBytesDefault
	ManifestMinMergeCountKey       = tbl.ManifestMinMergeCountKey
	ManifestMinMergeCountDefault   = tbl.ManifestMinMergeCountDefault
	ManifestMergeEnabledKey        = tbl.ManifestMergeEnabledKey
	ManifestMergeEnabledDefault    = tbl.ManifestMergeEnabledDefault
)
