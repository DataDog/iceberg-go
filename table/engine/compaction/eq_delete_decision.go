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

package compaction

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/DataDog/iceberg-go"
)

// SurvivorSurvey describes the surviving data files in a snapshot
// AFTER a planned rewrite has logically removed its rewrite set. It
// is the input to [DecideDeadEqualityDeletes].
//
// EmptyPartMinSeq is the smallest sequence number among unpartitioned
// surviving data files. Per the Iceberg v2 reader predicate (see
// table/scanner.go matchEqualityDeletesToData), an unpartitioned data
// file applies to every equality delete — so it is always part of an
// eq-delete's "applicable survivors" minimum.
//
// PartMinSeq maps the partition tuple (encoded via partitionMatchKey)
// to the smallest sequence number among surviving partitioned data
// files in that tuple. The key intentionally does NOT include
// SpecID — the reader's predicate ignores SpecID, so the writer-side
// cleanup must too.
//
// Sentinel "no survivor in this bucket" is math.MaxInt64.
type SurvivorSurvey struct {
	EmptyPartMinSeq int64
	PartMinSeq      map[string]int64
}

// NewSurvivorSurvey returns a survey initialized with the no-survivor
// sentinel for the empty-partition bucket and an empty per-partition
// map. Callers populate via AddSurvivor.
func NewSurvivorSurvey() *SurvivorSurvey {
	return &SurvivorSurvey{
		EmptyPartMinSeq: math.MaxInt64,
		PartMinSeq:      make(map[string]int64),
	}
}

// AddSurvivor records a surviving data file's (partition, seq) into
// the survey. partition is the data file's partition tuple from
// iceberg.DataFile.Partition() (nil/empty for unpartitioned tables);
// seq is the data file's sequence number from the manifest entry.
func (s *SurvivorSurvey) AddSurvivor(partition map[int]any, seq int64) {
	if seq < 0 {
		seq = 0
	}
	if len(partition) == 0 {
		s.EmptyPartMinSeq = min(seq, s.EmptyPartMinSeq)

		return
	}
	key := partitionMatchKey(partition)
	if cur, ok := s.PartMinSeq[key]; ok {
		seq = min(seq, cur)
	}
	s.PartMinSeq[key] = seq
}

func (s *SurvivorSurvey) applicableMinSeq(eqPartition map[int]any) int64 {
	if len(eqPartition) == 0 {
		if len(s.PartMinSeq) == 0 {
			return s.EmptyPartMinSeq
		}

		return min(s.EmptyPartMinSeq, slices.Min(slices.Collect(maps.Values(s.PartMinSeq))))
	}
	if v, ok := s.PartMinSeq[partitionMatchKey(eqPartition)]; ok {
		return min(s.EmptyPartMinSeq, v)
	}

	return s.EmptyPartMinSeq
}

// DecideDeadEqualityDeletes returns the eq-delete files from candidates
// that no surviving data file could ever apply to.
func DecideDeadEqualityDeletes(survey *SurvivorSurvey, candidates []iceberg.ManifestEntry) []iceberg.DataFile {
	if survey == nil || len(candidates) == 0 {
		return nil
	}

	dead := make([]iceberg.DataFile, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, e := range candidates {
		df := e.DataFile()
		path := df.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		if e.SequenceNum() < 0 {
			continue
		}
		if survey.applicableMinSeq(df.Partition()) >= e.SequenceNum() {
			seen[path] = struct{}{}
			dead = append(dead, df)
		}
	}

	return dead
}

func partitionBucketKey(specID int32, part map[int]any) string {
	if len(part) == 0 {
		return fmt.Sprintf("%d:_", specID)
	}

	return string(appendPartitionTuple(fmt.Appendf(nil, "%d:", specID), part))
}

func partitionMatchKey(part map[int]any) string {
	if len(part) == 0 {
		return ""
	}

	return string(appendPartitionTuple(nil, part))
}

func appendPartitionTuple(dst []byte, part map[int]any) []byte {
	ids := make([]int, 0, len(part))
	for id := range part {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		dst = fmt.Appendf(dst, "%d=%v;", id, part[id])
	}

	return dst
}
