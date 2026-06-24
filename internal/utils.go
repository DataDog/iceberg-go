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

package internal

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"runtime"
	"slices"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Enumerated is a quick way to represent a sequenced value that can
// be processed in parallel and then needs to be reordered.
type Enumerated[T any] struct {
	Value T
	Index int
	Last  bool
}

type pqueue[T any] struct {
	queue   []*T
	compare func(a, b *T) bool
}

func (pq *pqueue[T]) Len() int            { return len(pq.queue) }
func (pq *pqueue[T]) Less(i, j int) bool  { return pq.compare(pq.queue[i], pq.queue[j]) }
func (pq *pqueue[T]) Swap(i, j int)       { pq.queue[i], pq.queue[j] = pq.queue[j], pq.queue[i] }
func (pq *pqueue[T]) Push(x any)          { pq.queue = append(pq.queue, x.(*T)) }
func (pq *pqueue[T]) Pop() any {
	old := pq.queue
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	pq.queue = old[0 : n-1]
	return item
}

// MakeSequencedChan creates a channel that outputs values in a given order
// based on the comesAfter and isNext functions.
func MakeSequencedChan[T any](bufferSize uint, source <-chan T, comesAfter, isNext func(a, b *T) bool, initial T) <-chan T {
	pq := pqueue[T]{queue: make([]*T, 0), compare: comesAfter}
	heap.Init(&pq)
	previous, out := &initial, make(chan T, bufferSize)
	go func() {
		defer close(out)
		for val := range source {
			heap.Push(&pq, &val)
			for pq.Len() > 0 && isNext(previous, pq.queue[0]) {
				previous = heap.Pop(&pq).(*T)
				out <- *previous
			}
		}
	}()
	return out
}

// MapExec runs fn on each element of slice concurrently across nWorkers goroutines.
func MapExec[T, S any](ctx context.Context, nWorkers int, slice iter.Seq[T], fn func(T) (S, error)) iter.Seq2[S, error] {
	if nWorkers <= 0 {
		nWorkers = runtime.GOMAXPROCS(0)
	}

	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan T, nWorkers)
	out := make(chan S, nWorkers)

	for range nWorkers {
		g.Go(func() error {
			for v := range ch {
				result, err := fn(v)
				if err != nil {
					return err
				}
				select {
				case out <- result:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}
			return nil
		})
	}

	var err error
	go func() {
		defer func() {
			close(ch)
			err = g.Wait()
			close(out)
		}()
		for v := range slice {
			select {
			case ch <- v:
			case <-ctx.Done():
				return
			}
		}
	}()

	return func(yield func(S, error) bool) {
		defer func() {
			// drain out if we exit early
			for range out {
			}
		}()

		for v := range out {
			if !yield(v, nil) {
				return
			}
		}

		if err != nil {
			var z S
			yield(z, err)
		}
	}
}

// RowGroupBloomPred holds the physical-encoded bytes for each literal in a
// bloom-filterable predicate on one field. A row group can be skipped when
// NONE of the bytes appear in the column's bloom filter.
type RowGroupBloomPred struct {
	FieldID   int
	PhysBytes [][]byte // one entry for EqualTo; one per value for In
}

// Helper function to find the difference between two slices (a - b).
func Difference(a, b []string) []string {
	m := make(map[string]bool)
	for _, item := range b {
		m[item] = true
	}

	diff := make([]string, 0)
	for _, item := range a {
		if !m[item] {
			diff = append(diff, item)
		}
	}

	return diff
}

type Bin[T any] struct {
	binWeight    int64
	targetWeight int64
	items        []T
}

func (b *Bin[T]) Weight() int64            { return b.binWeight }
func (b *Bin[T]) CanAdd(weight int64) bool { return b.binWeight+weight <= b.targetWeight }
func (b *Bin[T]) Add(item T, weight int64) {
	b.binWeight += weight
	b.items = append(b.items, item)
}

func PackingIterator[T any](itr iter.Seq[T], targetWeight int64, lookback int, weightFunc func(T) int64, largestBinFirst bool) iter.Seq[[]T] {
	bins := make([]Bin[T], 0)
	findBin := func(weight int64) *Bin[T] {
		for i := range bins {
			if bins[i].CanAdd(weight) {
				return &bins[i]
			}
		}

		return nil
	}

	removeBin := func() Bin[T] {
		if largestBinFirst {
			maxBin := slices.MaxFunc(bins, func(a, b Bin[T]) int {
				return cmp.Compare(a.Weight(), b.Weight())
			})
			i := slices.IndexFunc(bins, func(e Bin[T]) bool {
				return e.Weight() == maxBin.Weight()
			})

			bins = slices.Delete(bins, i, i+1)

			return maxBin
		}

		var out Bin[T]
		out, bins = bins[0], bins[1:]

		return out
	}

	return func(yield func([]T) bool) {
		for item := range itr {
			w := weightFunc(item)
			bin := findBin(w)
			if bin != nil {
				bin.Add(item, w)
			} else {
				bin := Bin[T]{targetWeight: targetWeight}
				bin.Add(item, w)
				bins = append(bins, bin)

				if len(bins) > lookback {
					if !yield(removeBin().items) {
						return
					}
				}
			}
		}

		for len(bins) > 0 {
			if !yield(removeBin().items) {
				return
			}
		}
	}
}

type SlicePacker[T any] struct {
	TargetWeight    int64
	Lookback        int
	LargestBinFirst bool
}

func (s *SlicePacker[T]) Pack(items []T, weightFunc func(T) int64) [][]T {
	return slices.Collect(PackingIterator(slices.Values(items), s.TargetWeight,
		s.Lookback, weightFunc, s.LargestBinFirst))
}

func (s *SlicePacker[T]) PackEnd(items []T, weightFunc func(T) int64) [][]T {
	items = slices.Clone(items)
	slices.Reverse(items)
	packed := s.Pack(items, weightFunc)
	slices.Reverse(packed)

	result := make([][]T, 0, len(packed))
	for _, items := range packed {
		slices.Reverse(items)
		result = append(result, items)
	}

	return result
}

type CountingWriter struct {
	Count int64
	W     io.Writer
}

func (w *CountingWriter) Write(p []byte) (int, error) {
	n, err := w.W.Write(p)
	w.Count += int64(n)

	return n, err
}

func RecoverError(err *error) {
	if r := recover(); r != nil {
		switch e := r.(type) {
		case string:
			*err = fmt.Errorf("error encountered during arrow schema visitor: %s", e)
		case error:
			*err = fmt.Errorf("error encountered during arrow schema visitor: %w", e)
		}
	}
}

func SingleErrorIter[T any](err error) iter.Seq2[T, error] {
	var z T

	return func(yield func(T, error) bool) {
		_ = yield(z, err)
	}
}

func Counter(start int) iter.Seq[int] {
	var current atomic.Int64
	current.Store(int64(start) - 1)

	return func(yield func(int) bool) {
		for {
			if !yield(int(current.Add(1))) {
				return
			}
		}
	}
}

// CheckedClose is a helper function to close a resource and return an error if it fails.
// It is intended to be used in a defer statement.
func CheckedClose(c io.Closer, err *error) {
	*err = errors.Join(*err, c.Close())
}

// SliceEqualHelper compares the equality of two slices whose elements have an Equals method
func SliceEqualHelper[T interface{ Equals(T) bool }](s1, s2 []T) bool {
	return slices.EqualFunc(s1, s2, func(t1, t2 T) bool {
		return t1.Equals(t2)
	})
}
