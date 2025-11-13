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

package view

//import (
//	"context"
//	"iter"
//	"log"
//	"runtime"
//	"slices"
//
//	"github.com/DataDog/iceberg-go"
//	"github.com/DataDog/iceberg-go/internal"
//	"github.com/DataDog/iceberg-go/io"
//	tblutils "github.com/DataDog/iceberg-go/table/internal"
//	"github.com/apache/arrow-go/v18/arrow"
//	"github.com/apache/arrow-go/v18/arrow/array"
//	"golang.org/x/sync/errgroup"
//)
//
//type FSysF func(ctx context.Context) (io.IO, error)
//
//type Identifier = []string
//
//type CatalogIO interface {
//	LoadView(context.Context, Identifier) (*View, error)
//	CommitView(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error)
//}
//
//type View struct {
//	identifier       Identifier
//	metadata         Metadata
//	metadataLocation string
//	cat              CatalogIO
//	fsF              FSysF
//}
//
//func (t View) Equals(other View) bool {
//	return slices.Equal(t.identifier, other.identifier) &&
//		t.metadataLocation == other.metadataLocation &&
//		t.metadata.Equals(other.metadata)
//}
//
//func (t View) Identifier() Identifier               { return t.identifier }
//func (t View) Metadata() Metadata                   { return t.metadata }
//func (t View) MetadataLocation() string             { return t.metadataLocation }
//func (t View) CurrentVersion() *Version             { return t.metadata.CurrentVersion() }
//func (t View) CurrentSchema() *iceberg.Schema       { return t.metadata.CurrentSchema() }
//func (t View) Properties() iceberg.Properties       { return t.metadata.Properties() }
//func (t View) NameMapping() iceberg.NameMapping     { return t.metadata.NameMapping() }
//func (t View) Location() string                     { return t.metadata.Location() }
//func (t View) SnapshotByName(name string) *Snapshot { return t.metadata.SnapshotByName(name) }
//func (t View) Schemas() map[int]*iceberg.Schema {
//	m := make(map[int]*iceberg.Schema)
//	for _, s := range t.metadata.Schemas() {
//		m[s.ID] = s
//	}
//
//	return m
//}
//
//func New(ident Identifier, meta Metadata, metadataLocation string, fsF FSysF, cat CatalogIO) *View {
//	return &View{
//		identifier:       ident,
//		metadata:         meta,
//		metadataLocation: metadataLocation,
//		fsF:              fsF,
//		cat:              cat,
//	}
//}
//
//func NewFromLocation(
//	ctx context.Context,
//	ident Identifier,
//	metalocation string,
//	fsysF FSysF,
//	cat CatalogIO,
//) (*View, error) {
//	var meta Metadata
//
//	fsys, err := fsysF(ctx)
//	if err != nil {
//		return nil, err
//	}
//	if rf, ok := fsys.(io.ReadFileIO); ok {
//		data, err := rf.ReadFile(metalocation)
//		if err != nil {
//			return nil, err
//		}
//
//		if meta, err = ParseMetadataBytes(data); err != nil {
//			return nil, err
//		}
//	} else {
//		f, err := fsys.Open(metalocation)
//		if err != nil {
//			return nil, err
//		}
//		defer f.Close()
//
//		if meta, err = ParseMetadata(f); err != nil {
//			return nil, err
//		}
//	}
//
//	return New(ident, meta, metalocation, fsysF, cat), nil
//}
