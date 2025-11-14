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

package table

import (
	"context"
	"slices"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/io"
)

type View struct {
	identifier       Identifier
	metadata         ViewMetadata
	metadataLocation string
}

func (t View) Equals(other View) bool {
	return slices.Equal(t.identifier, other.identifier) &&
		t.metadataLocation == other.metadataLocation &&
		t.metadata.Equals(other.metadata)
}

func (t View) Identifier() Identifier           { return t.identifier }
func (t View) Metadata() ViewMetadata           { return t.metadata }
func (t View) MetadataLocation() string         { return t.metadataLocation }
func (t View) CurrentVersion() *ViewVersion     { return t.metadata.CurrentVersion() }
func (t View) CurrentSchema() *iceberg.Schema   { return t.metadata.CurrentSchema() }
func (t View) Properties() iceberg.Properties   { return t.metadata.Properties() }
func (t View) Location() string                 { return t.metadata.Location() }
func (t View) Versions() []*ViewVersion         { return t.metadata.Versions() }
func (t View) Schemas() map[int]*iceberg.Schema { return t.metadata.SchemasByID() }

func NewView(ident Identifier, meta ViewMetadata, metadataLocation string) *View {
	return &View{
		identifier:       ident,
		metadata:         meta,
		metadataLocation: metadataLocation,
	}
}

func NewViewFromLocation(
	ctx context.Context,
	ident Identifier,
	metalocation string,
	fsysF FSysF,
) (*View, error) {
	var meta ViewMetadata

	fsys, err := fsysF(ctx)
	if err != nil {
		return nil, err
	}
	if rf, ok := fsys.(io.ReadFileIO); ok {
		data, err := rf.ReadFile(metalocation)
		if err != nil {
			return nil, err
		}

		if meta, err = ParseViewMetadataBytes(data); err != nil {
			return nil, err
		}
	} else {
		f, err := fsys.Open(metalocation)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		if meta, err = ParseViewMetadata(f); err != nil {
			return nil, err
		}
	}

	return NewView(ident, meta, metalocation), nil
}
