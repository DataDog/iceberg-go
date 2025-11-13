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
//	"encoding/json"
//	"errors"
//	"fmt"
//	"io"
//	"maps"
//	"slices"
//	"strconv"
//	"strings"
//	"time"
//
//	"github.com/DataDog/iceberg-go"
//
//	"github.com/google/uuid"
//)
//
//const (
//	// LastAddedID is used in place of ID fields (e.g. schema, version) to indicate that
//	// the last added instance of that type should be used.
//	LastAddedID = -1
//)
//
//const (
//	supportedViewFormatVersion = 1
//)
//
//// Metadata for an iceberg view as specified in the Iceberg spec
//// https://iceberg.apache.org/view-spec/
//type Metadata interface {
//	// FormatVersion indicates the version of this metadata, 1 for V1
//	FormatVersion() int
//	// ViewUUID returns a UUID that identifies the view, generated when the
//	// view is created. Implementations must throw an exception if a view's
//	// UUID does not match the expected UUID after refreshing metadata.
//	ViewUUID() uuid.UUID
//	// Location is the table's base location. This is used by writers to determine
//	// where to store data files, manifest files, and table metadata files.
//	Location() string
//	// Schemas returns the list of view schemas
//	Schemas() []*iceberg.Schema
//	// CurrentVersionID returns the ID of the current version of the view (version-id)
//	CurrentVersionID() int
//	// CurrentVersion returns the current version of the view
//	CurrentVersion() *Version
//	// CurrentSchemaID returns the ID of the current schema
//	CurrentSchemaID() int
//	// CurrentSchema returns the current schema of the view
//	CurrentSchema() *iceberg.Schema
//	// Versions returns the list of view versions
//	Versions() []*Version
//	// VersionLog returns a list of version log entries
//	// with the timestamp and version-id for every change to current-version-id
//	VersionLog() []VersionHistoryEntry
//	// Properties is a string to string map of view properties.
//	Properties() iceberg.Properties
//
//	Equals(Metadata) bool
//}
//
//// VersionSummary is string to string map of summary metadata about a view's version
//type VersionSummary map[string]string
//
//// Representation is a struct containing information about a view's representation
//// https://iceberg.apache.org/view-spec/#sql-representation
//type Representation struct {
//	// Must be sql
//	Type string `json:"type"`
//	// A SQL SELECT statement
//	Sql string `json:"sql"`
//	// The dialect of the sql SELECT statement (e.g., "trino" or "spark")
//	Dialect string `json:"dialect"`
//}
//
//func (r *Representation) Equals(other *Representation) bool {
//	return r.Type == other.Type &&
//		r.Sql == other.Sql &&
//		r.Dialect == other.Dialect
//}
//
//type Version struct {
//	// ID for the version
//	VersionID int `json:"version-id"`
//	// ID of the schema for the view version
//	SchemaID int `json:"schema-id"`
//	// Timestamp when the version was created (ms from epoch)
//	TimestampMS int64 `json:"timestamp-ms"`
//	// A string to string map of summary metadata about the version
//	Summary VersionSummary `json:"summary"`
//	// A list of representations for the view definition
//	Representations []Representation `json:"representations"`
//}
//
//// Equals checks whether the other Version would behave the same
//// while ignoring the view version id, and the creation timestamp
//func (v *Version) Equals(other *Version) bool {
//	return v.SchemaID == other.SchemaID &&
//		maps.Equal(v.Summary, other.Summary) &&
//		slices.Equal(v.Representations, other.Representations)
//}
//
//func (v *Version) Clone() *Version {
//	if v == nil {
//		return nil
//	}
//
//	cloned := *v
//	cloned.Summary = maps.Clone(v.Summary)
//	cloned.Representations = slices.Clone(v.Representations)
//	return &cloned
//}
//
//type VersionHistoryEntry struct {
//	// Timestamp when the view's current-version-id was updated (ms from epoch)
//	TimestampMS int64 `json:"timestamp-ms"`
//	// ID that current-version-id was set to
//	VersionID int `json:"version-id"`
//}
//
//type MetadataBuilder struct {
//	base    Metadata
//	updates []Update
//
//	// common fields
//	formatVersion    int
//	uuid             uuid.UUID
//	loc              string
//	schemaList       []*iceberg.Schema
//	versionList      []*Version
//	currentVersionID int
//	versionLog       []VersionHistoryEntry
//	props            iceberg.Properties
//
//	// lookup maps
//	versionsById map[int]*Version
//	schemasById  map[int]*iceberg.Schema
//
//	// update tracking
//	versionHistoryEntry *VersionHistoryEntry
//	lastAddedVersionID  *int
//	lastAddedSchemaID   *int
//}
//
//func NewMetadataBuilder() (*MetadataBuilder, error) {
//	return &MetadataBuilder{
//		updates:     make([]Update, 0),
//		schemaList:  make([]*iceberg.Schema, 0),
//		versionList: make([]*Version, 0),
//		versionLog:  make([]VersionHistoryEntry, 0),
//		props:       make(iceberg.Properties),
//	}, nil
//}
//
//func MetadataBuilderFromBase(metadata Metadata) (*MetadataBuilder, error) {
//	b := &MetadataBuilder{}
//	b.base = metadata
//
//	// Copy fields
//	b.formatVersion = metadata.FormatVersion()
//	b.uuid = metadata.ViewUUID()
//	b.loc = metadata.Location()
//	b.schemaList = slices.Clone(metadata.Schemas())
//	b.currentVersionID = metadata.CurrentVersionID()
//	b.versionList = cloneSlice(metadata.Versions())
//	b.versionLog = slices.Clone(metadata.VersionLog())
//	b.props = maps.Clone(metadata.Properties())
//
//	// Build lookup maps
//	b.versionsById = indexBy(b.versionList, func(vl *Version) int { return vl.VersionID })
//	b.schemasById = indexBy(b.schemaList, func(sc *iceberg.Schema) int { return sc.ID })
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) HasChanges() bool { return len(b.updates) > 0 }
//
//func (b *MetadataBuilder) CurrentVersion() *Version {
//	v, _ := b.GetVersionByID(b.currentVersionID)
//
//	return v
//}
//
//func (b *MetadataBuilder) CurrentSchema() *iceberg.Schema {
//	s, _ := b.GetSchemaByID(b.CurrentVersion().VersionID)
//
//	return s
//}
//
//func (b *MetadataBuilder) SetCurrentVersion(version *Version, schema *iceberg.Schema) (*MetadataBuilder, error) {
//	newSchemaID, err := b.addSchema(schema)
//	if err != nil {
//		return nil, err
//	}
//
//	newVersion := version.Clone()
//	newVersion.SchemaID = newSchemaID
//	newVersionID, err := b.addVersion(newVersion)
//	if err != nil {
//		return nil, err
//	}
//
//	return b.SetCurrentVersionID(newVersionID)
//}
//
//func (b *MetadataBuilder) AddVersion(newVersion *Version) (*MetadataBuilder, error) {
//	_, err := b.addVersion(newVersion)
//	return b, err
//}
//
//func (b *MetadataBuilder) addVersion(newVersion *Version) (int, error) {
//	newVersionID := b.reuseOrCreateNewVersionID(newVersion)
//	version := newVersion.Clone()
//	if newVersionID != version.VersionID {
//		version.VersionID = newVersionID
//	}
//
//	// Check if this version was added in an update already
//	if _, ok := b.versionsById[newVersionID]; ok {
//		for _, upd := range b.updates {
//			if vvu, ok := upd.(*addViewVersionUpdate); ok && vvu.Version.VersionID == newVersionID {
//				b.lastAddedVersionID = &newVersionID
//				return newVersionID, nil
//			}
//		}
//	}
//
//	// If the SchemaID of the version is unset (nil), we attach the lastAddedSchemaID to it.
//	// If we have no lastAddedSchemaID, we fail
//	if version.SchemaID == LastAddedID {
//		if b.lastAddedSchemaID == nil {
//			return 0, errors.New("cannot set schema for version: no schema has been added")
//		}
//		version.SchemaID = *b.lastAddedSchemaID
//	}
//
//	if _, ok := b.schemasById[version.SchemaID]; !ok {
//		return 0, fmt.Errorf("cannot add version with unknown schema: %d", version.SchemaID)
//	}
//
//	if len(version.Representations) == 0 {
//		return 0, fmt.Errorf("cannot add version with no representations")
//	}
//	dialects := make(map[string]struct{})
//	for _, repr := range version.Representations {
//		normalizedDialect := strings.ToLower(repr.Dialect)
//		if _, ok := dialects[normalizedDialect]; ok {
//			return 0, fmt.Errorf("invalid view version: cannot add multiple queries for dialect %s", normalizedDialect)
//		}
//		dialects[normalizedDialect] = struct{}{}
//	}
//
//	b.versionList = append(b.versionList, version)
//	b.versionsById[version.VersionID] = version
//
//	if b.lastAddedSchemaID != nil && version.SchemaID == *b.lastAddedSchemaID {
//		updateVersion := version.Clone()
//		updateVersion.SchemaID = LastAddedID
//		b.updates = append(b.updates, NewAddViewVersionUpdate(updateVersion))
//	} else {
//		b.updates = append(b.updates, NewAddViewVersionUpdate(version))
//	}
//
//	b.lastAddedVersionID = &newVersionID
//	return newVersionID, nil
//}
//
//func (b *MetadataBuilder) AddSchema(schema *iceberg.Schema) (*MetadataBuilder, error) {
//	_, err := b.addSchema(schema)
//	return nil, err
//}
//
//func (b *MetadataBuilder) addSchema(schema *iceberg.Schema) (int, error) {
//	newSchemaID := b.reuseOrCreateNewSchemaID(schema)
//
//	if _, ok := b.schemasById[newSchemaID]; ok {
//		if b.lastAddedSchemaID == nil || *b.lastAddedSchemaID != newSchemaID {
//			b.updates = append(b.updates, NewAddSchemaUpdate(schema))
//			b.lastAddedSchemaID = &newSchemaID
//		}
//
//		return newSchemaID, nil
//	}
//
//	schema.ID = newSchemaID
//
//	b.schemaList = append(b.schemaList, schema)
//	b.schemasById[newSchemaID] = schema
//	b.updates = append(b.updates, NewAddSchemaUpdate(schema))
//	b.lastAddedSchemaID = &newSchemaID
//
//	return newSchemaID, nil
//}
//
//func (b *MetadataBuilder) RemoveProperties(keys []string) (*MetadataBuilder, error) {
//	if len(keys) == 0 {
//		return b, nil
//	}
//
//	b.updates = append(b.updates, NewRemovePropertiesUpdate(keys))
//	for _, key := range keys {
//		delete(b.props, key)
//	}
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) SetCurrentVersionID(newVersionID int) (*MetadataBuilder, error) {
//	if newVersionID == LastAddedID {
//		if b.lastAddedVersionID == nil {
//			return nil, errors.New("can't set current version to last added, no version has been added")
//		}
//		newVersionID = *b.lastAddedVersionID
//	}
//
//	if newVersionID == b.currentVersionID {
//		return b, nil
//	}
//
//	version, ok := b.versionsById[newVersionID]
//	if !ok {
//		return nil, fmt.Errorf("can't set current version to version with id %d", newVersionID)
//	}
//
//	if b.lastAddedVersionID != nil && *b.lastAddedVersionID == newVersionID {
//		b.updates = append(b.updates, NewSetCurrentVersionUpdate(LastAddedID))
//	} else {
//		b.updates = append(b.updates, NewSetCurrentVersionUpdate(newVersionID))
//	}
//	b.currentVersionID = newVersionID
//
//	// Set the current history entry
//	updateTimestampMS := version.TimestampMS
//	for _, update := range b.updates {
//		if v, ok := update.(*addViewVersionUpdate); ok && v.Version.VersionID == newVersionID {
//			updateTimestampMS = time.Now().UnixMilli()
//			break
//		}
//	}
//
//	b.versionHistoryEntry = &VersionHistoryEntry{
//		VersionID:   version.VersionID,
//		TimestampMS: updateTimestampMS,
//	}
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) SetFormatVersion(formatVersion int) (*MetadataBuilder, error) {
//	if formatVersion < b.formatVersion {
//		return nil, fmt.Errorf("downgrading format version from %d to %d is not allowed",
//			b.formatVersion, formatVersion)
//	}
//
//	if formatVersion > supportedViewFormatVersion {
//		return nil, fmt.Errorf("unsupported format version %d", formatVersion)
//	}
//
//	if formatVersion == b.formatVersion {
//		return b, nil
//	}
//
//	b.updates = append(b.updates, NewUpgradeFormatVersionUpdate(formatVersion))
//	b.formatVersion = formatVersion
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) SetLoc(loc string) (*MetadataBuilder, error) {
//	if b.loc == loc {
//		return b, nil
//	}
//
//	b.updates = append(b.updates, NewSetLocationUpdate(loc))
//	b.loc = loc
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) SetProperties(props iceberg.Properties) (*MetadataBuilder, error) {
//	if len(props) == 0 {
//		return b, nil
//	}
//
//	b.updates = append(b.updates, NewSetPropertiesUpdate(props))
//	if b.props == nil {
//		b.props = props
//	} else {
//		maps.Copy(b.props, props)
//	}
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) SetUUID(uuid uuid.UUID) (*MetadataBuilder, error) {
//	if b.uuid == uuid {
//		return b, nil
//	}
//
//	b.updates = append(b.updates, NewAssignUUIDUpdate(uuid))
//	b.uuid = uuid
//
//	return b, nil
//}
//
//func (b *MetadataBuilder) buildCommonMetadata() (*commonMetadata, error) {
//	md := &commonMetadata{
//		FormatVersionValue:    b.formatVersion,
//		UUID:                  b.uuid,
//		Loc:                   b.loc,
//		CurrentVersionIDValue: b.currentVersionID,
//		VersionList:           b.versionList,
//		VersionHistoryList:    b.versionLog,
//		SchemaList:            b.schemaList,
//		Props:                 b.props,
//	}
//	md.init()
//
//	return md, nil
//}
//
//func (b *MetadataBuilder) GetVersionByID(id int) (*Version, error) {
//	if v, ok := b.versionsById[id]; ok {
//		return v, nil
//	}
//
//	return nil, fmt.Errorf("%w: version with id %d not found", iceberg.ErrInvalidArgument, id)
//}
//
//func (b *MetadataBuilder) GetSchemaByID(id int) (*iceberg.Schema, error) {
//	if s, ok := b.schemasById[id]; ok {
//		return s, nil
//	}
//
//	return nil, fmt.Errorf("%w: schema with id %d not found", iceberg.ErrInvalidArgument, id)
//}
//
//func (b *MetadataBuilder) Build() (Metadata, error) {
//	common, err := b.buildCommonMetadata()
//	if err != nil {
//		return nil, err
//	}
//	if err := common.validate(); err != nil {
//		return nil, err
//	}
//
//	if b.formatVersion != supportedViewFormatVersion {
//		return nil, fmt.Errorf("unsupported format version %d", b.formatVersion)
//	}
//
//	b.versionLog = append(b.versionLog, *b.versionHistoryEntry)
//
//	return &metadataV1{
//		*common,
//	}, nil
//}
//
//func (b *MetadataBuilder) reuseOrCreateNewVersionID(newVersion *Version) int {
//	newVersionID := newVersion.VersionID
//	for _, version := range b.versionList {
//		if newVersion.Equals(version) {
//			return version.VersionID
//		}
//		if version.VersionID >= newVersionID {
//			newVersionID = version.VersionID + 1
//		}
//	}
//
//	return newVersionID
//}
//
//func (b *MetadataBuilder) reuseOrCreateNewSchemaID(newSchema *iceberg.Schema) int {
//	newSchemaID := newSchema.ID
//	for _, schema := range b.schemaList {
//		if newSchema.Equals(schema) {
//			return schema.ID
//		}
//		if schema.ID >= newSchemaID {
//			newSchemaID = schema.ID + 1
//		}
//	}
//
//	return newSchemaID
//}
//
//// maxBy returns the maximum value of extract(e) for all e in elems.
//// If elems is empty, returns 0.
//func maxBy[S ~[]E, E any](elems S, extract func(e E) int) int {
//	m := 0
//	for _, e := range elems {
//		m = max(m, extract(e))
//	}
//
//	return m
//}
//
//var (
//	ErrInvalidMetadataFormatVersion = errors.New("invalid or missing format-version in table metadata")
//	ErrInvalidMetadata              = errors.New("invalid metadata")
//)
//
//// ParseMetadata parses json metadata provided by the passed in reader,
//// returning an error if one is encountered.
//func ParseMetadata(r io.Reader) (Metadata, error) {
//	data, err := io.ReadAll(r)
//	if err != nil {
//		return nil, err
//	}
//
//	return ParseMetadataBytes(data)
//}
//
//// ParseMetadataString is like [ParseMetadata], but for a string rather than
//// an io.Reader.
//func ParseMetadataString(s string) (Metadata, error) {
//	return ParseMetadataBytes([]byte(s))
//}
//
//// ParseMetadataBytes is like [ParseMetadataString] but for a byte slice.
//func ParseMetadataBytes(b []byte) (Metadata, error) {
//	ver := struct {
//		FormatVersion int `json:"format-version"`
//	}{}
//	if err := json.Unmarshal(b, &ver); err != nil {
//		return nil, err
//	}
//
//	var ret Metadata
//	switch ver.FormatVersion {
//	case 1:
//		ret = &metadataV1{}
//	default:
//		return nil, ErrInvalidMetadataFormatVersion
//	}
//
//	return ret, json.Unmarshal(b, ret)
//}
//
//func sliceEqualHelper[T interface{ Equals(T) bool }](s1, s2 []T) bool {
//	return slices.EqualFunc(s1, s2, func(t1, t2 T) bool {
//		return t1.Equals(t2)
//	})
//}
//
//// indexBy indexes a slice into a map, using a provided extractKey function
//// The extractKey function will be called on each item in the slice and assign the
//// item as a value for that key in the resultant map.
//func indexBy[T any, K comparable](s []T, extractKey func(T) K) map[K]T {
//	index := make(map[K]T)
//	for _, v := range s {
//		index[extractKey(v)] = v
//	}
//	return index
//}
//
//// Cloner is an interface which implements a Clone method for deep copying itself
//type cloner[T any] interface {
//	// Clone returns a deep copy of the underlying object
//	Clone() T
//}
//
//// cloneSlice returns a deep-clone of a Slice of elements implementing Cloner
//func cloneSlice[T cloner[T]](val []T) []T {
//	cloned := make([]T, len(val))
//	for i, elem := range val {
//		cloned[i] = elem.Clone()
//	}
//	return cloned
//}
//
//// https://iceberg.apache.org/view-spec/
//type commonMetadata struct {
//	FormatVersionValue    int                   `json:"format-version"`
//	UUID                  uuid.UUID             `json:"view-uuid"`
//	Loc                   string                `json:"location"`
//	CurrentVersionIDValue int                   `json:"current-version-id"`
//	VersionList           []*Version            `json:"versions"`
//	SchemaList            []*iceberg.Schema     `json:"schemas"`
//	VersionHistoryList    []VersionHistoryEntry `json:"version-log"`
//	Props                 iceberg.Properties    `json:"properties,omitempty"`
//
//	// cached lookup helpers, must be initialized in init()
//	versionsByID map[int]*Version
//	schemasByID  map[int]*iceberg.Schema
//}
//
//func (c *commonMetadata) Equals(other *commonMetadata) bool {
//	if other == nil {
//		return false
//	}
//
//	if c == other {
//		return true
//	}
//
//	panic("implement me")
//}
//
//func (c *commonMetadata) FormatVersion() int         { return c.FormatVersionValue }
//func (c *commonMetadata) ViewUUID() uuid.UUID        { return c.UUID }
//func (c *commonMetadata) Location() string           { return c.Loc }
//func (c *commonMetadata) Versions() []*Version       { return c.VersionList }
//func (c *commonMetadata) Schemas() []*iceberg.Schema { return c.SchemaList }
//func (c *commonMetadata) CurrentVersionID() int {
//	return c.CurrentVersionIDValue
//}
//func (c *commonMetadata) CurrentVersion() *Version {
//	version, ok := c.versionsByID[c.CurrentVersionIDValue]
//	if !ok {
//		panic("current version not found")
//	}
//	return version
//}
//func (c *commonMetadata) CurrentSchemaID() int {
//	return c.CurrentVersion().SchemaID
//}
//func (c *commonMetadata) CurrentSchema() *iceberg.Schema {
//	schema, ok := c.schemasByID[c.CurrentSchemaID()]
//	if !ok {
//		panic("current schema not found")
//	}
//	return schema
//}
//func (c *commonMetadata) VersionLog() []VersionHistoryEntry {
//	return c.VersionHistoryList
//}
//
//func (c *commonMetadata) Properties() iceberg.Properties {
//	return c.Props
//}
//
//func (c *commonMetadata) validate() error {
//	if len(c.VersionList) == 0 {
//		return errors.New("invalid view: no versions were added")
//	}
//
//	if len(c.SchemaList) == 0 {
//		return errors.New("invalid view: no schemas were added")
//	}
//
//	return nil
//}
//
//// init performs state initialization for commonMetadata instances,
//// such as constructing the version and schema indexes.
//// It should be called on a new commonMetadata instance before returning
//// to a caller.
//func (c *commonMetadata) init() {
//	c.versionsByID = indexBy(c.VersionList, func(v *Version) int { return v.VersionID })
//	c.schemasByID = indexBy(c.SchemaList, func(v *iceberg.Schema) int { return v.ID })
//}
//
//type metadataV1 struct {
//	commonMetadata
//}
//
//func (m *metadataV1) Equals(other Metadata) bool {
//	rhs, ok := other.(*metadataV1)
//	if !ok {
//		return false
//	}
//
//	if m == rhs {
//		return true
//	}
//
//	return m.commonMetadata.Equals(&rhs.commonMetadata)
//}
//
//func (m *metadataV1) UnmarshalJSON(b []byte) error {
//	type Alias metadataV1
//	aux := (*Alias)(m)
//
//	if err := json.Unmarshal(b, aux); err != nil {
//		return err
//	}
//
//	m.init()
//
//	return m.validate()
//}
//
//const DefaultFormatVersion = supportedViewFormatVersion
//
//// NewMetadata creates a new view metadata object using the provided schema, information, generating a fresh UUID for
//// the new table metadata.
//func NewMetadata(sc *iceberg.Schema, location string, props iceberg.Properties) (Metadata, error) {
//	return NewMetadataWithUUID(sc, location, props, uuid.Nil)
//}
//
//// NewMetadataWithUUID is like NewMetadata, but allows the caller to specify the UUID of the view rather than creating a new one.
//func NewMetadataWithUUID(sc *iceberg.Schema, location string, props iceberg.Properties, tableUuid uuid.UUID) (Metadata, error) {
//	freshSchema, err := iceberg.AssignFreshSchemaIDs(sc, nil)
//	if err != nil {
//		return nil, err
//	}
//
//	if tableUuid == uuid.Nil {
//		tableUuid = uuid.New()
//	}
//
//	formatVersion := DefaultFormatVersion
//	if props != nil {
//		verStr, ok := props["format-version"]
//		if ok {
//			if formatVersion, err = strconv.Atoi(verStr); err != nil {
//				formatVersion = DefaultFormatVersion
//			}
//			delete(props, "format-version")
//		}
//	}
//
//	common := commonMetadata{
//		FormatVersionValue:    formatVersion,
//		UUID:                  tableUuid,
//		Loc:                   location,
//		SchemaList:            []*iceberg.Schema{freshSchema},
//		CurrentVersionIDValue: freshSchema.ID,
//		Props:                 props,
//	}
//
//	switch formatVersion {
//	case 1:
//		return &metadataV1{
//			commonMetadata: common,
//		}, nil
//	default:
//		return nil, fmt.Errorf("invalid format version: %d", formatVersion)
//	}
//}
