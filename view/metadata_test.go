package view

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildChainer is a helper for chaining build commands without error handling
// the build chainer requires that all errors are nil before proceeding.
type buildChainer struct {
	*MetadataBuilder
	t *testing.T
}

func newCB(t *testing.T) *buildChainer {
	return &buildChainer{
		newTestBuilder(),
		t,
	}
}

func (bc *buildChainer) chain(mb *MetadataBuilder, err error) *buildChainer {
	require.NoError(bc.t, err)
	bc.MetadataBuilder = mb
	return bc
}

func newTestBuilder() *MetadataBuilder {
	b, _ := NewMetadataBuilder()
	return b
}

func newTestVersion(versionID, schemaID int, opts ...VersionOpt) *Version {
	version, _ := NewVersion(
		versionID,
		schemaID,
		Representations{NewRepresentation("select * from table", "sql")},
		table.Identifier{"defaultns"},
		opts...)
	return version
}

func newTestVersionWithSQL(versionID, schemaID int, sql string, opts ...VersionOpt) *Version {
	version, _ := NewVersion(
		versionID,
		schemaID,
		Representations{NewRepresentation(sql, "spark")},
		table.Identifier{"defaultns"},
		opts...)
	return version
}

func newTestSchema(schemaID int, optFieldName ...string) *iceberg.Schema {
	fieldName := "x"
	if len(optFieldName) > 0 {
		fieldName = optFieldName[0]
	}
	return iceberg.NewSchema(schemaID, iceberg.NestedField{ID: 1, Name: fieldName, Type: iceberg.PrimitiveTypes.Int64})
}

// Test the Equals method on the Version struct
func TestVersionEquals(t *testing.T) {
	summary := VersionSummary{"foo.bar": "foobar"}
	representations := []Representation{
		{"sql", "SELECT * FROM my.table", "spark"},
		{"sql", "SELECT * FROM my.table", "trino"},
	}
	v1 := &Version{
		VersionID:       1,
		SchemaID:        1,
		TimestampMS:     0,
		Summary:         summary,
		Representations: slices.Clone(representations),
	}
	v2 := &Version{
		VersionID:       v1.VersionID + 1,
		SchemaID:        1,
		TimestampMS:     v1.TimestampMS + 1,
		Summary:         summary,
		Representations: slices.Clone(representations),
	}
	assert.True(t, v1.Equals(v2), fmt.Sprintf("Expected the same SchemaID, Summary, Representation for %v, got %v", v1, v2))
}

func TestBuild_NullAndMissingFields(t *testing.T) {
	// Empty location
	md, err := newTestBuilder().Build()
	assert.Nil(t, md)
	assert.ErrorContains(t, err, "invalid location")

	// Missing versions
	builder, err := newTestBuilder().SetLoc("location")
	require.NoError(t, err)
	md, err = builder.Build()
	assert.Nil(t, md)
	assert.ErrorContains(t, err, "invalid view: no versions were added")

	// Attempted setting to missing version ID
	builder, err = newTestBuilder().SetLoc("location")
	require.NoError(t, err)
	_, err = builder.SetCurrentVersionID(1)
	assert.ErrorContains(t, err, "cannot set current version to unknown version with id")
	md, err = builder.Build()
	assert.Nil(t, md)
	assert.ErrorContains(t, err, "invalid view: no versions were added")

	// Invalid UUID
	builder, err = newTestBuilder().SetUUID(uuid.Nil)
	assert.ErrorContains(t, err, "cannot set uuid to null")
}

func TestSetFormatVersion_Invalid(t *testing.T) {
	// Downgrade
	_, err := newTestBuilder().SetFormatVersion(0)
	assert.ErrorContains(t, err, fmt.Sprintf("downgrading format version from %d to %d is not allowed", DefaultViewFormatVersion, 0))

	// Invalid upgrade
	_, err = newTestBuilder().SetFormatVersion(supportedViewFormatVersion + 1)
	assert.ErrorContains(t, err, fmt.Sprintf("unsupported format version %d", supportedViewFormatVersion+1))
}

func TestAddVersion_EmptySchemas(t *testing.T) {
	_, err := newTestBuilder().AddVersion(newTestVersion(1, 1))
	assert.ErrorContains(t, err, "cannot add version with unknown schema")
}

func TestSetCurrentVersionID_LastAdded(t *testing.T) {
	testSchemaID := 1
	bc := newCB(t)
	builder := bc.
		chain(bc.SetLoc("location")).
		chain(bc.AddSchema(newTestSchema(testSchemaID))).
		chain(bc.AddVersion(newTestVersion(1, LastAddedID))).
		chain(bc.SetCurrentVersionID(LastAddedID))

	md, err := builder.Build()
	assert.NotNil(t, md)
	assert.NoError(t, err)
}

func TestSetCurrentVersionID_Invalid(t *testing.T) {
	testSchemaID := 1
	builder, err := newTestBuilder().AddSchema(newTestSchema(testSchemaID))
	require.NoError(t, err)

	builder, err = builder.AddVersion(newTestVersion(1, testSchemaID))
	require.NoError(t, err)

	_, err = builder.SetCurrentVersionID(23)
	assert.ErrorContains(t, err, "cannot set current version to unknown version with id")
}

func TestAddVersion_InvalidSchemaID(t *testing.T) {
	testSchemaID := 1
	builder, err := newTestBuilder().AddSchema(newTestSchema(testSchemaID))
	require.NoError(t, err)

	builder, err = builder.AddVersion(newTestVersion(1, 23))
	assert.ErrorContains(t, err, "cannot add version with unknown schema: 23")
}

func TestViewVersionHistory_MaintainsCorrectTimeline(t *testing.T) {
	v1 := newTestVersion(1, 1, WithTimestampMS(1000))
	v2 := newTestVersion(2, 1, WithTimestampMS(2000))
	v2.Representations[0].Dialect = "differentdialect"

	cb := newCB(t)
	// Build metadata for version V1 as current
	viewMD, err := cb.chain(cb.SetLoc("location")).
		chain(cb.AddSchema(newTestSchema(1))).
		chain(cb.AddVersion(v1)).
		chain(cb.AddVersion(v2)).
		chain(cb.SetCurrentVersionID(1)).
		Build()
	require.NoError(t, err)

	// Build updated for v2 as current
	builder, err := MetadataBuilderFromBase(viewMD)
	require.NoError(t, err)
	timeBeforeAdd := time.Now().UnixMilli()
	builder, err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)
	updatedMD, err := builder.Build()
	require.NoError(t, err)

	require.Len(t, updatedMD.VersionLog(), 2)
	assert.Equal(t, VersionHistoryEntry{VersionID: 1, TimestampMS: 1000}, updatedMD.VersionLog()[0])
	// Since second build updated current version to a previously added version, it should have used
	// system time
	assert.Equal(t, 2, updatedMD.VersionLog()[1].VersionID)
	assert.GreaterOrEqual(t, updatedMD.VersionLog()[1].TimestampMS, timeBeforeAdd)

	// Add third version and set current version, it should use the latest timestamp
	v3 := newTestVersion(3, 1, WithTimestampMS(3000))
	v3.Representations[0].Dialect = "otherotherdialect"
	builder, err = MetadataBuilderFromBase(updatedMD)
	require.NoError(t, err)
	bc := &buildChainer{builder, t}
	v3MD, err := bc.chain(builder.AddVersion(v3)).chain(builder.SetCurrentVersionID(3)).Build()
	require.NoError(t, err)
	// Should have final version history entry with timestamp 3000
	expectedLog := append(slices.Clone(updatedMD.VersionLog()), VersionHistoryEntry{VersionID: 3, TimestampMS: 3000})
	assert.Equal(t, expectedLog, v3MD.VersionLog())
}

func TestViewMetadataAndUpdates(t *testing.T) {
	// Test schemas
	s1 := newTestSchema(0, "x")
	s2 := newTestSchema(1, "y")

	// Test versions
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(2, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(3, 1, "select count(*) as count from ns.tbl")

	// Build metadata
	uuid_ := uuid.New()
	props := iceberg.Properties{"k1": "v1", "k2": "v2"}
	cb := newCB(t)
	md, err := cb.chain(cb.SetUUID(uuid_)).
		chain(cb.SetLoc("location")).
		chain(cb.SetProperties(props)).
		chain(cb.AddSchema(s1)).
		chain(cb.AddSchema(s2)).
		chain(cb.AddVersion(v1)).
		chain(cb.AddVersion(v2)).
		chain(cb.AddVersion(v3)).
		chain(cb.SetCurrentVersionID(3)).
		Build()
	require.NoError(t, err)

	// Metadata properties
	assert.Len(t, md.Versions(), 3)
	assert.Equal(t, []*Version{v1, v2, v3}, md.Versions())
	assert.Len(t, md.VersionLog(), 1)
	assert.Equal(t, md.CurrentVersionID(), 3)
	assert.Equal(t, *md.CurrentVersion(), *v3)
	assert.Equal(t, props, md.Properties())
	assert.Equal(t, "location", md.Location())
	assert.Equal(t, []*iceberg.Schema{s1, s2}, md.Schemas())
	assert.Equal(t, s2.ID, md.CurrentSchemaID())

	// Updates
	require.Len(t, md.Updates(), 9)

	assert.Equal(t, NewAssignUUIDUpdate(uuid_), md.Updates()[0])
	assert.Equal(t, NewSetLocationUpdate("location"), md.Updates()[1])
	assert.Equal(t, NewSetPropertiesUpdate(props), md.Updates()[2])
	assert.Equal(t, NewAddSchemaUpdate(s1), md.Updates()[3])
	assert.Equal(t, NewAddSchemaUpdate(s2), md.Updates()[4])
	assert.Equal(t, NewAddViewVersionUpdate(v1), md.Updates()[5])
	assert.Equal(t, NewAddViewVersionUpdate(v2), md.Updates()[6])
	// We use last added ID (-1) where applicable
	expectedV3 := v3.Clone()
	expectedV3.SchemaID = LastAddedID
	assert.Equal(t, NewAddViewVersionUpdate(expectedV3), md.Updates()[7])
	assert.Equal(t, NewSetCurrentVersionUpdate(LastAddedID), md.Updates()[8])
}

func TestSetUUID(t *testing.T) {
	uuid_ := uuid.New()
	cb := newCB(t)
	md, err := cb.chain(cb.SetUUID(uuid_)).
		chain(cb.SetLoc("location")).
		chain(cb.AddSchema(newTestSchema(0))).
		chain(cb.AddVersion(newTestVersion(1, 0))).
		chain(cb.SetCurrentVersionID(1)).
		Build()
	require.NoError(t, err)
	assert.Equal(t, uuid_, md.ViewUUID())

	// Should carry over to noop rebuild.
	updatedBuilder, err := MetadataBuilderFromBase(md)
	require.NoError(t, err)
	updatedMD, err := updatedBuilder.Build()
	require.NoError(t, err)
	assert.Equal(t, uuid_, updatedMD.ViewUUID())
	assert.Empty(t, updatedMD.Updates())

	// Reassignment to same UUID should be a noop with no changes
	updatedBuilder, err = MetadataBuilderFromBase(md)
	require.NoError(t, err)
	cb = &buildChainer{updatedBuilder, t}
	updatedMD, err = cb.chain(cb.SetUUID(uuid_)).Build()
	require.NoError(t, err)
	assert.Equal(t, uuid_, updatedMD.ViewUUID())
	assert.Empty(t, updatedMD.Updates())

	// Reassignment to different UUID should fail
	updatedBuilder, err = MetadataBuilderFromBase(md)
	require.NoError(t, err)
	_, err = cb.SetUUID(uuid.New())
	require.ErrorContains(t, err, "cannot reassign uuid")
}

func TestAddVersion_IDReassignment(t *testing.T) {
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(1, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(1, 0, "select count(*) as count from ns.tbl")
	cb := newCB(t)
	md, err := cb.chain(cb.SetLoc("location")).
		chain(cb.AddSchema(newTestSchema(0))).
		chain(cb.AddVersion(v1)).
		chain(cb.AddVersion(v2)).
		chain(cb.AddVersion(v3)).
		chain(cb.SetCurrentVersionID(3)).
		Build()
	require.NoError(t, err)

	// IDs should be mutated
	expectedV2 := v2.Clone()
	expectedV2.VersionID++
	expectedV3 := v3.Clone()
	expectedV3.VersionID += 2
	assert.Equal(t, expectedV3, md.CurrentVersion())
	assert.Equal(t, []*Version{v1, expectedV2, expectedV3}, md.Versions())
}

type clonable struct {
	foo []int
	bar int
}

func (c *clonable) Clone() *clonable {
	cloned := *c
	cloned.foo = slices.Clone(c.foo)
	return &cloned
}

func TestCloneSlice(t *testing.T) {
	x := []*clonable{{[]int{1, 2, 3}, 4}}
	clonedX := cloneSlice(x)
	assert.EqualValues(t, x, clonedX)
	clonedX[0].foo[0] = 5
	assert.NotEqualValues(t, x, clonedX)
}
