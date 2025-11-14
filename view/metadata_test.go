package view

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		Representations: representations,
	}
	v2 := &Version{
		VersionID:       1,
		SchemaID:        1,
		TimestampMS:     0,
		Summary:         summary,
		Representations: representations,
	}
	assert.True(t, v1.Equals(v2), fmt.Sprintf("Expected the same SchemaID, Summary, Representation for %v, got %v", v1, v2))
}
