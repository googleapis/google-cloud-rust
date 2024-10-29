package internal

import (
	"testing"

	"github.com/iancoleman/strcase"
)

func TestToSnakeCase(t *testing.T) {
	msg := strcase.ToSnake("FooBar")
	if msg != "foo_bar" {
		t.Fatalf(`ToSnake("FooBar") = %q, want "foo_bar"`, msg)
	}
}
