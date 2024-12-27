// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httprule

import (
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"testing"
)

// ### Path template syntax
//
//	    Template = "/" Segments [ Verb ] ;
//	    Segments = Segment { "/" Segment } ;
//	    Segment  = "*" | "**" | LITERAL | Variable ;
//	    Variable = "{" FieldPath [ "=" Segments ] "}" ;
//	    FieldPath = IDENT { "." IDENT } ;
//	    Verb     = ":" LITERAL ;

// `/v1/projects/{project}/secrets/{secret}:getIamPolicy`
// should produce:
//
//	[]PathSegment{
//	  {Literal:   "v1"},
//	  {Literal:   "projects"},
//	  {FieldPath: "project"},
//	  {Literal:   "secrets"},
//	  {FieldPath: "secret"},
//	  {Verb:      "getIamPolicy"},
//	}
func TestProtobuf_PathSegments(t *testing.T) {
	expectError(t, "foo", "path must start with slash")
	expectError(t, "/", "path cannot end with slash")
	expectError(t, "/foo/", "path cannot end with slash")

	//TODO these should work in the future, but expect an error for now, because the parser is not implemented
	expectError(t, "/foo:bar", "verb is not implemented")
	expectError(t, "/v1/{name}", "variable is not implemented")
	expectError(t, "/v1/{name=projects}", "variable with value is not implemented")
	expectError(t, "/v1/{name=*}", "variable with wildcard is not implemented")
	expectError(t, "/v1/*/foo", "path with wildcard is not implemented")
}

func TestProtobuf_PathSegmentLiterals(t *testing.T) {
	expectEqual(t, "/v1", []api.PathSegment{api.NewLiteralPathSegment("v1")})
	expectEqual(t, "/v1/foo", []api.PathSegment{
		api.NewLiteralPathSegment("v1"),
		api.NewLiteralPathSegment("foo"),
	})
}

func expectEqual(t *testing.T, path string, want []api.PathSegment) {
	t.Helper()
	got, err := Parse(path)
	if err != nil {
		t.Fatal("expected no error, got:", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("failed parsing path [%s] (-want, +got):\n%s", path, diff)
	}
}
func expectError(t *testing.T, path string, want string) {
	t.Helper()
	_, err := Parse(path)
	if err == nil {
		t.Errorf("Parse(%s) succeeded, want error: %s", path, want)
	}
}
