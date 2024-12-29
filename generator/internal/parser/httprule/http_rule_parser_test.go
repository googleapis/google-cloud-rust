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
	"github.com/google/go-cmp/cmp/cmpopts"
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

func TestProtobuf_Parse(t *testing.T) {
	tests := []struct {
		path        string
		want        []api.PathSegment
		explanation string
	}{
		{"/foo", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
		}, ""},
		{"/foo/bar", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewLiteralPathSegment("bar"),
		}, ""},
		{"/v1/*/foo", []api.PathSegment{
			api.NewLiteralPathSegment("v1"),
			api.NewLiteralPathSegment("*"),
			api.NewLiteralPathSegment("foo"),
		}, ""},
		{"/v1/**/foo", []api.PathSegment{
			api.NewLiteralPathSegment("v1"),
			api.NewLiteralPathSegment("**"),
			api.NewLiteralPathSegment("foo"),
		}, ""},
		{"/foo:bar", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewVerbPathSegment("bar"),
		}, ""},

		{"/foo/{bar}", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewFieldPathPathSegment("bar"),
		}, ""},
		{"/foo/{bar=baz}", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewFieldPathPathSegment("bar"),
		}, ""},
		{"/foo/{bar=*}", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewFieldPathPathSegment("bar"),
		}, ""},
		{"/foo/{bar=*}/baz", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewFieldPathPathSegment("bar"),
			api.NewLiteralPathSegment("baz"),
		}, ""},
		{"/foo/{bar.baz}", []api.PathSegment{
			api.NewLiteralPathSegment("foo"),
			api.NewFieldPathPathSegment("bar.baz"),
		}, ""},
		{"foo", nil, "path must start with slash"},
		{"/", nil, "path cannot end with slash"},
		{"/foo/", nil, "path cannot end with slash"},
		{"/foo/***/bar", nil, "wildcard literal cannot exceed two *"},

		//verb tests
		{"/foo/:bar", nil, "verb cannot come after slash"},
		{"/foo:bar/baz", nil, "verb must be the last segment"},
		{":foo", nil, "verb cannot be the first segment"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			if tc.want != nil {
				expectEqual(t, tc.path, tc.want)
			} else {
				expectError(t, tc.path, tc.explanation)
			}

		})
	}
}

func expectEqual(t *testing.T, path string, want []api.PathSegment) {
	t.Helper()
	got, err := Parse(path)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Fatalf("failed parsing path [%s] (-want, +got):\n%s", path, diff)
	}
}
func expectError(t *testing.T, path string, explanation string) {
	t.Helper()
	_, err := Parse(path)
	if err == nil {
		t.Fatalf("Parse(%s) succeeded, want error: %s", path, explanation)
	}
}
