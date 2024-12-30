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
		want        *PathTemplate
		explanation string
	}{
		{"/foo", template(literal("foo")), ""},
		{"/foo/bar", template(literal("foo"), literal("bar")), ""},
		{"/v1/*/foo", template(literal("v1"), match(), literal("foo")), ""},
		{"/v1/**/foo", template(literal("v1"), matchR(), literal("foo")), ""},
		{"/foo:bar", templateV(segments(literal("foo")), verb("bar")), ""},
		{"/foo/{bar}", template(literal("foo"), varr("bar")), ""},
		{"/foo/{bar.baz}", template(literal("foo"), varr("bar", "baz")), ""},
		{"/foo/{bar=baz}", template(literal("foo"), varrs(ids("bar"), segments(literal("baz")))), ""},
		{"/foo/{bar=*}", template(literal("foo"), varrs(ids("bar"), segments(match()))), ""},
		{"/foo/{bar=*}/baz", template(literal("foo"), varrs(ids("bar"), segments(match())), literal("baz")), ""},
		{"/foo/{bar=*}/baz:qux", templateV(segments(literal("foo"), varrs(ids("bar"), segments(match())), literal("baz")), verb("qux")), ""},
		{"foo", nil, "path must start with slash"},
		{"/", nil, "path cannot end with slash"},
		{"/foo/", nil, "path cannot end with slash"},

		// the following test is failing because * is a sub-delimiter, which is allowed in the LITERAL segment
		//{"/foo/***/bar", nil, "wildcard literal cannot exceed two *"},

		{"/foo/:bar", nil, "verb cannot come after slash"},
		{"/foo:bar/baz", nil, "verb must be the last segment"},
		{":foo", nil, "verb cannot be the first segment"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			got, err := Parse(tc.path)
			if tc.want != nil {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				if got == nil {
					t.Fatalf("expected path template for %s, got nil", tc.path)
				}
				if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
					t.Fatalf("failed parsing path [%s] (-want, +got):\n%s", tc.path, diff)
				}
			} else {
				if err == nil {
					t.Fatalf("Parse(%s) succeeded, want error: %s", tc.path, tc.explanation)
				}
			}
		})
	}
}

func Test_parseSegments(t *testing.T) {
	tests := []struct {
		path        string
		want        []*Segment
		explanation string
	}{
		{"foo", []*Segment{{Literal: newLiteral("foo")}}, ""},
		{"foo/bar", []*Segment{{Literal: newLiteral("foo")}, {Literal: newLiteral("bar")}}, ""},
		{"v1/*/foo", []*Segment{{Literal: newLiteral("v1")}, {Match: &Match{}}, {Literal: newLiteral("foo")}}, ""},
		{"v1/**/foo", []*Segment{{Literal: newLiteral("v1")}, {MatchRecursive: &MatchRecursive{}}, {Literal: newLiteral("foo")}}, ""},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			if tc.want != nil {
				got, err := parseSegments(tc.path)
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
					t.Fatalf("failed parsing path [%s] (-want, +got):\n%s", tc.path, diff)
				}
			} else {
				_, err := parseSegments(tc.path)
				if err == nil {
					t.Fatalf("Parse(%s) succeeded, want error: %s", tc.path, tc.explanation)
				}
			}

		})
	}

}

func templateV(s []*Segment, v *Literal) *PathTemplate {
	return &PathTemplate{Segments: s, Verb: v}
}

func template(s ...*Segment) *PathTemplate {
	return &PathTemplate{Segments: s}
}
func segments(s ...*Segment) []*Segment {
	return s
}

func matchR() *Segment {
	return &Segment{MatchRecursive: &MatchRecursive{}}
}

func match() *Segment {
	return &Segment{Match: &Match{}}
}

func verb(s string) *Literal {
	return newLiteral(s)
}

func literal(s string) *Segment {
	return &Segment{Literal: newLiteral(s)}
}

func varr(i ...string) *Segment {
	return &Segment{Variable: &Variable{FieldPath: ids(i...)}}
}

func varrs(i []*Identifier, s []*Segment) *Segment {
	return &Segment{Variable: &Variable{FieldPath: i, Segments: s}}
}

func ids(i ...string) []*Identifier {
	var ids []*Identifier
	for _, id := range i {
		ids = append(ids, newIdentifier(id))
	}
	return ids
}

func newLiteral(s string) *Literal {
	literal := Literal(s)
	return &literal
}

func newIdentifier(s string) *Identifier {
	identifier := Identifier(s)
	return &identifier
}
