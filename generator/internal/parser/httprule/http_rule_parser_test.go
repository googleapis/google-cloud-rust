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

func TestProtobuf_Parse(t *testing.T) {
	tests := []struct {
		path        string
		want        *PathTemplate
		explanation string
	}{
		{"/foo", expectTemplate().withLiteral("foo"), ""},
		{"/foo/bar", expectTemplate().withLiteral("foo").withLiteral("bar"), ""},
		{"/v1/*/foo", expectTemplate().withLiteral("v1").withMatch().withLiteral("foo"), ""},
		{"/v1/**/foo", expectTemplate().withLiteral("v1").withMatchRecursive().withLiteral("foo"), ""},
		{"/foo:bar", expectTemplate().withLiteral("foo").withVerb("bar"), ""},
		{"/foo/{bar}", expectTemplate().withLiteral("foo").withVariableNamed("bar"), ""},
		{"/foo/{bar.baz}", expectTemplate().withLiteral("foo").withVariableNamed("bar", "baz"), ""},
		{"/foo/{bar=baz}", expectTemplate().withLiteral("foo").withVariable(
			variable("bar").withLiteral("baz")), ""},
		{"/foo/{bar=*}", expectTemplate().withLiteral("foo").withVariable(
			variable("bar").withMatch()), ""},
		{"/foo/{bar=*}/baz", expectTemplate().withLiteral("foo").withVariable(
			variable("bar").withMatch()).
			withLiteral("baz"), ""},
		{"/foo/{bar=**}/baz:qux", expectTemplate().withLiteral("foo").withVariable(
			variable("bar").withMatchRecursive()).
			withLiteral("baz").withVerb("qux"), ""},
		{"foo", nil, "path must start with slash"},
		{"/", nil, "path cannot end with slash"},
		{"/foo/", nil, "path cannot end with slash"},
		{"/foo/***/bar", nil, "wildcard literal cannot exceed two *, and * isn't allowed in a LITERAL"},
		{"/%0f", expectTemplate().withLiteral("%0f"), ""},
		{"/%0z", nil, "bad percent encoding"},
		{"/foo//bar", nil, "segment is too short"},
		{"/foo/:", nil, "verb is too short"},
		{"/foo/{}/bar", nil, "var too short"},
		{"/foo/{a.}/bar", nil, "var identifier too short"},
		{"/foo/{.a}/bar", nil, "var identifier too short"},
		{"/foo/{a=}/bar", nil, "var value too short"},
		{"/foo/{9bar}", nil, "var identifier has bad first character"},
		{"/foo/{bar9}", expectTemplate().withLiteral("foo").withVariableNamed("bar9"), ""},
		{"/foo/{b&r}", nil, "var identifier has bad character"},
		{"/foo/:bar", nil, "verb cannot come after slash"},
		{"/foo:bar/baz", nil, "verb must be the last segment, and : isn't allowed in a LITERAL"},
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

func expectTemplate() *PathTemplate {
	return &PathTemplate{}
}

func (p *PathTemplate) withLiteral(l string) *PathTemplate {
	p.Segments = append(p.Segments, &Segment{Literal: (*Literal)(&l)})
	return p
}

func (v *Variable) withLiteral(l string) *Variable {
	v.Segments = append(v.Segments, &Segment{Literal: (*Literal)(&l)})
	return v
}

func (p *PathTemplate) withMatchRecursive() *PathTemplate {
	p.Segments = append(p.Segments, &Segment{MatchRecursive: &MatchRecursive{}})
	return p
}

func (v *Variable) withMatchRecursive() *Variable {
	v.Segments = append(v.Segments, &Segment{MatchRecursive: &MatchRecursive{}})
	return v
}

func (p *PathTemplate) withMatch() *PathTemplate {
	p.Segments = append(p.Segments, &Segment{Match: &Match{}})
	return p
}

func (v *Variable) withMatch() *Variable {
	v.Segments = append(v.Segments, &Segment{Match: &Match{}})
	return v
}

func (p *PathTemplate) withVariableNamed(idsAsStr ...string) *PathTemplate {
	p.Segments = append(p.Segments, &Segment{Variable: variable(idsAsStr...)})
	return p
}

func variable(idsAsStr ...string) *Variable {
	var ids []*Identifier
	for _, idAsStr := range idsAsStr {
		id := Identifier(idAsStr)
		ids = append(ids, &id)
	}
	return &Variable{FieldPath: ids}
}

func (p *PathTemplate) withVariable(v *Variable) *PathTemplate {
	p.Segments = append(p.Segments, &Segment{Variable: v})
	return p
}

func (p *PathTemplate) withVerb(v string) *PathTemplate {
	l := Literal(v)
	p.Verb = &l
	return p
}
