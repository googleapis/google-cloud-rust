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

package language

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

type goCaseConvertTest struct {
	Input    string
	Expected string
}

func TestGo_ToSnake(t *testing.T) {
	c := &GoCodec{}
	var snakeConvertTests = []goCaseConvertTest{
		{"FooBar", "foo_bar"},
		{"foo_bar", "foo_bar"},
		{"data_crc32c", "data_crc32c"},
		{"Map", "map_"},
		{"switch", "switch_"},
	}
	for _, test := range snakeConvertTests {
		if output := c.ToSnake(test.Input); output != test.Expected {
			t.Errorf("Output %s not equal to expected %s, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestGo_ToPascal(t *testing.T) {
	c := &GoCodec{}
	var pascalConvertTests = []goCaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"return", "Return"},
	}
	for _, test := range pascalConvertTests {
		if output := c.ToPascal(test.Input); output != test.Expected {
			t.Errorf("Output %s not equal to expected %s, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestGo_MessageNames(t *testing.T) {
	message := &genclient.Message{
		Name: "Replication",
		ID:   "..Replication",
		Fields: []*genclient.Field{
			{
				Name:     "automatic",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &genclient.Message{
		Name: "Automatic",
		ID:   "..Replication.Automatic",
	}

	api := newTestAPI([]*genclient.Message{message, nested}, []*genclient.Enum{}, []*genclient.Service{})

	c := &GoCodec{}
	if got := c.MessageName(message, api.State); got != "Replication" {
		t.Errorf("mismatched message name, want=Replication, got=%s", got)
	}
	if got := c.FQMessageName(message, api.State); got != "Replication" {
		t.Errorf("mismatched message name, want=Replication, got=%s", got)
	}

	if got := c.MessageName(nested, api.State); got != "Replication_Automatic" {
		t.Errorf("mismatched message name, want=SecretVersion_Automatic, got=%s", got)
	}
	if got := c.FQMessageName(nested, api.State); got != "Replication_Automatic" {
		t.Errorf("mismatched message name, want=Replication_Automatic, got=%s", got)
	}
}

func TestGo_EnumNames(t *testing.T) {
	message := &genclient.Message{
		Name: "SecretVersion",
		ID:   "..SecretVersion",
		Fields: []*genclient.Field{
			{
				Name:     "automatic",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &genclient.Enum{
		Name: "State",
		ID:   "..SecretVersion.State",
	}

	api := newTestAPI([]*genclient.Message{message}, []*genclient.Enum{nested}, []*genclient.Service{})

	c := &GoCodec{}
	if got := c.EnumName(nested, api.State); got != "SecretVersion_State" {
		t.Errorf("mismatched message name, want=SecretVersion_Automatic, got=%s", got)
	}
	if got := c.FQEnumName(nested, api.State); got != "SecretVersion_State" {
		t.Errorf("mismatched message name, want=SecretVersion_State, got=%s", got)
	}
}

func TestGo_FormatDocComments(t *testing.T) {
	input := `Some comments describing the thing.

The next line has some extra trailing whitespace:

We want to respect whitespace at the beginning, because it important in Markdown:
- A thing
  - A nested thing
- The next thing

Now for some fun with block quotes

` + "```" + `
Maybe they wanted to show some JSON:
{
  "foo": "bar"
}
` + "```"

	want := []string{
		"Some comments describing the thing.",
		"",
		"The next line has some extra trailing whitespace:",
		"",
		"We want to respect whitespace at the beginning, because it important in Markdown:",
		"- A thing",
		"  - A nested thing",
		"- The next thing",
		"",
		"Now for some fun with block quotes",
		"",
		"```",
		"Maybe they wanted to show some JSON:",
		"{",
		`  "foo": "bar"`,
		"}",
		"```",
	}
	c := &GoCodec{}
	got := c.FormatDocComments(input)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestGo_Validate(t *testing.T) {
	api := newTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}})
	c := &GoCodec{}
	if err := c.Validate(api); err != nil {
		t.Errorf("unexpected error in API validation %q", err)
	}
	if c.SourceSpecificationPackageName != "p1" {
		t.Errorf("mismatched source package name, want=p1, got=%s", c.SourceSpecificationPackageName)
	}
}

func TestGo_ValidateMessageMismatch(t *testing.T) {
	api := newTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}})
	c := &GoCodec{}
	if err := c.Validate(api); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}

	api = newTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}})
	c = &GoCodec{}
	if err := c.Validate(api); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}

	api = newTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	c = &GoCodec{}
	if err := c.Validate(api); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}
}
