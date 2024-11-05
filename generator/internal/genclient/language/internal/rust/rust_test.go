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

package rust

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

type ScalarFieldTest struct {
	Typez    genclient.Typez
	Optional bool
	Expected string
}

var scalarFieldTests = []ScalarFieldTest{
	{genclient.INT32_TYPE, false, "i32"},
	{genclient.INT64_TYPE, false, "i64"},
	{genclient.UINT32_TYPE, true, "Option<u32>"},
	{genclient.UINT64_TYPE, true, "Option<u64>"},
	{genclient.BOOL_TYPE, true, "Option<bool>"},
	{genclient.STRING_TYPE, true, "Option<String>"},
	{genclient.BYTES_TYPE, true, "Option<bytes::Bytes>"},
}

func TestScalarFields(t *testing.T) {
	for _, test := range scalarFieldTests {
		field := genclient.Field{Typez: test.Typez, Optional: test.Optional}
		if output := ScalarFieldType(&field); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q", output, test.Expected)
		}
	}
}

type CaseConvertTest struct {
	Input    string
	Expected string
}

func TestToSnake(t *testing.T) {
	c := &Codec{}
	var snakeConvertTests = []CaseConvertTest{
		{"FooBar", "foo_bar"},
		{"foo_bar", "foo_bar"},
		{"data_crc32c", "data_crc32c"},
		{"True", "r#true"},
		{"Static", "r#static"},
		{"Trait", "r#trait"},
		{"Self", "r#self"},
		{"self", "r#self"},
		{"yield", "r#yield"},
	}
	for _, test := range snakeConvertTests {
		if output := c.ToSnake(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestToPascal(t *testing.T) {
	c := &Codec{}
	var pascalConvertTests = []CaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"Self", "r#Self"},
		{"self", "r#Self"},
		{"yield", "Yield"},
	}
	for _, test := range pascalConvertTests {
		if output := c.ToPascal(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q", output, test.Expected)
		}
	}
}

func TestFormatDocComments(t *testing.T) {
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
		"```norust",
		"Maybe they wanted to show some JSON:",
		"{",
		`  "foo": "bar"`,
		"}",
		"```",
	}
	c := &Codec{}
	got := c.FormatDocComments(input)
	if diff := cmp.Diff(want, got); len(diff) > 0 {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestMessageNames(t *testing.T) {
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

	api := genclient.NewTestAPI([]*genclient.Message{message, nested}, []*genclient.Enum{}, []*genclient.Service{})

	c := &Codec{}
	if got := c.MessageName(message, api.State); got != "Replication" {
		t.Errorf("mismatched message name, got=%s, want=Replication", got)
	}
	if got := c.FQMessageName(message, api.State); got != "crate::Replication" {
		t.Errorf("mismatched message name, got=%s, want=crate::Replication", got)
	}

	if got := c.MessageName(nested, api.State); got != "Automatic" {
		t.Errorf("mismatched message name, got=%s, want=Automatic", got)
	}
	if got := c.FQMessageName(nested, api.State); got != "crate::replication::Automatic" {
		t.Errorf("mismatched message name, got=%s, want=crate::replication::Automatic", got)
	}
}

func TestEnumNames(t *testing.T) {
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

	api := genclient.NewTestAPI([]*genclient.Message{message}, []*genclient.Enum{nested}, []*genclient.Service{})

	c := &Codec{}
	if got := c.EnumName(nested, api.State); got != "State" {
		t.Errorf("mismatched message name, got=%s, want=Automatic", got)
	}
	if got := c.FQEnumName(nested, api.State); got != "crate::secret_version::State" {
		t.Errorf("mismatched message name, got=%s, want=crate::secret_version::State", got)
	}
}
