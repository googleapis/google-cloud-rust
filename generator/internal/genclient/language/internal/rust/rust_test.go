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

func TestFieldType(t *testing.T) {
	target := &genclient.Message{
		Name: "Target",
		ID:   "..Target",
	}
	message := &genclient.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*genclient.Field{
			{
				Name:     "f_int32",
				Typez:    genclient.INT32_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_int32_optional",
				Typez:    genclient.INT32_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_int32_repeated",
				Typez:    genclient.INT32_TYPE,
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_msg",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_msg_repeated",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_timestamp",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_timestamp_repeated",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: false,
				Repeated: true,
			},
		},
	}
	api := genclient.NewTestAPI([]*genclient.Message{target, message}, []*genclient.Enum{}, []*genclient.Service{})

	expectedTypes := map[string]string{
		"f_int32":              "i32",
		"f_int32_optional":     "Option<i32>",
		"f_int32_repeated":     "Vec<i32>",
		"f_msg":                "Option<crate::model::Target>",
		"f_msg_repeated":       "Vec<crate::model::Target>",
		"f_timestamp":          "Option<gax_placeholder::Timestamp>",
		"f_timestamp_repeated": "Vec<gax_placeholder::Timestamp>",
	}
	c := &Codec{}
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := c.FieldType(field, api.State)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
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
	if got := c.FQMessageName(message, api.State); got != "crate::model::Replication" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::Replication", got)
	}

	if got := c.MessageName(nested, api.State); got != "Automatic" {
		t.Errorf("mismatched message name, got=%s, want=Automatic", got)
	}
	if got := c.FQMessageName(nested, api.State); got != "crate::model::replication::Automatic" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::replication::Automatic", got)
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
	if got := c.FQEnumName(nested, api.State); got != "crate::model::secret_version::State" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::secret_version::State", got)
	}
}
