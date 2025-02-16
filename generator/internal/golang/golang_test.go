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

package golang

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

type goCaseConvertTest struct {
	Input    string
	Expected string
}

func TestGeneratedFiles(t *testing.T) {
	files := generatedFiles()
	if len(files) == 0 {
		t.Errorf("expected a non-empty list of template files from generatedFiles()")
	}
}

func TestToPascal(t *testing.T) {
	var pascalConvertTests = []goCaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"return", "Return"},
	}
	for _, test := range pascalConvertTests {
		if output := toPascal(test.Input); output != test.Expected {
			t.Errorf("Output %s not equal to expected %s, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestMessageNames(t *testing.T) {
	replication := sample.Replication()
	automatic := sample.Automatic()
	for _, test := range []struct {
		message *api.Message
		want    string
	}{
		{replication, "Replication"},
		{automatic, "Replication_Automatic"},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := messageName(test.message, nil); got != test.want {
				t.Errorf("goMessageName = %q, want = %q", got, test.want)
			}
		})
	}
}

func TestEnumNames(t *testing.T) {
	message := &api.Message{
		Name: "SecretVersion",
		ID:   "..SecretVersion",
		Fields: []*api.Field{
			{
				Name:     "automatic",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &api.Enum{
		Name: "State",
		ID:   "..SecretVersion.State",
	}

	_ = api.NewTestAPI([]*api.Message{message}, []*api.Enum{nested}, []*api.Service{})
	if got := enumName(nested, nil); got != "SecretVersion_State" {
		t.Errorf("mismatched message name, want=SecretVersion_Automatic, got=%s", got)
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
		"```",
		"Maybe they wanted to show some JSON:",
		"{",
		`  "foo": "bar"`,
		"}",
		"```",
	}
	state := &api.APIState{}
	got := formatDocComments(input, state)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}
