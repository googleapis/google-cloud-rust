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
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

type goCaseConvertTest struct {
	Input    string
	Expected string
}

func TestGo_ToPascal(t *testing.T) {
	var pascalConvertTests = []goCaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"return", "Return"},
	}
	for _, test := range pascalConvertTests {
		if output := goToPascal(test.Input); output != test.Expected {
			t.Errorf("Output %s not equal to expected %s, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestGo_MessageNames(t *testing.T) {
	replication := &api.Message{
		Name: "Replication",
		ID:   "..Replication",
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
	automatic := &api.Message{
		Parent: replication,
		Name:   "Automatic",
		ID:     "..Replication.Automatic",
	}

	for _, test := range []struct {
		message *api.Message
		want    string
	}{
		{replication, "Replication"},
		{automatic, "Replication_Automatic"},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := goMessageName(test.message, nil); got != test.want {
				t.Errorf("goMessageName = %q, want = %q", got, test.want)
			}
		})
	}
}

func TestGo_EnumNames(t *testing.T) {
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

	_ = newTestAPI([]*api.Message{message}, []*api.Enum{nested}, []*api.Service{})
	if got := goEnumName(nested, nil); got != "SecretVersion_State" {
		t.Errorf("mismatched message name, want=SecretVersion_Automatic, got=%s", got)
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
	state := &api.APIState{}
	got := goFormatDocComments(input, state)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestGo_Validate(t *testing.T) {
	api := newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	if err := goValidate(api, "p1"); err != nil {
		t.Errorf("unexpected error in API validation %q", err)
	}
}

func TestGo_ValidateMessageMismatch(t *testing.T) {
	const sourceSpecificationPackageName = "p1"
	test := newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	if err := goValidate(test, "p1"); err == nil {
		t.Errorf("expected an error in API validation got=%s", sourceSpecificationPackageName)
	}

	test = newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	if err := goValidate(test, "p1"); err == nil {
		t.Errorf("expected an error in API validation got=%s", sourceSpecificationPackageName)
	}

	test = newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	if err := goValidate(test, "p1"); err == nil {
		t.Errorf("expected an error in API validation got=%s", sourceSpecificationPackageName)
	}
}
