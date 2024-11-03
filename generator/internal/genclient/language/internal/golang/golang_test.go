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

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

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
		{"Map", "map_"},
		{"switch", "switch_"},
	}
	for _, test := range snakeConvertTests {
		if output := c.ToSnake(test.Input); output != test.Expected {
			t.Errorf("Output %s not equal to expected %s, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestToPascal(t *testing.T) {
	c := &Codec{}
	var pascalConvertTests = []CaseConvertTest{
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
	if got := c.EnumName(nested, api.State); got != "SecretVersion_State" {
		t.Errorf("mismatched message name, want=SecretVersion_Automatic, got=%s", got)
	}
	if got := c.FQEnumName(nested, api.State); got != "SecretVersion_State" {
		t.Errorf("mismatched message name, want=SecretVersion_State, got=%s", got)
	}
}
