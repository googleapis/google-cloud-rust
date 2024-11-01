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
