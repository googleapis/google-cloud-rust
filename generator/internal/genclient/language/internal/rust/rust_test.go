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
