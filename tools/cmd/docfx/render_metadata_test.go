// Copyright 2025 Google LLC
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

package main

import (
	"os"
	ospath "path"
	"strings"
	"testing"
)

func TestRenderMetadata(t *testing.T) {
	input := &crate{
		Name:    "test-only",
		Version: "0.0.0-test",
		Root:    1234567890,
		Index: map[string]item{
			"1234567890": {
				Name: "root-name",
			},
		},
	}
	outDir := t.TempDir()
	if err := renderMetadata(input, outDir); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(ospath.Join(outDir, "docs.metadata"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	if idx := strings.Index(text, `name: "root-name"`); idx == -1 {
		t.Errorf("expected line with `name: ...` in: %q", text)
	}
}
