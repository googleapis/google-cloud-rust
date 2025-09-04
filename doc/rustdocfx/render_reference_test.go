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
	"encoding/json"
	"fmt"
	"os"
	fspath "path"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRenderReference(t *testing.T) {
	input, err := testDataPublicCA()
	if err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	id := findCrateId(t, input)
	uid, err := renderReference(input, id, outDir)
	if err != nil {
		t.Fatal(err)
	}
	if uid != "crate.google_cloud_security_publicca_v1" {
		t.Errorf("mismatched uid, got=%s, want=%s", uid, "")
	}
	contents, err := os.ReadFile(fspath.Join(outDir, fmt.Sprintf("%s.yml", uid)))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(contents), "\n")
	idx := slices.IndexFunc(lines, func(a string) bool { return strings.Contains(a, "summary: |") })
	if idx == -1 {
		t.Fatalf("missing `summary: |` line in output YAML %s", contents)
	}
	got := lines[idx+1 : idx+4]
	want := []string{
		"    Google Cloud Client Libraries for Rust - Public Certificate Authority API",
		"    ",
		"    **FEEDBACK WANTED:** We believe the APIs in this crate are stable, and",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched summary lines in generated YAML (-want +got):\n%s", diff)
	}
}

func findCrateId(t *testing.T, crate *crate) string {
	t.Helper()
	for id := range crate.Index {
		if crate.getKind(id) == crateKind {
			return id
		}
	}
	t.Fatalf("cannot find crate id")
	return ""
}

func testDataPublicCA() (*crate, error) {
	contents, err := os.ReadFile("testdata/google_cloud_security_publicca_v1.json")
	if err != nil {
		return nil, err
	}
	crate := new(crate)
	// Our parser cannot handle certain attributes
	json.Unmarshal(contents, &crate) // ignore errors
	return crate, nil
}
