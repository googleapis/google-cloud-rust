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
	"os"
	fspath "path"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type librariesEntry struct {
	PkgName      string
	Language     string
	DocsUrl      string
	APIShortName string
	Product      string
}

type librariesIndex = map[string][]librariesEntry

func TestRenderIndex(t *testing.T) {
	input := []crate{
		{
			Name:     "google-cloud-iam-v1",
			Version:  "1.0.2",
			Location: "../../src/generated/iam/v1",
		},
		{
			Name:     "google-cloud-storage",
			Version:  "1.2.3",
			Location: "../../src/storage",
		},
	}

	outDir := t.TempDir()
	if err := renderIndex(input, outDir); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(fspath.Join(outDir, "_libraries.json"))
	if err != nil {
		t.Fatal(err)
	}
	var index librariesIndex
	if err := json.Unmarshal(contents, &index); err != nil {
		t.Fatal(err)
	}
	got, ok := index["google-cloud-storage"]
	if !ok {
		t.Fatalf("missing google-cloud-storage in generated index")
	}
	want := []librariesEntry{
		{
			PkgName:      "google-cloud-storage",
			Language:     "Rust",
			DocsUrl:      "https://docs.rs/google-cloud-storage/latest",
			APIShortName: "storage",
			Product:      "Cloud Storage API",
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("generated index entry mismatch (-want +got):\n%s", diff)
	}
}
