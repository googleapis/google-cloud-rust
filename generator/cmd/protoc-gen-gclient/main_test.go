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

package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestRun_Rust(t *testing.T) {
	tDir := t.TempDir()
	if err := run("testdata/rust/rust.bin", tDir, "../../templates"); err != nil {
		t.Fatal(err)
	}
	diff(t, "testdata/rust/golden", tDir)
}

func diff(t *testing.T, goldenDir, outputDir string) {
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	if *updateGolden {
		for _, f := range files {
			b, err := os.ReadFile(filepath.Join(outputDir, f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			outFileName := filepath.Join(goldenDir, f.Name())
			t.Logf("writing golden file %s", outFileName)
			if err := os.WriteFile(outFileName, b, os.ModePerm); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	for _, f := range files {
		want, err := os.ReadFile(filepath.Join(goldenDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(outputDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("mismatch(-want, +got): %s", diff)
		}
	}
}
