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

package gcloud

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gopkg.in/yaml.v3"
)

func TestCommandYAML(t *testing.T) {
	const root = "testdata/parallelstore/surface"
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}
			if strings.Contains(string(data), "_PARTIALS_") {
				return
			}

			var commands []*Command
			if err := yaml.Unmarshal(data, &commands); err != nil {
				t.Fatalf("failed to unmarshal YAML: %v", err)
			}
			var got bytes.Buffer
			enc := yaml.NewEncoder(&got)
			enc.SetIndent(2)
			if err := enc.Encode(commands); err != nil {
				t.Fatalf("failed to marshal struct to YAML: %v", err)
			}

			lines := strings.Split(string(data), "\n")

			// Skip all leading comments and blank lines
			var index int
			for i, line := range lines {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "#") || trimmed == "" {
					continue
				}
				index = i
				break
			}

			want := strings.Join(lines[index:], "\n")

			if diff := cmp.Diff(want, got.String()); diff != "" {
				t.Errorf("mismatch (-want, +got):\n%s", diff)
			}
		})
	}
}
