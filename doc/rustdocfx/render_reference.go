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
	"fmt"
	"os"
	"path/filepath"

	"github.com/cbroglie/mustache"
)

// renderReference renders a rustdoc element as a docfx universal reference.
func renderReference(crate *crate, id, outDir string) (string, error) {
	reference, err := newDocfxManagedReference(crate, id)
	if err != nil {
		return "", err
	}
	contents, err := templatesProvider("universalReference.yml.mustache")
	if err != nil {
		return "", err
	}
	output, err := mustache.RenderPartials(contents, &mustacheProvider{}, reference)
	if err != nil {
		return "", err
	}
	uid, err := crate.getDocfxUid(id)
	if err != nil {
		return "", err
	}
	outputFile := filepath.Join(outDir, fmt.Sprintf("%s.yml", uid))
	if err := os.WriteFile(outputFile, []byte(output), 0644); err != nil {
		return "", err
	}
	return uid, nil
}
