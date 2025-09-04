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
	"path/filepath"
	"sort"
	"strings"

	"github.com/cbroglie/mustache"
)

func renderTOC(toc docfxTableOfContent, outDir string) error {
	// Sort the toc before rendering.
	sort.SliceStable(toc.Items, func(i, j int) bool {
		// Always put the crate as the first item.
		if strings.HasPrefix(toc.Items[i].Uid, "crate.") {
			return true
		} else if strings.HasPrefix(toc.Items[j].Uid, "crate.") {
			return false
		}
		return toc.Items[i].Name < toc.Items[j].Name
	})
	contents, err := templatesProvider("toc.yml.mustache")
	if err != nil {
		return err
	}
	output, err := mustache.RenderPartials(contents, &mustacheProvider{}, toc)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, "toc.yml"), []byte(output), 0644)
}
