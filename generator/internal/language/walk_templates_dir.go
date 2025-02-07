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

package language

import (
	"io/fs"
	"path/filepath"
	"strings"
)

// Handles a common case for Codecs: the templates filenames can encode the
// output filenames.
//
// For some languages (e.g. Go and Rust) the name of the mustache templates can
// encode the name of the output file, for example, `src/foo.go.mustache` can
// generate `src/foo.go`.
//
// This is not true for all languages. For example, in Java it would be more
// idiomatic to generate one file per (generated) class.
//
// Even in Rust, we may want to skip some files if the crate does not have
// any services.
func WalkTemplatesDir(fsys fs.FS, root string) []GeneratedFile {
	var result []GeneratedFile
	fs.WalkDir(fsys, root, func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) != ".mustache" {
			return nil
		}
		if strings.Count(d.Name(), ".") == 1 {
			// skipping partials
			return nil
		}
		dirname := filepath.Dir(strings.TrimPrefix(path, root))
		basename := strings.TrimSuffix(d.Name(), ".mustache")
		result = append(result, GeneratedFile{
			TemplatePath: path,
			OutputPath:   filepath.Join(dirname, basename),
		})
		return nil
	})
	return result
}
