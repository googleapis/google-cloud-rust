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

package protobuf

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func DetermineInputFiles(source string, options map[string]string) ([]string, error) {
	if _, ok := options["include-list"]; ok {
		if _, ok := options["exclude-list"]; ok {
			return nil, fmt.Errorf("cannot use both `exclude-list` and `include-list` in the source options")
		}
	}

	// `config.Source` is relative to the `googleapis-root`,
	// or `extra-protos-root`, when that is set. It should always be a directory
	// and by default all the the files in that directory are used.
	for _, opt := range config.SourceRoots(options) {
		location, ok := options[opt]
		if !ok {
			// Ignore options that are not set
			continue
		}
		stat, err := os.Stat(path.Join(location, source))
		if err == nil && stat.IsDir() {
			// Found a matching directory, use it.
			source = path.Join(location, source)
			break
		}
	}
	files := map[string]bool{}
	if err := findFiles(files, source); err != nil {
		return nil, err
	}
	applyIncludeList(files, source, options)
	applyExcludeList(files, source, options)
	var list []string
	for name, ok := range files {
		if ok {
			list = append(list, name)
		}
	}
	sort.Strings(list)
	return list, nil
}

func findFiles(files map[string]bool, source string) error {
	const maxDepth = 1
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		depth := strings.Count(filepath.ToSlash(strings.TrimPrefix(path, source)), "/")
		if info.IsDir() && depth >= maxDepth {
			return filepath.SkipDir
		}
		if depth > maxDepth {
			return nil
		}
		if filepath.Ext(path) == ".proto" {
			files[path] = true
		}
		return nil
	})
}

func applyIncludeList(files map[string]bool, sourceDirectory string, options map[string]string) {
	list, ok := options["include-list"]
	if !ok {
		return
	}
	// Ignore any discovered paths, only the paths from the include list apply.
	clear(files)
	for _, p := range strings.Split(list, ",") {
		files[path.Join(sourceDirectory, p)] = true
	}
}

func applyExcludeList(files map[string]bool, sourceDirectory string, options map[string]string) {
	list, ok := options["exclude-list"]
	if !ok {
		return
	}
	for _, p := range strings.Split(list, ",") {
		delete(files, path.Join(sourceDirectory, p))
	}
}
