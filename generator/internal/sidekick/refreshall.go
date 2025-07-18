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

package sidekick

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func init() {
	newCommand(
		"sidekick refresh-all",
		"Reruns the generator for all client libraries.",
		`
Reruns the generator for all client libraries, using the configuration parameters saved in the .sidekick.toml file for each library.
`,
		cmdSidekick,
		refreshAll,
	).
		addAltName("refreshall").
		addAltName("refreshAll")
}

func overrideSources(rootConfig *config.Config) (*config.Config, error) {
	override := *rootConfig
	override.Codec = maps.Clone(rootConfig.Codec)
	override.Source = maps.Clone(rootConfig.Source)
	for _, root := range config.AllSourceRoots(rootConfig.Source) {
		configPrefix := strings.TrimSuffix(root, "-root")
		if _, ok := rootConfig.Source[root]; !ok {
			continue
		}
		source, err := makeSourceRoot(rootConfig, configPrefix)
		if err != nil {
			return nil, err
		}
		if subdir, ok := rootConfig.Source[configPrefix+"-subdir"]; ok {
			source = path.Join(source, subdir)
		}
		override.Source[root] = source
	}
	return &override, nil
}

func refreshAll(rootConfig *config.Config, cmdLine *CommandLine) error {
	override, err := overrideSources(rootConfig)
	if err != nil {
		return err
	}
	directories, err := findAllDirectories()
	if err != nil {
		return err
	}

	type result struct {
		dir string
		err error
	}
	results := make(chan result)
	var wg sync.WaitGroup
	fmt.Printf("refreshing %d directories\n", len(directories))
	for _, dir := range directories {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := refreshDir(override, cmdLine, dir)
			results <- result{dir: dir, err: err}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	var failures []error
	for e := range results {
		if e.err != nil {
			failures = append(failures, fmt.Errorf("error refreshing directory %s: %w", e.dir, e.err))
		}
	}
	if failures == nil {
		return nil
	}
	return errors.Join(failures...)
}

func findAllDirectories() ([]string, error) {
	var result []string
	err := fs.WalkDir(os.DirFS("."), ".", func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		dir := filepath.Dir(path)
		ignored := []string{
			"target/package/", // The output from `cargo package`
			"generator/",      // Testing
		}
		for _, candidate := range ignored {
			if strings.Contains(dir, candidate) {
				return nil
			}
		}
		if d.Name() == ".sidekick.toml" && dir != "." {
			result = append(result, dir)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
