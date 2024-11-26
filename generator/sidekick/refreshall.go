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
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func RefreshAll(rootConfig *Config, args []string) error {
	fs := flag.NewFlagSet("refreshall", flag.ExitOnError)
	var (
		dryrun = fs.Bool("dry-run", false, "do a dry-run: find and report directories, but do not perform any changes.")
	)
	fs.Parse(args)

	root, err := makeGoogleapisRoot(rootConfig)
	if err != nil {
		return err
	}
	directories, err := findAllDirectories(rootConfig)
	if err != nil {
		return err
	}

	override := *rootConfig
	override.Codec = maps.Clone(rootConfig.Codec)
	override.Source = maps.Clone(rootConfig.Source)
	override.Source["googleapis-root"] = root

	type result struct {
		dir string
		err error
	}
	results := make(chan result)
	var wg sync.WaitGroup
	for _, dir := range directories {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if *dryrun {
				fmt.Printf("refreshing directory %s\n", dir)
				err = Refresh(&override, []string{"-dry-run", dir})
			} else {
				err = Refresh(&override, []string{dir})
			}
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

func findAllDirectories(_ *Config) ([]string, error) {
	var result []string
	err := fs.WalkDir(os.DirFS("."), ".", func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		dir := filepath.Dir(path)
		if strings.Contains(dir, "/testdata/go/") {
			// Skip these directories. They are intended for testing only.
			return nil
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
