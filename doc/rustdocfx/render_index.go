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
	"embed"
	"encoding/json"
	"log/slog"
	"os"
	fspath "path"
	"path/filepath"

	"github.com/cbroglie/mustache"
)

//go:embed all:templates
var templates embed.FS

// mustacheProvider implements the interface required by `mustache.RenderPartials` using
// the embedded templates as source.
type mustacheProvider struct{}

// index provides a context for the `_libraries.json` mustache template. It
// contains all the client libraries we want indexed at cloud.google.com
type index struct {
	Entries []*indexEntry
}

// indexEntry provides a context for a single entry in the `_libraries.json`
// mustache template
type indexEntry struct {
	PkgName      string
	Version      string
	APIShortName string
	Product      string
	Last         bool
}

// repoMetadata simplifies parsing of `.repo-metadata.json` files.
type repoMetadata struct {
	ApiId        string `json:"api_id"`
	ApiShortName string `json:"api_shortname"`
	NamePretty   string `json:"name_pretty"`
}

// renderIndex generates an index file in a format suitable for cloud.google.com
func renderIndex(crates []crate, outDir string) error {
	contents, err := templatesProvider("index.json.mustache")
	if err != nil {
		return err
	}
	context := &index{}
	for _, c := range crates {
		metadata := repoMetadata{}
		contents, err := os.ReadFile(fspath.Join(c.Location, ".repo-metadata.json"))
		if err != nil {
			slog.Warn("cannot read repo metadata", "location", c.Location, "package", c.Name)
			continue
		}
		if err := json.Unmarshal(contents, &metadata); err != nil {
			continue
		}
		context.Entries = append(context.Entries, &indexEntry{
			PkgName:      c.Name,
			Version:      c.Version,
			APIShortName: metadata.ApiShortName,
			Product:      metadata.NamePretty,
		})
	}
	if len(context.Entries) != 0 {
		context.Entries[len(context.Entries)-1].Last = true
	}
	output, err := mustache.RenderPartials(contents, &mustacheProvider{}, context)
	if err != nil {
		return err
	}
	os.MkdirAll(outDir, 0755) // ignore errors
	if err := os.WriteFile(filepath.Join(outDir, "_libraries.json"), []byte(output), 0644); err != nil {
		return err
	}
	return nil
}

// Get gets the contents of an embedded template called `name`.
func (p *mustacheProvider) Get(name string) (string, error) {
	return templatesProvider(name + ".mustache")
}

// templatesProvider reads the contents of `name` from the embedded templates.
func templatesProvider(name string) (string, error) {
	contents, err := templates.ReadFile(fspath.Join("templates", filepath.ToSlash(name)))
	if err != nil {
		return "", err
	}
	return string(contents), nil
}
