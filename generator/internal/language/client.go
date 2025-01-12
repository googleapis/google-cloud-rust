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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cbroglie/mustache"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

// GenerateClientRequest used to generate clients.
type GenerateClientRequest struct {
	// The in memory representation of a parsed input.
	API *api.API
	// An adapter to transform values into language idiomatic representations.
	Codec Codec
	// OutDir is the path to the output directory.
	OutDir string
}

func (r *GenerateClientRequest) outDir() string {
	if r.OutDir == "" {
		wd, _ := os.Getwd()
		return wd
	}
	return r.OutDir
}

type mustacheProvider struct {
	impl    func(string) (string, error)
	dirname string
}

func (p *mustacheProvider) Get(name string) (string, error) {
	return p.impl(filepath.Join(p.dirname, name) + ".mustache")
}

func GenerateClient(req *GenerateClientRequest) error {
	data := newTemplateData(req.API, req.Codec)
	var context []any
	context = append(context, data)
	if languageContext := req.Codec.AdditionalContext(req.API); languageContext != nil {
		context = append(context, languageContext)
	}

	provider := req.Codec.TemplatesProvider()
	var errs []error
	for _, gen := range req.Codec.GeneratedFiles() {
		templateContents, err := provider(gen.TemplatePath)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		destination := filepath.Join(req.outDir(), gen.OutputPath)
		os.MkdirAll(filepath.Dir(destination), 0777) // Ignore errors
		nestedProvider := mustacheProvider{
			impl:    provider,
			dirname: filepath.Dir(gen.TemplatePath),
		}
		s, err := mustache.RenderPartials(templateContents, &nestedProvider, context...)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := os.WriteFile(destination, []byte(s), os.ModePerm); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors generating output files: %w", errors.Join(errs...))
	}
	return nil
}
