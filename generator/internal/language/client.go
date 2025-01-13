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

type mustacheProvider struct {
	impl    func(string) (string, error)
	dirname string
}

func (p *mustacheProvider) Get(name string) (string, error) {
	return p.impl(filepath.Join(p.dirname, name) + ".mustache")
}

func GenerateClient(model *api.API, language, outdir string, options map[string]string) error {
	var (
		data           any
		provider       templateProvider
		generatedFiles []GeneratedFile
	)
	switch language {
	case "rust":
		codec, err := newRustCodec(outdir, options)
		if err != nil {
			return err
		}
		if err := codec.validate(model); err != nil {
			return err
		}
		data = newRustTemplateData(model, codec)
		provider = rustTemplatesProvider()
		generatedFiles = codec.generatedFiles()
	case "go":
		var err error
		data, err = newGoTemplateData(model, options)
		if err != nil {
			return err
		}
		provider = goTemplatesProvider()
		generatedFiles = walkTemplatesDir(goTemplates, "templates/go")
	default:
		return fmt.Errorf("unknown language: %s", language)
	}

	var errs []error
	for _, gen := range generatedFiles {
		templateContents, err := provider(gen.TemplatePath)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if outdir == "" {
			wd, _ := os.Getwd()
			outdir = wd
		}
		destination := filepath.Join(outdir, gen.OutputPath)
		os.MkdirAll(filepath.Dir(destination), 0777) // Ignore errors
		nestedProvider := mustacheProvider{
			impl:    provider,
			dirname: filepath.Dir(gen.TemplatePath),
		}
		s, err := mustache.RenderPartials(templateContents, &nestedProvider, data)
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
