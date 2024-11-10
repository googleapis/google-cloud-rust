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

// Package openapi reads OpenAPI v3 specifications and converts them into
// the `genclient` model.
package openapi

import (
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	parser "github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser/openapi"
)

type Options struct {
	Language      string
	OutDir        string
	TemplateDir   string
	ServiceConfig string
	// Only used for local testing
}

func Translate(inputPath string, opts *Options) (*genclient.GenerateRequest, error) {
	popts := genclient.ParserOptions{
		Source:        inputPath,
		ServiceConfig: opts.ServiceConfig,
	}
	parser := parser.NewParser()
	api, err := parser.Parse(popts)
	if err != nil {
		return nil, err
	}
	codec, err := language.NewCodec(opts.Language)
	if err != nil {
		return nil, err
	}
	return &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      opts.OutDir,
		TemplateDir: opts.TemplateDir,
	}, nil
}
