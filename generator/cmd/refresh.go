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
	"path"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser"
)

func Refresh(specFormat string, popts *genclient.ParserOptions, copts *genclient.CodecOptions) error {
	parser, err := parser.NewParser(specFormat)
	if err != nil {
		return err
	}

	api, err := parser.Parse(*popts)
	if err != nil {
		return err
	}

	codec, err := language.NewCodec(copts)
	if err != nil {
		return err
	}
	if err = codec.Validate(api); err != nil {
		return err
	}
	request := &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      path.Join(copts.ProjectRoot, copts.OutDir),
		TemplateDir: copts.TemplateDir,
	}
	return genclient.Generate(request)
}
