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
	"fmt"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func GenerateClient(model *api.API, language, outdir string, options map[string]string) error {
	var (
		data           any
		provider       templateProvider
		generatedFiles []GeneratedFile
	)
	switch language {
	case "rust":
		codec, err := newRustCodec(options)
		if err != nil {
			return err
		}
		data, err = newRustTemplateData(model, codec, outdir)
		if err != nil {
			return err
		}
		provider = rustTemplatesProvider()
		hasServices := len(model.State.ServiceByID) > 0
		generatedFiles = rustGeneratedFiles(codec.generateModule, hasServices)
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

	return GenerateFromRoot(outdir, data, provider, generatedFiles)
}
