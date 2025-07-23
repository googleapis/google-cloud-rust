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

package rust

import (
	"embed"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
)

//go:embed all:templates
var templates embed.FS

func Generate(model *api.API, outdir string, cfg *config.Config) error {
	codec, err := newCodec(cfg.General.SpecificationFormat == "protobuf", cfg.Codec)
	if err != nil {
		return err
	}
	annotations := annotateModel(model, codec)
	provider := templatesProvider()
	generatedFiles := codec.generatedFiles(annotations.HasServices())
	return language.GenerateFromModel(outdir, model, provider, generatedFiles)
}

func GenerateStorage(outdir string, storageModel *api.API, storageConfig *config.Config, controlModel *api.API, controlConfig *config.Config) error {
	storageCodec, err := newCodec(storageConfig.General.SpecificationFormat == "protobuf", storageConfig.Codec)
	if err != nil {
		return err
	}
	annotateModel(storageModel, storageCodec)
	controlCodec, err := newCodec(controlConfig.General.SpecificationFormat == "protobuf", controlConfig.Codec)
	if err != nil {
		return err
	}
	annotateModel(controlModel, controlCodec)

	model := &api.API{
		Codec: &storageAnnotations{
			Storage: storageModel,
			Control: controlModel,
		},
	}
	provider := templatesProvider()
	generatedFiles := language.WalkTemplatesDir(templates, "templates/storage")
	return language.GenerateFromModel(outdir, model, provider, generatedFiles)
}

type storageAnnotations struct {
	Storage *api.API
	Control *api.API
}

func templatesProvider() language.TemplateProvider {
	return func(name string) (string, error) {
		contents, err := templates.ReadFile(name)
		if err != nil {
			return "", err
		}
		return string(contents), nil
	}
}

func (c *codec) generatedFiles(hasServices bool) []language.GeneratedFile {
	if c.templateOverride != "" {
		return language.WalkTemplatesDir(templates, c.templateOverride)
	}
	var root string
	switch {
	case !hasServices:
		root = "templates/nosvc"
	default:
		root = "templates/crate"
	}
	return language.WalkTemplatesDir(templates, root)
}
