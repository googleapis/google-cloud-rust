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
	"fmt"
	"path"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/codec_sample"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	"github.com/googleapis/google-cloud-rust/generator/internal/dart"
	"github.com/googleapis/google-cloud-rust/generator/internal/gcloud"
	"github.com/googleapis/google-cloud-rust/generator/internal/golang"
	"github.com/googleapis/google-cloud-rust/generator/internal/parser"
	"github.com/googleapis/google-cloud-rust/generator/internal/rust"
	"github.com/googleapis/google-cloud-rust/generator/internal/rust_prost"
)

func init() {
	newCommand(
		"sidekick refresh",
		"Reruns the generator for a single client library.",
		`
Reruns the generator for a single client library, using the configuration parameters saved in the .sidekick.toml file.
`,
		cmdSidekick,
		refresh,
	)
}

// refresh reruns the generator in one directory, using the configuration
// parameters saved in its `.sidekick.toml` file.
func refresh(rootConfig *config.Config, cmdLine *CommandLine) error {
	override, err := overrideSources(rootConfig)
	if err != nil {
		return err
	}
	return refreshDir(override, cmdLine, cmdLine.Output)
}

func loadDir(rootConfig *config.Config, output string) (*api.API, *config.Config, error) {
	config, err := config.MergeConfigAndFile(rootConfig, path.Join(output, ".sidekick.toml"))
	if err != nil {
		return nil, nil, err
	}
	if config.General.SpecificationFormat == "" {
		return nil, nil, fmt.Errorf("must provide general.specification-format")
	}
	if config.General.SpecificationSource == "" {
		return nil, nil, fmt.Errorf("must provide general.specification-source")
	}
	model, err := createModel(config)
	if err != nil {
		return nil, nil, err
	}
	return model, config, nil
}

func createModel(config *config.Config) (*api.API, error) {
	var err error
	var model *api.API
	switch config.General.SpecificationFormat {
	case "openapi":
		model, err = parser.ParseOpenAPI(config.General.SpecificationSource, config.General.ServiceConfig, config.Source)
	case "protobuf":
		model, err = parser.ParseProtobuf(config.General.SpecificationSource, config.General.ServiceConfig, config.Source)
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown parser %q", config.General.SpecificationFormat)
	}
	if err != nil {
		return nil, err
	}
	api.LabelRecursiveFields(model)
	if err := api.CrossReference(model); err != nil {
		return nil, err
	}
	if err := api.SkipModelElements(model, config.Source); err != nil {
		return nil, err
	}
	if err := api.PatchDocumentation(model, config); err != nil {
		return nil, err
	}
	// Verify all the services, messages and enums are in the same package.
	if err := api.Validate(model); err != nil {
		return nil, err
	}
	if name, ok := config.Source["name-override"]; ok {
		model.Name = name
	}
	if title, ok := config.Source["title-override"]; ok {
		model.Title = title
	}
	if description, ok := config.Source["description-override"]; ok {
		model.Description = description
	}
	return model, nil
}

func refreshDir(rootConfig *config.Config, cmdLine *CommandLine, output string) error {
	model, config, err := loadDir(rootConfig, output)
	if err != nil {
		return err
	}
	if cmdLine.DryRun {
		return nil
	}

	switch config.General.Language {
	case "rust":
		return rust.Generate(model, output, config)
	case "rust_storage":
		// The StorageControl client depends on multiple specification sources.
		// We load them both here manually, and pass them along to
		// `rust.GenerateStorage` which will merge them appropriately.
		storageModel, storageConfig, err := loadDir(rootConfig, "src/storage/src/generated/gapic")
		if err != nil {
			return err
		}
		controlModel, controlConfig, err := loadDir(rootConfig, "src/storage/src/generated/gapic_control")
		if err != nil {
			return err
		}
		return rust.GenerateStorage(output, storageModel, storageConfig, controlModel, controlConfig)
	case "rust+prost":
		return rust_prost.Generate(model, output, config)
	case "go":
		return golang.Generate(model, output, config)
	case "dart":
		return dart.Generate(model, output, config)
	case "sample":
		return codec_sample.Generate(model, output, config)
	case "gcloud":
		return gcloud.Generate(model, output, config)
	default:
		return fmt.Errorf("unknown language: %s", config.General.Language)
	}
}
