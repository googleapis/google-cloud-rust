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
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/parser"
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
func refresh(rootConfig *Config, cmdLine *CommandLine) error {
	return refreshDir(rootConfig, cmdLine, cmdLine.Output)
}

func refreshDir(rootConfig *Config, cmdLine *CommandLine, output string) error {
	config, err := mergeConfigAndFile(rootConfig, path.Join(output, ".sidekick.toml"))
	if err != nil {
		return err
	}
	if config.General.SpecificationFormat == "" {
		return fmt.Errorf("must provide general.specification-format")
	}
	if config.General.SpecificationSource == "" {
		return fmt.Errorf("must provide general.specification-source")
	}

	var model *api.API
	switch config.General.SpecificationFormat {
	case "openapi":
		model, err = parser.ParseOpenAPI(config.General.SpecificationSource, config.General.ServiceConfig, config.Source)
	case "protobuf":
		model, err = parser.ParseProtobuf(config.General.SpecificationSource, config.General.ServiceConfig, config.Source)
	default:
		return fmt.Errorf("unknown parser %q", config.General.SpecificationFormat)
	}
	if err != nil {
		return err
	}
	if cmdLine.DryRun {
		return nil
	}
	return language.GenerateClient(model, config.General.Language, output, config.Codec)
}
