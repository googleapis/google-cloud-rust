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

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/parser/openapi"
	"github.com/googleapis/google-cloud-rust/generator/internal/parser/protobuf"
)

// refresh reruns the generator in one directory, using the configuration
// parameters saved in its `.sidekick.toml` file.
func refresh(rootConfig *Config, cmdLine *CommandLine, output string) error {
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

	specFormat := config.General.SpecificationFormat
	popts := &genclient.ParserOptions{
		Source:        config.General.SpecificationSource,
		ServiceConfig: config.General.ServiceConfig,
		Options:       config.Source,
	}

	copts := &genclient.CodecOptions{
		Language:    config.General.Language,
		OutDir:      output,
		TemplateDir: config.General.TemplateDir,
		Options:     config.Codec,
	}

	var api *genclient.API
	switch specFormat {
	case "openapi":
		api, err = openapi.Parse(*popts)
	case "protobuf":
		api, err = protobuf.Parse(*popts)
	default:
		return fmt.Errorf("unknown parser %q", specFormat)
	}
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
		OutDir:      copts.OutDir,
		TemplateDir: copts.TemplateDir,
	}
	if cmdLine.DryRun {
		return nil
	}
	return genclient.Generate(request)
}
