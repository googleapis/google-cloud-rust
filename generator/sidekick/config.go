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
	"fmt"
	"os"

	toml "github.com/pelletier/go-toml/v2"
)

type Config struct {
	General GeneralConfig `toml:"general"`

	Source map[string]string `toml:"source,omitempty"`
	Codec  map[string]string `toml:"codec,omitempty"`
}

// Configuration parameters that affect Parsers and Codecs, including the
// selection of parser and codec.
type GeneralConfig struct {
	Language            string `toml:"language,omitempty"`
	TemplateDir         string `toml:"template-dir,omitempty"`
	SpecificationFormat string `toml:"specification-format,omitempty"`
	SpecificationSource string `toml:"specification-source,omitempty"`
	ServiceConfig       string `toml:"service-config,omitempty"`
}

func LoadRootConfig(filename string) (*Config, error) {
	config := &Config{
		Codec:  map[string]string{},
		Source: map[string]string{},
	}
	if contents, err := os.ReadFile(filename); err == nil {
		err = toml.Unmarshal(contents, &config)
		if err != nil {
			return nil, fmt.Errorf("error reading top-level configuration: %w", err)
		}
	}
	// Ignore errors reading the top-level file.
	return config, nil
}

func MergeConfigAndFile(rootConfig *Config, filename string) (*Config, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var local Config
	err = toml.Unmarshal(contents, &local)
	if err != nil {
		return nil, fmt.Errorf("error reading configuration %s: %w", filename, err)
	}
	return MergeConfigs(rootConfig, &local)
}

func MergeConfigs(rootConfig, local *Config) (*Config, error) {
	merged := Config{
		General: GeneralConfig{
			Language:            rootConfig.General.Language,
			TemplateDir:         rootConfig.General.TemplateDir,
			SpecificationFormat: rootConfig.General.SpecificationFormat,
		},
		Source: map[string]string{},
		Codec:  map[string]string{},
	}
	for k, v := range rootConfig.Codec {
		merged.Codec[k] = v
	}
	for k, v := range rootConfig.Source {
		merged.Source[k] = v
	}

	// Ignore `SpecificationSource` and `ServiceConfig` at the top-level
	// configuration. It makes no sense to set those globally.
	merged.General.SpecificationSource = local.General.SpecificationSource
	merged.General.ServiceConfig = local.General.ServiceConfig
	if local.General.SpecificationFormat != "" {
		merged.General.SpecificationFormat = local.General.SpecificationFormat
	}
	if local.General.Language != "" {
		merged.General.Language = local.General.Language
	}
	if local.General.TemplateDir != "" {
		merged.General.TemplateDir = local.General.TemplateDir
	}
	for k, v := range local.Codec {
		merged.Codec[k] = v
	}
	for k, v := range local.Source {
		merged.Source[k] = v
	}
	// Ignore errors reading the top-level file.
	return &merged, nil
}
