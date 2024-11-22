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
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	toml "github.com/pelletier/go-toml/v2"
)

func TestMergeLocalForGeneral(t *testing.T) {
	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			TemplateDir:         "root-template-dir",
			SpecificationFormat: "root-specification-format",
		},
	}

	local := Config{
		General: GeneralConfig{
			Language:            "local-language",
			TemplateDir:         "local-template-dir",
			SpecificationFormat: "local-specification-format",
			SpecificationSource: "local-specification-source",
			ServiceConfig:       "local-service-config",
		},
	}

	got, err := mergeTestConfigs(t, &root, &local)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		General: GeneralConfig{
			Language:            "local-language",
			TemplateDir:         "local-template-dir",
			SpecificationFormat: "local-specification-format",
			SpecificationSource: "local-specification-source",
			ServiceConfig:       "local-service-config",
		},
		Codec:  map[string]string{},
		Source: map[string]string{},
	}

	if diff := cmp.Diff(want, got); len(diff) != 0 {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func TestMergeIgnoreRootSourceAndServiceConfig(t *testing.T) {
	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			TemplateDir:         "root-template-dir",
			SpecificationFormat: "root-specification-format",
			SpecificationSource: "root-specification-source",
			ServiceConfig:       "root-service-config",
		},
	}

	local := Config{
		General: GeneralConfig{
			Language:            "local-language",
			TemplateDir:         "local-template-dir",
			SpecificationFormat: "local-specification-format",
		},
	}

	got, err := mergeTestConfigs(t, &root, &local)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		General: GeneralConfig{
			Language:            "local-language",
			TemplateDir:         "local-template-dir",
			SpecificationFormat: "local-specification-format",
			SpecificationSource: "",
			ServiceConfig:       "",
		},
		Codec:  map[string]string{},
		Source: map[string]string{},
	}

	if diff := cmp.Diff(want, got); len(diff) != 0 {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func TestMergeCodecAndSource(t *testing.T) {
	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			TemplateDir:         "root-template-dir",
			SpecificationFormat: "root-specification-format",
		},
		Codec: map[string]string{
			"codec-a": "root-a-value",
			"codec-b": "root-b-value",
		},
		Source: map[string]string{
			"source-a": "root-a-value",
			"source-b": "root-b-value",
		},
	}

	local := Config{
		General: GeneralConfig{
			SpecificationSource: "local-specification-source",
			ServiceConfig:       "local-service-config",
		},
		Codec: map[string]string{
			"codec-b": "local-b-value",
			"codec-c": "local-c-value",
		},
		Source: map[string]string{
			"source-b": "local-b-value",
			"source-c": "local-c-value",
		},
	}

	got, err := mergeTestConfigs(t, &root, &local)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		General: GeneralConfig{
			Language:            "root-language",
			TemplateDir:         "root-template-dir",
			SpecificationFormat: "root-specification-format",
			SpecificationSource: "local-specification-source",
			ServiceConfig:       "local-service-config",
		},
		Codec: map[string]string{
			"codec-a": "root-a-value",
			"codec-b": "local-b-value",
			"codec-c": "local-c-value",
		},
		Source: map[string]string{
			"source-a": "root-a-value",
			"source-b": "local-b-value",
			"source-c": "local-c-value",
		},
	}

	if diff := cmp.Diff(want, got); len(diff) != 0 {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func mergeTestConfigs(t *testing.T, root, local *Config) (*Config, error) {
	t.Helper()
	tempFile, err := os.CreateTemp(t.TempDir(), "sidekick.toml")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tempFile.Name())
	to := toml.NewEncoder(tempFile)
	if err := to.Encode(local); err != nil {
		return nil, err
	}
	tempFile.Close()
	return MergeConfig(root, tempFile.Name())
}
