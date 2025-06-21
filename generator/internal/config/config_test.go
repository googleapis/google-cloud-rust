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

package config

import (
	"bytes"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	toml "github.com/pelletier/go-toml/v2"
)

func TestLoadRootConfigOnlyGeneral(t *testing.T) {
	tempFile, err := os.CreateTemp(t.TempDir(), "root-config-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			SpecificationFormat: "root-specification-format",
		},
	}

	to := toml.NewEncoder(tempFile)
	if err := to.Encode(&root); err != nil {
		t.Fatal(err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := LoadRootConfig(tempFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		General: root.General,
		Source:  map[string]string{},
		Codec:   map[string]string{},
	}
	if diff := cmp.Diff(want, got); len(diff) != 0 {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func TestLoadRootConfig(t *testing.T) {
	tempFile, err := os.CreateTemp(t.TempDir(), "root-config-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			SpecificationFormat: "root-specification-format",
		},
		Source: map[string]string{
			"s1": "v1",
			"s2": "v2",
		},
		Codec: map[string]string{
			"o1": "v3",
			"o2": "v4",
		},
	}

	to := toml.NewEncoder(tempFile)
	if err := to.Encode(&root); err != nil {
		t.Fatal(err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := LoadRootConfig(tempFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(&root, got); len(diff) != 0 {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func TestMergeLocalForGeneral(t *testing.T) {
	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			SpecificationFormat: "root-specification-format",
		},
	}

	local := Config{
		General: GeneralConfig{
			Language:            "local-language",
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

func TestMergeLocalForDocumentationOverrides(t *testing.T) {
	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			SpecificationFormat: "root-specification-format",
		},
		CommentOverrides: []DocumentationOverride{
			{
				ID: "root.Override",
			},
		},
	}

	local := Config{
		General: GeneralConfig{
			Language:            "local-language",
			SpecificationFormat: "local-specification-format",
			SpecificationSource: "local-specification-source",
			ServiceConfig:       "local-service-config",
		},
		CommentOverrides: []DocumentationOverride{
			{
				ID: "local.Override",
			},
		},
	}

	got, err := mergeTestConfigs(t, &root, &local)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		General: GeneralConfig{
			Language:            "local-language",
			SpecificationFormat: "local-specification-format",
			SpecificationSource: "local-specification-source",
			ServiceConfig:       "local-service-config",
		},
		Codec:  map[string]string{},
		Source: map[string]string{},
		CommentOverrides: []DocumentationOverride{
			{
				ID: "local.Override",
			},
		},
	}

	if diff := cmp.Diff(want, got); len(diff) != 0 {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func TestMergeIgnoreRootSourceAndServiceConfig(t *testing.T) {
	root := Config{
		General: GeneralConfig{
			Language:            "root-language",
			SpecificationFormat: "root-specification-format",
			SpecificationSource: "root-specification-source",
			ServiceConfig:       "root-service-config",
		},
	}

	local := Config{
		General: GeneralConfig{
			Language:            "local-language",
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

func TestSaveOmitEmpty(t *testing.T) {
	input := Config{
		General: GeneralConfig{
			SpecificationSource: "test-only-source",
			ServiceConfig:       "test-only-config",
		},
		Codec:  map[string]string{},
		Source: map[string]string{},
	}
	output := bytes.Buffer{}

	to := toml.NewEncoder(&output)
	if err := to.Encode(input); err != nil {
		t.Fatal(err)
	}

	got := output.String()
	want := `[general]
specification-source = 'test-only-source'
service-config = 'test-only-config'
`

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
	return MergeConfigAndFile(root, tempFile.Name())
}
