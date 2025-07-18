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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

const (
	// projectRoot is the root of the google-cloud-rust. The golden files for
	// these tests depend on code in ../../auth and ../../src/gax.
	projectRoot = "../.."
	testdataDir = "testdata"
)

var (
	googleapisRoot             = fmt.Sprintf("%s/googleapis", testdataDir)
	outputDir                  = fmt.Sprintf("%s/test-only", testdataDir)
	secretManagerServiceConfig = "googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml"
	specificationSource        = fmt.Sprintf("%s/openapi/secretmanager_openapi_v1.json", testdataDir)
)

func TestRustFromOpenAPI(t *testing.T) {
	outDir, err := os.MkdirTemp(t.TempDir(), "golden")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outDir)
	cmdLine := &CommandLine{
		Command:             []string{},
		ProjectRoot:         projectRoot,
		SpecificationFormat: "openapi",
		SpecificationSource: specificationSource,
		ServiceConfig:       fmt.Sprintf("%s/%s", testdataDir, secretManagerServiceConfig),
		Language:            "rust",
		Output:              outDir,
		Codec: map[string]string{
			"not-for-publication":       "true",
			"copyright-year":            "2024",
			"package-name-override":     "secretmanager-golden-openapi",
			"package:wkt":               "package=google-cloud-wkt,source=google.protobuf",
			"package:gax":               "package=gcp-sdk-gax,feature=unstable-sdk-client",
			"disabled-rustdoc-warnings": "redundant_explicit_links",
		},
	}

	cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
	if err := runCommand(cmdGenerate, cmdLine); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{".sidekick.toml", "README.md", "Cargo.toml", "src/lib.rs"} {
		filename := path.Join(outDir, expected)
		stat, err := os.Stat(filename)
		if os.IsNotExist(err) {
			t.Errorf("missing %s: %s", filename, err)
		}
		if stat.Mode().Perm()|0666 != 0666 {
			t.Errorf("generated files should not be executable %s: %o", filename, stat.Mode())
		}
	}
}

func TestRustFromProtobuf(t *testing.T) {
	outDir, err := os.MkdirTemp(t.TempDir(), "golden")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outDir)

	type TestConfig struct {
		Source        string
		ServiceConfig string
		Name          string
		ExtraOptions  map[string]string
	}

	configs := []TestConfig{
		{
			Source:        "googleapis/google/type",
			ServiceConfig: "googleapis/google/type/type.yaml",
			Name:          "type",
		},
		{
			Source:        "googleapis/google/cloud/location",
			ServiceConfig: "googleapis/google/cloud/location/cloud.yaml",
			Name:          "location",
		},
		{
			Source: "googleapis/google/iam/v1",
			Name:   "iam/v1",
			ExtraOptions: map[string]string{
				"package:gtype":             "package=type-golden-protobuf,source=google.type",
				"disabled-rustdoc-warnings": "redundant_explicit_links,broken_intra_doc_links",
			},
		},
		{
			Source:        "googleapis/google/cloud/secretmanager/v1",
			ServiceConfig: secretManagerServiceConfig,
			Name:          "secretmanager",
			ExtraOptions: map[string]string{
				"package:iam":               "package=iam-v1-golden-protobuf,source=google.iam.v1",
				"package:location":          "package=location-golden-protobuf,source=google.cloud.location",
				"disabled-rustdoc-warnings": "broken_intra_doc_links",
			},
		},
	}
	for _, config := range configs {
		if config.Source != "" {
			config.Source = filepath.Join(testdataDir, config.Source)
		}
		if config.ServiceConfig != "" {
			config.ServiceConfig = filepath.Join(testdataDir, config.ServiceConfig)
		}
		cmdLine := &CommandLine{
			Command:             []string{},
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": googleapisRoot,
			},
			ServiceConfig: config.ServiceConfig,
			Language:      "rust",
			Output:        path.Join(outDir, config.Name),
			Codec: map[string]string{
				"not-for-publication":   "true",
				"copyright-year":        "2024",
				"package-name-override": strings.Replace(config.Name, "/", "-", -1) + "-golden-protobuf",
				"package:wkt":           "package=google-cloud-wkt,source=google.protobuf",
				"package:gax":           "package=gcp-sdk-gax,feature=unstable-sdk-client",
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
		if err := runCommand(cmdGenerate, cmdLine); err != nil {
			t.Fatal(err)
		}

		for _, expected := range []string{".sidekick.toml", "README.md", "Cargo.toml", "src/lib.rs"} {
			filename := path.Join(outDir, config.Name, expected)
			stat, err := os.Stat(filename)
			if os.IsNotExist(err) {
				t.Errorf("missing %s: %s", filename, err)
			}
			if stat.Mode().Perm()|0666 != 0666 {
				t.Errorf("generated files should not be executable %s: %o", filename, stat.Mode())
			}
		}
	}
}

func TestRustModuleFromProtobuf(t *testing.T) {
	outDir, err := os.MkdirTemp(t.TempDir(), "golden")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outDir)

	type TestConfig struct {
		Source        string
		ServiceConfig string
		Name          string
		ExtraOptions  map[string]string
	}
	configs := []TestConfig{
		{
			Source:        "google/rpc",
			ServiceConfig: "google/rpc/rpc_publish.yaml",
			Name:          "rpc",
			ExtraOptions: map[string]string{
				"module-path": "crate::error::rpc::generated",
				"package:wkt": "package=google-cloud-wkt,source=google.protobuf",
			},
		},
		{
			Source:        "google/type",
			ServiceConfig: "google/type/type.yaml",
			Name:          "type",
		},
	}

	for _, config := range configs {
		cmdLine := &CommandLine{
			Command:             []string{},
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": googleapisRoot,
			},
			ServiceConfig: config.ServiceConfig,
			Language:      "rust",
			Output:        path.Join(outDir, config.Name),
			Codec: map[string]string{
				"copyright-year":    "2024",
				"template-override": "templates/mod",
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
		if err := runCommand(cmdGenerate, cmdLine); err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{".sidekick.toml", "mod.rs"} {
			filename := path.Join(outDir, config.Name, expected)
			stat, err := os.Stat(filename)
			if os.IsNotExist(err) {
				t.Errorf("missing %s: %s", filename, err)
			}
			if stat.Mode().Perm()|0666 != 0666 {
				t.Errorf("generated files should not be executable %s: %o", filename, stat.Mode())
			}
		}
	}
}

func TestRustBootstrapWkt(t *testing.T) {
	outDir, err := os.MkdirTemp(t.TempDir(), "golden")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outDir)

	type TestConfig struct {
		Source        string
		ServiceConfig string
		Name          string
		SourceOptions map[string]string
		CodecOptions  map[string]string
	}
	configs := []TestConfig{
		{
			Source: "google/protobuf",
			Name:   "wkt",
			SourceOptions: map[string]string{
				"include-list": "source_context.proto",
			},
			CodecOptions: map[string]string{
				"module-path": "crate",
			},
		},
	}

	for _, config := range configs {
		cmdLine := &CommandLine{
			Command:             []string{},
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": testdataDir,
			},
			Language: "rust",
			Output:   path.Join(outDir, config.Name),
			Codec: map[string]string{
				"copyright-year":    "2025",
				"template-override": "templates/mod",
			},
		}
		for k, v := range config.SourceOptions {
			cmdLine.Source[k] = v
		}
		for k, v := range config.CodecOptions {
			cmdLine.Codec[k] = v
		}
		cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
		if err := runCommand(cmdGenerate, cmdLine); err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{".sidekick.toml", "mod.rs"} {
			filename := path.Join(outDir, config.Name, expected)
			stat, err := os.Stat(filename)
			if os.IsNotExist(err) {
				t.Errorf("missing %s: %s", filename, err)
			}
			if stat.Mode().Perm()|0666 != 0666 {
				t.Errorf("generated files should not be executable %s: %o", filename, stat.Mode())
			}
		}
	}
}

func TestRustOverrideTitleAndDescription(t *testing.T) {
	outDir, err := os.MkdirTemp(t.TempDir(), "golden")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outDir)
	titleOverride := "Replace or Provide Custom Title"
	descriptionOverride := "Replace or Provide Custom Description\nIncluding multiple lines."
	cmdLine := &CommandLine{
		Command:             []string{},
		ProjectRoot:         projectRoot,
		SpecificationFormat: "protobuf",
		SpecificationSource: "google/type",
		Language:            "rust",
		Source: map[string]string{
			"googleapis-root":      googleapisRoot,
			"title-override":       titleOverride,
			"description-override": descriptionOverride,
		},
		Output: outDir,
		Codec: map[string]string{
			"copyright-year":        "2025",
			"package-name-override": "google-cloud-test-only",
		},
	}
	cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
	if err := runCommand(cmdGenerate, cmdLine); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{".sidekick.toml", "README.md", "Cargo.toml", "src/lib.rs"} {
		filename := path.Join(outDir, expected)
		stat, err := os.Stat(filename)
		if os.IsNotExist(err) {
			t.Errorf("missing %s: %s", filename, err)
		}
		if stat.Mode().Perm()|0666 != 0666 {
			t.Errorf("generated files should not be executable %s: %o", filename, stat.Mode())
		}
	}
	contents, err := os.ReadFile(path.Join(outDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), titleOverride) {
		t.Errorf("missing title override in README, want=%s, got=%s", titleOverride, contents)
	}
	if !strings.Contains(string(contents), descriptionOverride) {
		t.Errorf("missing description override in README, want=%s, got=%s", descriptionOverride, contents)
	}
}

func TestGoFromProtobuf(t *testing.T) {
	outDir, err := os.MkdirTemp(t.TempDir(), "golden")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outDir)

	type TestConfig struct {
		Source       string
		Name         string
		ExtraOptions map[string]string
		ModReplace   map[string]string
	}
	configs := []TestConfig{
		{
			Source: fmt.Sprintf("%s/google/type", googleapisRoot),
			Name:   "typez",
			ExtraOptions: map[string]string{
				"go-package-name": "typez",
			},
		},
		{
			Source: fmt.Sprintf("%s/google/iam/v1", googleapisRoot),
			Name:   "iam/v1",
			ExtraOptions: map[string]string{
				"import-mapping:google.type":     "typez;typez",
				"import-mapping:google.protobuf": "wkt;wkt",
				"go-package-name":                "iam",
			},
			ModReplace: map[string]string{},
		},
	}

	for _, config := range configs {
		cmdLine := &CommandLine{
			Command:             []string{},
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": googleapisRoot,
			},
			ServiceConfig: "",
			Language:      "go",
			Output:        path.Join(outDir, config.Name),
			Codec: map[string]string{
				"not-for-publication":   "true",
				"copyright-year":        "2024",
				"package-name-override": fmt.Sprintf("golden/%s", config.Name),
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
		if err := runCommand(cmdGenerate, cmdLine); err != nil {
			t.Fatal(err)
		}

		dir := path.Join(outDir, config.Name)
		execCommand(t, dir, "goimports", "-w", ".")
		execCommand(t, dir, "go", "mod", "tidy")
		for _, expected := range []string{".sidekick.toml", "go.mod", "client.go"} {
			filename := path.Join(outDir, config.Name, expected)
			stat, err := os.Stat(filename)
			if os.IsNotExist(err) {
				t.Errorf("missing %s: %s", filename, err)
			}
			if stat.Mode().Perm()|0666 != 0666 {
				t.Errorf("generated files should not be executable %s: %o", filename, stat.Mode())
			}
		}
	}
}

func execCommand(t *testing.T, dir, c string, arg ...string) {
	t.Helper()
	cmd := exec.Command(c, arg...)
	cmd.Dir = dir
	t.Logf("cd %s && %s", cmd.Dir, cmd.String())
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		t.Fatalf("%v: %v\n%s", cmd, err, output)
	}
}
