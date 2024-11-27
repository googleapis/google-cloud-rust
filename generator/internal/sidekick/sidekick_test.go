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
	"sort"
	"strings"
	"testing"
)

const (
	// projectRoot is the root of the google-cloud-rust. The golden files for
	// these tests depend on code in ../../auth and ../../src/gax.
	projectRoot = "../../.."
	testdataDir = "generator/testdata"
)

func TestRustFromOpenAPI(t *testing.T) {
	var outDir = fmt.Sprintf("%s/rust/openapi/golden", testdataDir)
	cmdLine := &CommandLine{
		Command:             "generate",
		ProjectRoot:         projectRoot,
		SpecificationFormat: "openapi",
		SpecificationSource: "generator/testdata/openapi/secretmanager_openapi_v1.json",
		ServiceConfig:       "generator/testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml",
		Language:            "rust",
		Output:              outDir,
		TemplateDir:         "generator/templates",
		Codec: map[string]string{
			"copyright-year":            "2024",
			"package-name-override":     "secretmanager-golden-openapi",
			"package:wkt":               "package=gcp-sdk-wkt,path=src/wkt,source=google.protobuf",
			"package:gax":               "package=gcp-sdk-gax,path=src/gax,feature=sdk_client",
			"package:google-cloud-auth": "package=google-cloud-auth,path=auth",
		},
	}
	if err := runSidekick(cmdLine); err != nil {
		t.Fatal(err)
	}
	runCommand(t, ".", "cargo", "fmt", "--manifest-path", path.Join(projectRoot, outDir, "Cargo.toml"))
}

func TestRustFromProtobuf(t *testing.T) {
	var outDir = fmt.Sprintf("%s/rust/gclient/golden", testdataDir)

	type TestConfig struct {
		Source        string
		ServiceConfig string
		Name          string
		ExtraOptions  map[string]string
	}

	configs := []TestConfig{
		{
			Source:        "generator/testdata/googleapis/google/type",
			ServiceConfig: "generator/testdata/googleapis/google/type/type.yaml",
			Name:          "type",
		},
		{
			Source:        "generator/testdata/googleapis/google/cloud/location",
			ServiceConfig: "generator/testdata/googleapis/google/cloud/location/cloud.yaml",
			Name:          "location",
		},
		{
			Source: "generator/testdata/googleapis/google/iam/v1",
			Name:   "iam/v1",
			ExtraOptions: map[string]string{
				"package:gtype": "package=type-golden-gclient,path=generator/testdata/rust/gclient/golden/type,source=google.type",
			},
		},
		{
			Source:        "generator/testdata/googleapis/google/cloud/secretmanager/v1",
			ServiceConfig: "generator/testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml",
			Name:          "secretmanager",
			ExtraOptions: map[string]string{
				"package:iam":      "package=iam-v1-golden-gclient,path=generator/testdata/rust/gclient/golden/iam/v1,source=google.iam.v1",
				"package:location": "package=location-golden-gclient,path=generator/testdata/rust/gclient/golden/location,source=google.cloud.location",
			},
		},
	}

	for _, config := range configs {
		cmdLine := &CommandLine{
			Command:             "generate",
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": "generator/testdata/googleapis",
			},
			ServiceConfig: config.ServiceConfig,
			Language:      "rust",
			Output:        path.Join(outDir, config.Name),
			TemplateDir:   "generator/templates",
			Codec: map[string]string{
				"copyright-year":            "2024",
				"package-name-override":     strings.Replace(config.Name, "/", "-", -1) + "-golden-gclient",
				"package:wkt":               "package=gcp-sdk-wkt,path=src/wkt,source=google.protobuf",
				"package:gax":               "package=gcp-sdk-gax,path=src/gax,feature=sdk_client",
				"package:google-cloud-auth": "package=google-cloud-auth,path=auth",
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		if err := runSidekick(cmdLine); err != nil {
			t.Fatal(err)
		}

		manifest := path.Join(projectRoot, outDir, config.Name, "Cargo.toml")
		if _, err := os.Stat(manifest); os.IsNotExist(err) {
			// The module test does not produce a Cargo.toml file
			continue
		}
		runCommand(t, ".", "cargo", "fmt", "--manifest-path", manifest)
	}
}

func TestRustModuleFromProtobuf(t *testing.T) {
	type TestConfig struct {
		Source        string
		ServiceConfig string
		Name          string
		ExtraOptions  map[string]string
	}
	configs := []TestConfig{
		{
			Source:        "google/rpc/error_details.proto",
			ServiceConfig: "google/rpc/rpc_publish.yaml",
			Name:          "rpc",
			ExtraOptions: map[string]string{
				"module-path":               "error::rpc::generated",
				"deserialize-with-defaults": "false",
				"package:wkt":               "package=gcp-sdk-wkt,path=src/wkt,source=google.protobuf",
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
			Command:             "generate",
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": "generator/testdata/googleapis",
			},
			ServiceConfig: config.ServiceConfig,
			Language:      "rust",
			Output:        path.Join("generator/testdata/rust/gclient/golden/module", config.Name),
			TemplateDir:   "generator/templates",
			Codec: map[string]string{
				"copyright-year":  "2024",
				"generate-module": "true",
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		if err := runSidekick(cmdLine); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGoFromProtobuf(t *testing.T) {
	var outDir = fmt.Sprintf("%s/go/gclient/golden", testdataDir)
	type TestConfig struct {
		Source       string
		Name         string
		ExtraOptions map[string]string
		ModReplace   map[string]string
	}
	configs := []TestConfig{
		{
			Source: "generator/testdata/googleapis/google/type",
			Name:   "typez",
			ExtraOptions: map[string]string{
				"go-package-name": "typez",
			},
		},
		{
			Source: "generator/testdata/googleapis/google/iam/v1",
			Name:   "iam/v1",
			ExtraOptions: map[string]string{
				"import-mapping:google.type":     "github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez;typez",
				"import-mapping:google.protobuf": "github.com/google-cloud-rust/generator/testdata/go/gclient/golden/wkt;wkt",
				"go-package-name":                "iam",
			},
			ModReplace: map[string]string{
				"github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez": "typez",
				"github.com/google-cloud-rust/generator/testdata/go/gclient/golden/wkt":   "wkt",
			},
		},
	}

	for _, config := range configs {
		cmdLine := &CommandLine{
			Command:             "generate",
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": "generator/testdata/googleapis",
			},
			ServiceConfig: "",
			Language:      "go",
			Output:        path.Join(outDir, config.Name),
			TemplateDir:   "generator/templates",
			Codec: map[string]string{
				"copyright-year":        "2024",
				"package-name-override": "github.com/google-cloud-rust/generator/testdata/go/gclient/golden/" + config.Name,
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		if err := runSidekick(cmdLine); err != nil {
			t.Fatal(err)
		}

		dir := path.Join(projectRoot, outDir, config.Name)
		runCommand(t, dir, "goimports", "-w", ".")

		for _, key := range orderedKeys(config.ModReplace) {
			dir := path.Join(projectRoot, outDir, config.Name)
			runCommand(t, dir, "go", "mod", "edit", "-replace", key+"=../../"+config.ModReplace[key])
		}
		runCommand(t, path.Join(projectRoot, outDir, config.Name), "go", "mod", "tidy")
	}
}

func runCommand(t *testing.T, dir, c string, arg ...string) {
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

func orderedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
