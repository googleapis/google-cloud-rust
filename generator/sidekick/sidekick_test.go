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
	"errors"
	"flag"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestRustFromOpenAPI(t *testing.T) {
	const (
		projectRoot = "../.."
		outDir      = "generator/testdata/rust/openapi/golden"
	)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatal(err)
	}
	args := []string{
		"-specification-format", "openapi",
		"-specification-source", "generator/testdata/openapi/secretmanager_openapi_v1.json",
		"-service-config", "generator/testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml",
		"-language", "rust",
		"-output", outDir,
		"-template-dir", "generator/templates",
		"-codec-option", "copyright-year=2024",
		"-codec-option", "package-name-override=secretmanager-golden-openapi",
		"-codec-option", "package:wkt=package=gcp-sdk-wkt,path=src/wkt,source=google.protobuf",
		"-codec-option", "package:gax=package=gcp-sdk-gax,path=src/gax,feature=sdk_client",
		"-codec-option", "package:google-cloud-auth=package=google-cloud-auth,path=auth",
	}
	if err := Generate(args); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("cargo", "fmt", "--manifest-path", path.Join(outDir, "Cargo.toml"))
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		t.Fatalf("%v: %v\n%s", cmd, err, output)
	}
}

func TestRustFromProtobuf(t *testing.T) {
	const (
		projectRoot = "../.."
		outDir      = "generator/testdata/rust/gclient/golden"
	)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatal(err)
	}
	type Config struct {
		Source       string
		Name         string
		ExtraOptions []string
	}

	configs := []Config{
		{
			Source: "generator/testdata/googleapis/google/type",
			Name:   "type",
			ExtraOptions: []string{
				"-service-config", "generator/testdata/googleapis/google/type/type.yaml",
			},
		},
		{
			Source: "generator/testdata/googleapis/google/cloud/location",
			Name:   "location",
			ExtraOptions: []string{
				"-service-config", "generator/testdata/googleapis/google/cloud/location/cloud.yaml",
			},
		},
		{
			Source: "generator/testdata/googleapis/google/iam/v1",
			Name:   "iam/v1",
			ExtraOptions: []string{
				"-codec-option", "package:gtype=package=type-golden-gclient,path=generator/testdata/rust/gclient/golden/type,source=google.type",
			},
		},
		{
			Source: "generator/testdata/googleapis/google/cloud/secretmanager/v1",
			Name:   "secretmanager",
			ExtraOptions: []string{
				"-service-config", "generator/testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml",
				"-codec-option", "package:iam=package=iam-v1-golden-gclient,path=generator/testdata/rust/gclient/golden/iam/v1,source=google.iam.v1",
			},
		},
	}

	for _, config := range configs {
		args := []string{
			"-specification-format", "protobuf",
			"-specification-source", config.Source,
			"-parser-option", "googleapis-root=generator/testdata/googleapis",
			"-language", "rust",
			"-output", path.Join(outDir, config.Name),
			"-template-dir", "generator/templates",
			"-codec-option", "copyright-year=2024",
			"-codec-option", "package-name-override=" + strings.Replace(config.Name, "/", "-", -1) + "-golden-gclient",
			"-codec-option", "package:wkt=package=gcp-sdk-wkt,path=src/wkt,source=google.protobuf",
			"-codec-option", "package:gax=package=gcp-sdk-gax,path=src/gax,feature=sdk_client",
			"-codec-option", "package:google-cloud-auth=package=google-cloud-auth,path=auth",
		}
		args = append(args, config.ExtraOptions...)
		if err := Generate(args); err != nil {
			t.Fatal(err)
		}

		manifest := path.Join(outDir, config.Name, "Cargo.toml")
		if _, err := os.Stat(manifest); os.IsNotExist(err) {
			// The module test does not produce a Cargo.toml file
			continue
		}
		cmd := exec.Command("cargo", "fmt", "--manifest-path", manifest)
		if output, err := cmd.CombinedOutput(); err != nil {
			if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
				t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
			}
			t.Fatalf("%v: %v\n%s", cmd, err, output)
		}
	}
}

func TestRustModuleFromProtobuf(t *testing.T) {
	const (
		projectRoot = "../.."
	)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatal(err)
	}

	type TestConfig struct {
		Source       string
		Name         string
		ExtraOptions []string
	}
	configs := []TestConfig{
		{
			Source: "generator/testdata/googleapis/google/rpc/error_details.proto",
			Name:   "rpc",
			ExtraOptions: []string{
				"-service-config", "generator/testdata/googleapis/google/rpc/rpc_publish.yaml",
				"-codec-option", "package:wkt=package=gcp-sdk-wkt,path=src/wkt,source=google.protobuf",
			},
		},
		{
			Source: "generator/testdata/googleapis/google/type",
			Name:   "type",
			ExtraOptions: []string{
				"-service-config", "generator/testdata/googleapis/google/type/type.yaml",
			},
		},
	}

	for _, config := range configs {
		args := []string{
			"-specification-format", "protobuf",
			"-specification-source", config.Source,
			"-parser-option", "googleapis-root=generator/testdata/googleapis",
			"-language", "rust",
			"-output", path.Join("generator/testdata/rust/gclient/golden/module", config.Name),
			"-template-dir", "generator/templates",
			"-codec-option", "copyright-year=2024",
			"-codec-option", "generate-module=true",
		}
		args = append(args, config.ExtraOptions...)

		if err := Generate(args); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGoFromProtobuf(t *testing.T) {
	const (
		projectRoot = "../.."
		outDir      = "generator/testdata/go/gclient/golden"
	)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatal(err)
	}

	type Config struct {
		Source       string
		Name         string
		ExtraOptions []string
		ModReplace   map[string]string
	}
	configs := []Config{
		{
			Source: "generator/testdata/googleapis/google/type",
			Name:   "typez",
			ExtraOptions: []string{
				"-codec-option", "go-package-name=typez",
				"-codec-option", "package-name-override=github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez",
			},
		},
		{
			Source: "generator/testdata/googleapis/google/iam/v1",
			Name:   "iam/v1",
			ExtraOptions: []string{
				"-codec-option", "package-name-override=github.com/google-cloud-rust/generator/testdata/go/gclient/golden/iam/v1",
				"-codec-option", "import-mapping:google.type=github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez;typez",
				"-codec-option", "import-mapping:google.protobuf=github.com/google-cloud-rust/generator/testdata/go/gclient/golden/wkt;wkt",
				"-codec-option", "go-package-name=iam",
			},
			ModReplace: map[string]string{
				"github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez": "typez",
				"github.com/google-cloud-rust/generator/testdata/go/gclient/golden/wkt":   "wkt",
			},
		},
	}

	for _, config := range configs {
		args := []string{
			"-specification-format", "protobuf",
			"-specification-source", config.Source,
			"-parser-option", "googleapis-root=generator/testdata/googleapis",
			"-language", "go",
			"-output", path.Join(outDir, config.Name),
			"-template-dir", "generator/templates",
			"-codec-option", "copyright-year=2024",
			"-codec-option", "package-name-override=github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez",
		}
		args = append(args, config.ExtraOptions...)
		if err := Generate(args); err != nil {
			t.Fatal(err)
		}

		cmd := exec.Command("goimports", "-w", ".")
		cmd.Dir = path.Join(outDir, config.Name)
		if output, err := cmd.CombinedOutput(); err != nil {
			if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
				t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
			}
			t.Fatalf("%v: %v\n%s", cmd, err, output)
		}

		for _, key := range orderedKeys(config.ModReplace) {
			cmd = exec.Command("go", "mod", "edit", "-replace", key+"=../../"+config.ModReplace[key])
			cmd.Dir = path.Join(outDir, config.Name)
			if output, err := cmd.CombinedOutput(); err != nil {
				if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
					t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
				}
				t.Fatalf("%v: %v\n%s", cmd, err, output)
			}
		}

		cmd = exec.Command("go", "mod", "tidy")
		cmd.Dir = path.Join(outDir, config.Name)
		if output, err := cmd.CombinedOutput(); err != nil {
			if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
				t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
			}
			t.Fatalf("%v: %v\n%s", cmd, err, output)
		}
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
