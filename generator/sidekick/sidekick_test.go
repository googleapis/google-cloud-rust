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
		"-codec-option", "package:wkt=package=types,path=types,source=google.protobuf",
		"-codec-option", "package:gax=package=gax,path=gax,feature=sdk_client",
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
			"-codec-option", "package:wkt=package=types,path=types,source=google.protobuf",
			"-codec-option", "package:gax=package=gax,path=gax,feature=sdk_client",
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
	args := []string{
		"-specification-format", "protobuf",
		"-specification-source", "generator/testdata/googleapis/google/type",
		"-parser-option", "googleapis-root=generator/testdata/googleapis",
		"-language", "rust",
		"-output", "generator/testdata/rust/gclient/golden/module",
		"-template-dir", "generator/templates",
		"-codec-option", "copyright-year=2024",
		"-codec-option", "generate-module=true",
	}

	if err := Generate(args); err != nil {
		t.Fatal(err)
	}
}
