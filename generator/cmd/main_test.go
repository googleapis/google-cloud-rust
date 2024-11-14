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
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestRustFromOpenAPI(t *testing.T) {
	const (
		projectRoot = ".."
		outDir      = "testdata/rust/openapi/golden"
	)

	popts := &genclient.ParserOptions{
		Source:        "../testdata/openapi/secretmanager_openapi_v1.json",
		ServiceConfig: "../testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml",
		Options:       map[string]string{},
	}

	copts := &genclient.CodecOptions{
		Language:    "rust",
		ProjectRoot: projectRoot,
		OutDir:      "testdata/rust/openapi/golden",
		TemplateDir: "../templates",
		Options: map[string]string{
			"package-name-override":   "secretmanager-golden-openapi",
			"package:gax_placeholder": "package=types,path=../../../../../types,source=google.protobuf",
			"package:gax":             "package=gax,path=../../../../../gax",
		},
	}
	err := Generate("openapi", popts, copts)
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("cargo", "fmt", "--manifest-path", path.Join(projectRoot, outDir, "Cargo.toml"))
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		t.Fatalf("%v: %v\n%s", cmd, err, output)
	}
}

func TestRustFromProtobuf(t *testing.T) {
	const (
		projectRoot = ".."
		outDir      = "testdata/rust/gclient/golden"
	)

	type Config struct {
		Source       string
		Name         string
		ExtraOptions map[string]string
	}

	configs := []Config{
		{
			Source: "../testdata/googleapis/google/type",
			Name:   "type",
		},
		{
			Source: "../testdata/googleapis/google/iam/v1",
			Name:   "iam/v1",
			ExtraOptions: map[string]string{
				"package:gtype": "package=type-golden-gclient,path=../../type,source=google.type",
			},
		},
		{
			Source: "../testdata/googleapis/google/cloud/secretmanager/v1",
			Name:   "secretmanager",
			ExtraOptions: map[string]string{
				"package:iam": "package=iam-v1-golden-gclient,path=../iam/v1,source=google.iam.v1",
			},
		},
	}

	for _, config := range configs {
		depth := strings.Count(outDir, "/") + strings.Count(config.Name, "/") + 2
		toProjectRoot := ".."
		for range depth {
			toProjectRoot = path.Join(toProjectRoot, "..")
		}
		popts := &genclient.ParserOptions{
			Source: config.Source,
			Options: map[string]string{
				"googleapis-root": "../testdata/googleapis",
				"input-root":      "../testdata",
			},
		}
		options := map[string]string{
			"package-name-override":   strings.Replace(config.Name, "/", "-", -1) + "-golden-gclient",
			"package:gax_placeholder": fmt.Sprintf("package=types,path=%s/types,source=google.protobuf", toProjectRoot),
			"package:gax":             fmt.Sprintf("package=gax,path=%s/gax", toProjectRoot),
		}
		for k, v := range config.ExtraOptions {
			options[k] = v
		}
		copts := &genclient.CodecOptions{
			Language:    "rust",
			ProjectRoot: projectRoot,
			OutDir:      path.Join(outDir, config.Name),
			TemplateDir: "../templates",
			Options:     options,
		}
		err := Generate("protobuf", popts, copts)
		if err != nil {
			t.Fatal(err)
		}

		cmd := exec.Command("cargo", "fmt", "--manifest-path", path.Join(projectRoot, outDir, config.Name, "Cargo.toml"))
		if output, err := cmd.CombinedOutput(); err != nil {
			if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
				t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
			}
			t.Fatalf("%v: %v\n%s", cmd, err, output)
		}
	}
}
