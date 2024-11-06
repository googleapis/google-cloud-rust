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
	"testing"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/translator/openapi"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestRun_Rust(t *testing.T) {
	const (
		inputPath = "../../testdata/openapi/secretmanager_openapi_v1.json"
		outDir    = "../../testdata/rust/openapi/golden"
	)
	options := &openapi.Options{
		Language:      "rust",
		OutDir:        outDir,
		TemplateDir:   "../../templates",
		ServiceConfig: "../../testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml",
	}
	if err := run(inputPath, options); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("cargo", "fmt", "--manifest-path", outDir+"/Cargo.toml")
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		t.Fatalf("%v: %v\n%s", cmd, err, output)
	}
}
