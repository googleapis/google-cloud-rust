// Copyright 2025 Google LLC
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
	"os"
	"path"
	"testing"
)

func TestRustProstConvert(t *testing.T) {
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
	}

	for _, config := range configs {
		cmdLine := &CommandLine{
			Command:             []string{},
			ProjectRoot:         projectRoot,
			SpecificationFormat: "protobuf",
			SpecificationSource: config.Source,
			Source: map[string]string{
				"googleapis-root": googleapisRoot,
				"skipped-ids":     ".google.rpc.Status",
			},
			ServiceConfig: config.ServiceConfig,
			Language:      "rust",
			Output:        path.Join(outDir, config.Name),
			Codec: map[string]string{
				"copyright-year":    "2024",
				"template-override": "templates/convert-prost",
			},
		}
		for k, v := range config.ExtraOptions {
			cmdLine.Codec[k] = v
		}
		cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
		if err := runCommand(cmdGenerate, cmdLine); err != nil {
			t.Fatal(err)
		}
	}
}
