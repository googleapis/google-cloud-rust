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

func TestSampleFromProtobuf(t *testing.T) {
	outDir := path.Join(testdataDir, "sample/golden/type")
	svcConfig := path.Join(testdataDir, "googleapis/google/type/type.yaml")
	specificationSource := path.Join(testdataDir, "googleapis/google/type")
	googleapisRoot := path.Join(testdataDir, "googleapis")

	cmdLine := &CommandLine{
		Command:             []string{},
		ProjectRoot:         projectRoot,
		SpecificationFormat: "protobuf",
		SpecificationSource: specificationSource,
		Source: map[string]string{
			"googleapis-root": googleapisRoot,
		},
		ServiceConfig: svcConfig,
		Language:      "sample",
		Output:        outDir,
		Codec: map[string]string{
			"copyright-year":      "2025",
			"not-for-publication": "true",
		},
	}
	cmdGenerate, _, _ := cmdSidekick.lookup([]string{"generate"})
	if err := runCommand(cmdGenerate, cmdLine); err != nil {
		t.Fatal(err)
	}

	for _, expected := range []string{".sidekick.toml", "README.md"} {
		filename := path.Join(projectRoot, outDir, expected)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Errorf("missing %s: %s", filename, err)
		}
	}
}
