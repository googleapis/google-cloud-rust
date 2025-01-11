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
	"fmt"
	"maps"
	"os"
	"path"
	"time"

	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	toml "github.com/pelletier/go-toml/v2"
)

func init() {
	newCommand(
		"sidekick generate",
		"Runs the generator for the first time for a client library.",
		`
Runs the generator for the first time for a client library.

Uses the configuration provided in the command line arguments, and saves it in a .sidekick.toml file in the output directory.
`,
		cmdSidekick,
		generate,
	)
}

// generate takes some state and applies it to a template to create a client
// library.
func generate(rootConfig *Config, cmdLine *CommandLine) error {
	local := Config{
		General: GeneralConfig{
			Language:            cmdLine.Language,
			SpecificationFormat: cmdLine.SpecificationFormat,
			SpecificationSource: cmdLine.SpecificationSource,
			ServiceConfig:       cmdLine.ServiceConfig,
		},
		Source: maps.Clone(cmdLine.Source),
		Codec:  maps.Clone(cmdLine.Codec),
	}
	if _, ok := local.Codec["copyright-year"]; !ok {
		generation_year, _, _ := time.Now().Date()
		local.Codec["copyright-year"] = fmt.Sprintf("%04d", generation_year)
	}

	if err := writeSidekickToml(cmdLine.Output, &local); err != nil {
		return err
	}

	override, err := overrideSources(rootConfig)
	if err != nil {
		return err
	}

	// Load the .sidekick.toml file and refresh the code.
	return refresh(override, cmdLine)
}

func writeSidekickToml(outDir string, config *Config) error {
	if err := os.MkdirAll(outDir, 0777); err != nil {
		return err
	}
	f, err := os.Create(path.Join(outDir, ".sidekick.toml"))
	if err != nil {
		return err
	}
	defer f.Close()

	year := config.Codec["copyright-year"]
	for _, line := range license.LicenseHeader(year) {
		if line == "" {
			fmt.Fprintln(f, "#")
		} else {
			fmt.Fprintf(f, "#%s\n", line)
		}
	}
	fmt.Fprintln(f, "")

	t := toml.NewEncoder(f)
	if err := t.Encode(config); err != nil {
		return err
	}
	return f.Close()
}
