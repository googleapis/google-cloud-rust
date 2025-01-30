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
	"os/exec"
	"path"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func init() {
	newCommand(
		"sidekick rust-generate",
		"Runs the generator for the first time for a client library assuming the target is the Rust monorepo.",
		`
Runs the generator for the first time for a client library.

Uses the configuration provided in the command line arguments, and saving it in
a .sidekick.toml file in the output directory.

Uses the conventions in the Rust monorepo to determine the source and output
directories from the name of the service config YAML file.
`,
		cmdSidekick,
		rust_generate,
	)
}

// generate takes some state and applies it to a template to create a client
// library.
func rust_generate(rootConfig *config.Config, cmdLine *CommandLine) error {
	if cmdLine.SpecificationSource == "" {
		cmdLine.SpecificationSource = path.Dir(cmdLine.ServiceConfig)
	}
	if cmdLine.Output == "" {
		cmdLine.Output = path.Join("src/generated", strings.TrimPrefix(cmdLine.SpecificationSource, "google/"))
	}

	if err := runExternalCommand("cargo", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `cargo --version`, please verify it is installed: %w", err)
	}
	if err := runExternalCommand("taplo", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `taplo --version`, please verify it is installed: %w", err)
	}
	if err := runExternalCommand("git", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `git --version`, please verify it is installed: %w", err)
	}

	if err := runExternalCommand("cargo", "new", "--vcs", "none", "--lib", cmdLine.Output); err != nil {
		return err
	}
	if err := runExternalCommand("taplo", "fmt", "Cargo.toml"); err != nil {
		return err
	}
	if err := generate(rootConfig, cmdLine); err != nil {
		return err
	}
	if err := runExternalCommand("cargo", "fmt"); err != nil {
		return err
	}
	if err := runExternalCommand("git", "add", cmdLine.Output); err != nil {
		return err
	}

	return nil
}

func runExternalCommand(c string, arg ...string) error {
	cmd := exec.Command(c, arg...)
	cmd.Dir = "."
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return fmt.Errorf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		return fmt.Errorf("%v: %v\n%s", cmd, err, output)
	}
	return nil
}
