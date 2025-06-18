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
	"log/slog"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	toml "github.com/pelletier/go-toml/v2"
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
		rustGenerate,
	)
}

// generate takes some state and applies it to a template to create a client
// library.
func rustGenerate(rootConfig *config.Config, cmdLine *CommandLine) error {
	if cmdLine.SpecificationSource == "" {
		cmdLine.SpecificationSource = path.Dir(cmdLine.ServiceConfig)
	}
	if cmdLine.Output == "" {
		cmdLine.Output = path.Join("src/generated", strings.TrimPrefix(cmdLine.SpecificationSource, "google/"))
	}

	if err := runExternalCommand("cargo", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `cargo --version`, the instructions on https://www.rust-lang.org/learn/get-started may solve this problem: %w", err)
	}
	if err := runExternalCommand("taplo", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `taplo --version`, please install using `cargo install taplo-cli`: %w", err)
	}
	if err := runExternalCommand("typos", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `typos --version`, please install using `cargo install typos-cli`: %w", err)
	}
	if err := runExternalCommand("git", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `git --version`, the instructions on https://github.com/git-guides/install-git may solve this problem: %w", err)
	}

	slog.Info("Preparing cargo workspace to get new package")
	if err := runExternalCommand("cargo", "new", "--vcs", "none", "--lib", cmdLine.Output); err != nil {
		return err
	}
	if err := runExternalCommand("taplo", "fmt", "Cargo.toml"); err != nil {
		return err
	}
	slog.Info("Generating new library code and adding it to git")
	if err := generate(rootConfig, cmdLine); err != nil {
		return err
	}
	if err := runExternalCommand("cargo", "fmt"); err != nil {
		return err
	}
	if err := runExternalCommand("git", "add", cmdLine.Output); err != nil {
		return err
	}
	packagez, err := getPackageName(cmdLine.Output)
	if err != nil {
		return err
	}
	slog.Info("Generated new client library", "package", packagez)
	slog.Info("Running `cargo test` on new client library")
	if err := runExternalCommand("cargo", "test", "--package", packagez); err != nil {
		return err
	}
	slog.Info("Running `cargo doc` on new client library")
	if err := runExternalCommand("env", "RUSTDOCFLAGS=-D warnings", "cargo", "doc", "--package", packagez, "--no-deps"); err != nil {
		return err
	}
	slog.Info("Running `cargo clippy` on new client library")
	if err := runExternalCommand("cargo", "clippy", "--package", packagez, "--", "--deny", "warnings"); err != nil {
		return err
	}
	slog.Info("Running `typos` on new client library")
	if err := runExternalCommand("typos"); err != nil {
		slog.Info("please manually add the typos to `.typos.toml` and fix the problem upstream")
		return err
	}
	if err := runExternalCommand("git", "add", "Cargo.lock", "Cargo.toml"); err != nil {
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

func getPackageName(output string) (string, error) {
	cargo := CargoConfig{}
	filename := path.Join(output, "Cargo.toml")
	if contents, err := os.ReadFile(filename); err == nil {
		err = toml.Unmarshal(contents, &cargo)
		if err != nil {
			return "", fmt.Errorf("error reading %s: %w", filename, err)
		}
	}
	// Ignore errors reading the top-level file.
	return cargo.Package.Name, nil
}

type CargoConfig struct {
	Package CargoPackage // `toml:"package"`
}

type CargoPackage struct {
	Name string // `toml:"name"`
}
