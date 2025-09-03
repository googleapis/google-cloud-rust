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

package main

import (
	"errors"
	"fmt"
	"os/exec"
)

// preFlightTests() verifies all the required commands are available.
func preFlightTests(upload bool) error {

	// TODO: Preflight checks for:
	// cargo workspaces
	// cargo rustdoc
	// docuploader
	// Failfast if not installed.
	if err := testExternalCommand("cargo", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `cargo --version`, the instructions on https://www.rust-lang.org/learn/get-started may solve this problem: %w", err)
	}
	if err := testExternalCommand("cargo", "+nightly", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `cargo +nightly --version`, run `rustup install nightly` to solve this problem: %w", err)
	}
	if err := testExternalCommand("cargo", "+nightly", "rustdoc", "--help"); err != nil {
		return fmt.Errorf("got an error trying to run `cargo +nightly rustdoc --help`, maybe running `rustup update nightly` will solve this problem: %w", err)
	}
	if err := testExternalCommand("cargo", "workspaces", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `cargo workspaces --version`, run `cargo install --locked cargo-workspaces` to solve this problem: %w", err)
	}
	if !upload {
		return nil
	}
	if err := testExternalCommand("docuploader", "--version"); err != nil {
		return fmt.Errorf("got an error trying to run `docuploader --version`. Consider using a Python virtual environment and run `pip install gcp-docuploader` to solve this problem: %w", err)
	}
	return nil
}

func testExternalCommand(c string, arg ...string) error {
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
