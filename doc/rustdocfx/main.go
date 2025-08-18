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

/*
Rustdocfx generates for Google Rust SDK reference documentation in DocFx YAML format.
See https://github.com/googleapis/doc-pipeline for more information.

Usage:

	rustdocfx [flags] [crate ...]

The flags are:

	    -out
		Write the result custom/file/path instead of stdout.
	    -project-root
		Top level directory of googleapis/google-cloud-rust.
*/
package main

import (
	//	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	out := flag.String("out", "docfx", "Output directory within project-root (default docfx)")
	projectRoot := flag.String("project-root", "", "Top level directory of googleapis/google-cloud-rust")
	flag.Parse()

	crates := flag.Args()

	// TODO: Preflight checks for:
	// cargo workspaces
	// cargo rustdoc
	// Failfast if not installed.

	// Create a temporary file to store `cargo workspace plan` output.
	tempFile, err := os.CreateTemp("", "cargo-plan-")
	if err != nil {
		fmt.Printf("Unable to create temp file for cargo workspace plan: %v\n", err)
		return
	}
	defer os.Remove(tempFile.Name())
	fmt.Printf("Created tmp file %s for cargo workspace plan\n", tempFile.Name())

	runCmd(tempFile, *projectRoot, "cargo", "workspaces", "plan", "--json")
	fmt.Printf("using cargo workspace plan for crates\n")

	jsonFile, err := os.Open(tempFile.Name())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer jsonFile.Close()

	byteValue, _ := io.ReadAll(jsonFile)
	workspaceCrates, err := getWorkspaceCrates(byteValue)
	if err != nil {
		fmt.Printf("Error getting workspace crates: %v\n", err)
		return
	}

	for i := 0; i < len(workspaceCrates); i++ {
		// TODO: Ignore the "gcp-sdk" crate.
		// TODO(NOW): Filter, right now, we only work on the first arguemnt.
		if workspaceCrates[i].Name == crates[0] {
			runCmd(nil, *projectRoot, "cargo", "+nightly", "-Z", "unstable-options", "rustdoc", "--output-format=json", fmt.Sprintf("--manifest-path=%s/Cargo.toml", workspaceCrates[i].Location))
			// cargo names are snake case while cargo rustdoc output files are kebob case.
			fileName := fmt.Sprintf("%s.json", strings.ReplaceAll(workspaceCrates[i].Name, "-", "_"))
			file := filepath.Join(*projectRoot, "/target/doc", fileName)
			rustDocFile, err := os.Open(file)
			if err != nil {
				// TODO(NOW): Failfast.
				fmt.Println(err)
			}
			defer rustDocFile.Close()
			jsonBytes, _ := io.ReadAll(rustDocFile)
			// TODO(NOW): Handle error.
			unmarshalRustdoc(&workspaceCrates[i], jsonBytes)

			// TODO(NOW): Should we handle the errors?
			crateOutDir := filepath.Join(*projectRoot, *out, workspaceCrates[i].Name)
			os.MkdirAll(crateOutDir, 0777) // Ignore errors

			// TODO(NOW): This is not needed.
			crate := workspaceCrates[i]
			err = generate(&crate, *projectRoot, crateOutDir)
			if err != nil {
				// TODO: Better log message for the failure with crate name.
				log.Fatalf("failed to generate for crate %s: %v", workspaceCrates[i].Name, err)
			}
		}
	}
}

func runCmd(stdout io.Writer, dir, name string, args ...string) error {
	slog.Info("Running command: ", "dir", dir, "name", name, "args", strings.Join(args, " "))

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cmd.Start: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("cmd.Wait: %s", err)
	}
	return nil
}
