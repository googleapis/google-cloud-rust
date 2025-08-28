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
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// Temporarily disable generation for certain crates.
var crateDenyList = []string{"gcp-sdk", "google-cloud-base"}

func main() {
	out := flag.String("out", "docfx", "Output directory within project-root (default docfx)")
	projectRoot := flag.String("project-root", "", "Top level directory of googleapis/google-cloud-rust")
	upload := flag.Bool("upload", false, "Upload generated docfx using docuploader")
	flag.Parse()

	crates := flag.Args()

	// TODO: Preflight checks for:
	// cargo workspaces
	// cargo rustdoc
	// docuploader
	// Failfast if not installed.

	// Create a temporary file to store `cargo workspace plan` output.
	tempFile, err := os.CreateTemp("", "cargo-plan-")
	if err != nil {
		log.Fatalf("Unable to create temp file for cargo workspace plan: %w\n", err)
	}
	defer os.Remove(tempFile.Name())
	fmt.Printf("Created tmp file %s for cargo workspace plan\n", tempFile.Name())

	runCmd(tempFile, *projectRoot, "cargo", "workspaces", "plan", "--json")
	fmt.Printf("using cargo workspace plan for crates\n")

	jsonFile, err := os.Open(tempFile.Name())
	if err != nil {
		log.Fatalf("Unable to open temp file for cargo workspace plan: %w\n", err)
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("Unable to read cargo workspace json file: %w\n", err)
	}

	workspaceCrates, err := getWorkspaceCrates(byteValue)
	if err != nil {
		log.Fatalf("Error getting workspace crates: %w\n", err)
	}

	for i := 0; i < len(workspaceCrates); i++ {
		// TODO: Allow for regex on crate names instead.
		if !slices.Contains(crateDenyList, workspaceCrates[i].Name) && (len(crates) == 0 || slices.Contains(crates, workspaceCrates[i].Name)) {
			crate := workspaceCrates[i]

			runCmd(nil, *projectRoot, "cargo", "+nightly", "-Z", "unstable-options", "rustdoc", "--output-format=json", fmt.Sprintf("--manifest-path=%s/Cargo.toml", workspaceCrates[i].Location))
			// cargo names are snake case while cargo rustdoc output files are kebob case.
			fileName := fmt.Sprintf("%s.json", strings.ReplaceAll(workspaceCrates[i].Name, "-", "_"))
			file := filepath.Join(*projectRoot, "/target/doc", fileName)
			rustDocFile, err := os.Open(file)
			if err != nil {
				log.Fatalf("Error opening rustdoc file: %w\n", err)
			}
			defer rustDocFile.Close()
			jsonBytes, err := io.ReadAll(rustDocFile)
			if err != nil {
				log.Fatalf("Error reading rustdoc file: %w\n", err)
			}
			unmarshalRustdoc(&crate, jsonBytes)

			crateOutDir := filepath.Join(*projectRoot, *out, crate.Name)
			os.MkdirAll(crateOutDir, 0777) // Ignore errors

			err = generate(&crate, *projectRoot, crateOutDir)
			if err != nil {
				log.Fatalf("failed to generate for crate %s: %w", crate.Name, err)
			}

			if *upload {
				fmt.Printf("Uploading crate:%s\n", crate.Name)
				// TODO: Add a flag to specify bucket location.
				runCmd(nil, "", "docuploader", "upload", "--staging-bucket=docs-staging-v2-dev", fmt.Sprintf("--metadata-file=%s/docs.metadata", crateOutDir), crateOutDir)
			}
		}
	}
}

func runCmd(stdout io.Writer, dir, name string, args ...string) error {
	fmt.Printf("Running command: dir=%s, name=%s, args=%s\n", dir, name, strings.Join(args, " "))

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cmd.Start: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("cmd.Wait: %w", err)
	}
	return nil
}
