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

// crateDenyList enumerates the crates which do not get documents at cloud.google.com
var crateDenyList = []string{"google-cloud-gax-internal"}

func main() {
	out := flag.String("out", "docfx", "Output directory within project-root (default docfx)")
	projectRoot := flag.String("project-root", "", "Top level directory of googleapis/google-cloud-rust")
	upload := flag.String("staging-bucket", "", "Upload the generated docfx to the gcs bucket using docuploader")
	flag.Parse()

	crates := flag.Args()

	if err := preFlightTests(*upload); err != nil {
		log.Fatal(err)
	}

	// Create a temporary file to store `cargo workspace plan` output.
	tempFile, err := os.CreateTemp("", "cargo-plan-")
	if err != nil {
		log.Fatalf("Unable to create temp file for cargo workspace plan: %v\n", err)
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()
	fmt.Printf("Created tmp file %s for cargo workspace plan\n", tempFile.Name())

	if err := runCmd(tempFile, *projectRoot, "cargo", "workspaces", "plan", "--json"); err != nil {
		log.Fatalf("Unable to get package list: %v", err)
	}
	fmt.Printf("using cargo workspace plan for crates\n")

	byteValue, err := os.ReadFile(tempFile.Name())
	if err != nil {
		log.Fatalf("Unable to read cargo workspace json file: %v\n", err)
	}

	workspaceCrates, err := getWorkspaceCrates(byteValue)
	if err != nil {
		log.Fatalf("Error getting workspace crates: %v\n", err)
	}

	if err := renderIndex(workspaceCrates, filepath.Join(*projectRoot, *out)); err != nil {
		log.Fatal(err)
	}

	for i, crate := range workspaceCrates {
		// TODO: Allow for regex on crate names instead.
		if !slices.Contains(crateDenyList, crate.Name) && (len(crates) == 0 || slices.Contains(crates, crate.Name)) {
			if err := runCmd(nil, *projectRoot, "cargo", "+nightly", "-Z", "unstable-options", "rustdoc", "--output-format=json", "--package", crate.Name); err != nil {
				fmt.Printf("Error in cargo rustdoc command: %v", err)
				continue
			}
			// cargo names are snake case while cargo rustdoc output files are kebab case.
			fileName := fmt.Sprintf("%s.json", strings.ReplaceAll(workspaceCrates[i].Name, "-", "_"))
			file := filepath.Join(*projectRoot, "/target/doc", fileName)
			jsonBytes, err := os.ReadFile(file)
			if err != nil {
				log.Fatalf("Error reading rustdoc file: %v\n", err)
			}
			unmarshalRustdoc(&crate, jsonBytes)

			crateOutDir := filepath.Join(*projectRoot, *out, crate.Name)
			_ = os.MkdirAll(crateOutDir, 0777) // Ignore errors

			err = generate(&crate, crateOutDir)
			if err != nil {
				log.Fatalf("failed to generate for crate %s: %v\n", crate.Name, err)
			}
			fmt.Printf("Generated docfx for crate: %s\n", crate.Name)

			if *upload != "" {
				fmt.Printf("Uploading crate: %s\n", crate.Name)
				if err := runCmd(nil, "", "docuploader", "upload", fmt.Sprintf("--staging-bucket=%s", *upload), "--destination-prefix=docfx", fmt.Sprintf("--metadata-file=%s/docs.metadata", crateOutDir), crateOutDir); err != nil {
					fmt.Printf("error uploading files: %v\n", err)
				}
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
