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
	"io/ioutil"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	outDir := flag.String("out", "docfx", "Output directory (default docfx)")
	projectRoot := flag.String("project-root", "", "Top level directory of googleapis/google-cloud-rust")
	flag.Parse()

	fmt.Printf("flag out=%s!\n", *outDir)
	fmt.Printf("flag project-root=%s!\n", *projectRoot)

	// ctx := context.Background()
	crates := flag.Args()

	// TODO: Preflight checks for:
	// cargo workspaces
	// cargo rustdoc

	// Create a temporary files to store `cargo workspace plan`'s output
	tempFile, err := os.CreateTemp("", "cargo-plan-")
	if err != nil {
		slog.Error("Unable to create temp file for cargo workspace plan")
	}
	defer func() {
		rerr := os.Remove(tempFile.Name())
		if err == nil {
			err = rerr
		}
	}()
	fmt.Printf("Created tmp file %s for cargo workspace plan\n", tempFile.Name())

	runCmd(tempFile, *projectRoot, "cargo", "workspaces", "plan", "--json")
	fmt.Printf("using cargo workspace plan for crates\n")

	// TODO(NOW): probably best to move this into the getWorkspaceCrates
	jsonFile, err := os.Open(tempFile.Name())
	if err != nil {
		// TODO: Exit early.
		fmt.Println(err)
	}
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	workspaceCrates, err := getWorkspaceCrates(byteValue)

	for i := 0; i < len(workspaceCrates); i++ {
		// TODO(NOW): Run cargo rustdoc
		if workspaceCrates[i].Name == crates[0] {
			fmt.Printf("crate.Name match: %s\n", workspaceCrates[i].Name)
			fmt.Printf("crate.Location: %s\n", workspaceCrates[i].Location)
			runCmd(nil, *projectRoot, "cargo", "+nightly", "-Z", "unstable-options", "rustdoc", "--output-format=json", fmt.Sprintf("--manifest-path=%s/Cargo.toml", workspaceCrates[i].Location))
			// TODO(NOW): This seem error prone wiht the directory being set by the flag
			// TODO(NOW): crate names are kebob case and output file is snake case
			// workspaceCrates[i].Rustdoc = fmt.Sprintf("%starget/doc/%s.json", *projectRoot, workspaceCrates[i].Name)
			fileName := fmt.Sprintf("%s.json", strings.ReplaceAll(workspaceCrates[i].Name, "-", "_"))
			workspaceCrates[i].Rustdoc = filepath.Join(*projectRoot, "/target/doc", fileName)
			fmt.Printf("crate.Rustdoc: %s\n", workspaceCrates[i].Rustdoc)
			// /usr/local/google/home/chuongph/Desktop/google-cloud-rust/target/doc/google_cloud_secretmanager_v1.json
			// cargo +nightly -Z unstable-options rustdoc --output-format json --manifest-path ./src/generated/cloud/secretmanager/v1/Cargo.toml
			unmarshalRustdoc(&workspaceCrates[i])
			fmt.Printf("crate.Root: %d\n", workspaceCrates[i].Root)
			fmt.Printf("crate.Index length: %d\n", len(workspaceCrates[i].Index))
			crate := workspaceCrates[i]
			rootIndex := strconv.FormatUint(uint64(crate.Root), 10)
			fmt.Printf("crate.Id: %d\n", crate.Index[rootIndex].Id)
			fmt.Printf("crate.Docs: %s\n", crate.Index[rootIndex].Docs)
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
