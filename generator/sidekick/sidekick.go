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

package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if err := root(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func root() error {
	var projectRoot = flag.String("project-root", "", "the root of the output project")
	flag.Parse()

	if *projectRoot != "" {
		if err := os.Chdir(*projectRoot); err != nil {
			return err
		}
	}

	// Load the top-level configuration file. If there are any errors loading
	// the file just run with the defaults.
	config, err := LoadRootConfig(".sidekick.toml")
	if err != nil {
		return err
	}

	args := flag.Args()
	if len(args) < 1 {
		return fmt.Errorf("you must provide a subcommand, either `generate`, `refresh`, or `refreshall`")
	}
	switch args[0] {
	case "generate":
		if err := Generate(config, args[1:]); err != nil {
			return err
		}
	case "refresh":
		if err := Refresh(config, args[1:]); err != nil {
			return err
		}
	case "refresh-all", "refreshall":
		if err := RefreshAll(config, args[1:]); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown subcommand %s", os.Args[1])
	}
	return nil
}
