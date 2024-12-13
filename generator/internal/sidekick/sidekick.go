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

// Package sidekick provides functionality for automating code generation.
package sidekick

import (
	"fmt"
	"maps"
	"os"
)

func Run() error {
	cmdLine, err := parseArgs()
	if err != nil {
		return err
	}
	return runSidekick(cmdLine)
}

func runSidekick(cmdLine *CommandLine) error {
	if cmdLine.ProjectRoot != "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		defer os.Chdir(cwd)
		if err := os.Chdir(cmdLine.ProjectRoot); err != nil {
			return err
		}
	}
	// Load the top-level configuration file. If there are any errors loading
	// the file just run with the defaults.
	rootConfig, err := loadRootConfig(".sidekick.toml")
	if err != nil {
		return err
	}
	argsConfig := &Config{
		General: GeneralConfig{
			Language: cmdLine.Language,
		},
		Source: maps.Clone(cmdLine.Source),
		Codec:  maps.Clone(cmdLine.Codec),
	}
	config, err := mergeConfigs(rootConfig, argsConfig)
	if err != nil {
		return err
	}

	switch cmdLine.Command {
	case "generate":
		return generate(config, cmdLine)
	case "refresh":
		return refresh(config, cmdLine, cmdLine.Output)
	case "refresh-all", "refreshall":
		return refreshAll(config, cmdLine)
	case "update":
		return update(config, cmdLine)
	default:
		return fmt.Errorf("unknown subcommand %s", cmdLine.Command)
	}
}
