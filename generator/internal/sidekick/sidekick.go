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
	"os"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

var cmdSidekick = newCommand(
	"sidekick",
	"sidekick is a tool for automating SDK generation.",
	``,
	nil, // nil parent is only allowed for the root command
	nil).
	addFlagString(&flagProjectRoot, "project-root", "the root of the output project").
	addFlagString(&format, "specification-format", "the specification format. Protobuf or OpenAPI v3.").
	addFlagString(&source, "specification-source", "the path to the input data").
	addFlagString(&serviceConfig, "service-config", "path to service config").
	addFlagString(&output, "output", "the path within project-root to put generated files").
	addFlagString(&flagLanguage, "language", "the generated language").
	addFlagBool(&dryrun, "dry-run", false, "do a dry-run: load the configuration, but do not perform any changes.").
	addFlagFunc("source-option", "source options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid source option, must be in key=value format (%s)", opt)
		}
		sourceOpts[components[0]] = components[1]
		return nil
	}).
	addFlagFunc("codec-option", "codec options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid codec option, must be in key=value format (%s)", opt)
		}
		codecOpts[components[0]] = components[1]
		return nil
	})

// Run is the entry point for the sidekick logic. It expects args to be the command line arguments, minus the program name.
func Run(args []string) error {
	if len(args) < 1 {
		cmdSidekick.printUsage()
		return fmt.Errorf("no command given")
	}
	if args[0] == "help" {
		cmd, found, unusedArgs := cmdSidekick.lookup(args[1:])
		if !found {
			return newNotFoundError(
				cmd,
				args[1:],
				unusedArgs,
				fmt.Sprintf(
					"Could not find help documentation for 'sidekick help %s'",
					strings.Join(args[1:], " ")))
		}
		cmd.printUsage()
		return nil
	}
	cmd, found, cmdArgs := cmdSidekick.lookup(args)
	if !found {
		return newNotFoundError(
			cmd,
			args,
			cmdArgs,
			fmt.Sprintf(
				"Could not find command 'sidekick %s'",
				strings.Join(args, " ")))
	}
	cmdLine, err := cmd.parseCmdLine(cmdArgs)
	if err != nil {
		return err
	}
	return runCommand(cmd, cmdLine)
}

func newNotFoundError(bestMatch *command, allArgs []string, unusedArgs []string, msg string) error {
	validHelp := "sidekick help"
	if bestMatch != cmdSidekick {
		validHelp += " " + strings.Join(allArgs[0:len(allArgs)-len(unusedArgs)], " ")
	}
	return fmt.Errorf(
		"%s. For help, run '%s'",
		msg,
		validHelp)
}

func runCommand(cmd *command, cmdLine *CommandLine) error {
	if cmdLine.ProjectRoot != "" {
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("could not get current working directory: %w", err)
		}
		defer func(dir string) {
			_ = os.Chdir(dir)
		}(cwd)
		if err = os.Chdir(cmdLine.ProjectRoot); err != nil {
			return fmt.Errorf("could not change to project root [%s]: %w", cmdLine.ProjectRoot, err)
		}
	}
	config, err := config.LoadConfig(cmdLine.Language, cmdLine.Source, cmdLine.Codec)
	if err != nil {
		return fmt.Errorf("could not load configuration: %w", err)
	}

	return cmd.run(config, cmdLine)
}
