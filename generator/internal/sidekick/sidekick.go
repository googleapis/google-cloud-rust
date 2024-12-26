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
)

var CmdSidekick = newCommand(
	"sidekick",
	"sidekick is a tool for automating code generation.",
	nil, //nil parent can only be used with the private newCommand function.
).
	AddFlagString(&flagProjectRoot, "project-root", "", "the root of the output project").
	AddFlagString(&format, "specification-format", "", "the specification format. Protobuf or OpenAPI v3.").
	AddFlagString(&source, "specification-source", "", "the path to the input data").
	AddFlagString(&serviceConfig, "service-config", "", "path to service config").
	AddFlagString(&output, "output", "", "the path within project-root to put generated files").
	AddFlagString(&flagLanguage, "language", "", "the generated language").
	AddFlagBool(&dryrun, "dry-run", false, "do a dry-run: load the configuration, but do not perform any changes.").
	AddFlagFunc("source-option", "source options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid source option, must be in key=value format (%s)", opt)
		}
		sourceOpts[components[0]] = components[1]
		return nil
	}).
	AddFlagFunc("codec-option", "codec options", func(opt string) error {
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
		_ = CmdSidekick.PrintUsage()
		return fmt.Errorf("no command given")
	}
	if args[0] == "help" {
		cmd, found, unusedArgs := CmdSidekick.Lookup(args[1:])
		if !found {
			return NotFoundError(cmd, args[1:], unusedArgs, fmt.Sprintf("Could not find help documentation for 'sidekick help %s'", strings.Join(args[1:], " ")))
		}
		return cmd.PrintUsage()
	} else {
		cmd, found, cmdArgs := CmdSidekick.Lookup(args)
		if !found {
			return NotFoundError(cmd, args, cmdArgs, fmt.Sprintf("Could not find command 'sidekick %s'", strings.Join(args, " ")))
		} else {
			var err error
			if cmdLine, err := cmd.ParseCmdLine(cmdArgs); err == nil {
				return runCommand(cmd, cmdLine)
			}
			return err
		}
	}
}

func NotFoundError(bestMatch *Command, allArgs []string, unusedArgs []string, msg string) error {
	validHelp := "sidekick help"
	if bestMatch != CmdSidekick {
		validHelp += " " + strings.Join(allArgs[0:len(allArgs)-len(unusedArgs)], " ")
	}
	return fmt.Errorf(
		"%s. For help, run '%s'",
		msg,
		validHelp)
}

func runCommand(cmd *Command, cmdLine *CommandLine) error {
	var err error
	if cmdLine.ProjectRoot != "" {
		if cwd, err := os.Getwd(); err == nil {
			defer func(dir string) {
				_ = os.Chdir(dir)
			}(cwd)
		}
		err = os.Chdir(cmdLine.ProjectRoot)
	}
	if config, err := loadConfig(cmdLine); err == nil {
		return cmd.Run(config, cmdLine)
	}
	return err
}
