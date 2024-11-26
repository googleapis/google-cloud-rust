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
	"strings"
)

// Represents the arguments received from the command line.
type CommandLine struct {
	Command             string
	ProjectRoot         string
	SpecificationFormat string
	SpecificationSource string
	ServiceConfig       string
	Source              map[string]string
	Output              string
	TemplateDir         string
	Language            string
	Codec               map[string]string
	DryRun              bool
}

func ParseArgs() (*CommandLine, error) {
	return ParseArgsExplicit(os.Args[1:])
}

func ParseArgsExplicit(args []string) (*CommandLine, error) {
	fs := flag.NewFlagSet("sidekick", flag.ContinueOnError)
	var (
		projectRoot   = fs.String("project-root", "", "the root of the output project")
		format        = fs.String("specification-format", "", "the specification format. Protobuf or OpenAPI v3.")
		source        = fs.String("specification-source", "", "the path to the input data")
		serviceConfig = fs.String("service-config", "", "path to service config")
		sourceOpts    = map[string]string{}
		output        = fs.String("output", "", "the path within project-root to put generated files")
		templateDir   = fs.String("template-dir", "", "the path to the template directory")
		language      = fs.String("language", "", "the generated language")
		codecOpts     = map[string]string{}
		dryrun        = fs.Bool("dry-run", false, "do a dry-run: load the configuration, but do not perform any changes.")
	)

	fs.Func("source-option", "source options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid source option, must be in key=value format (%s)", opt)
		}
		sourceOpts[components[0]] = components[1]
		return nil
	})
	fs.Func("codec-option", "codec options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid codec option, must be in key=value format (%s)", opt)
		}
		codecOpts[components[0]] = components[1]
		return nil
	})
	fs.Usage = func() {
		fmt.Println("Usage: sidekick [options] <command (generate|refresh|refreshall)>")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	args = fs.Args()
	var command string
	switch len(args) {
	case 0:
		return nil, fmt.Errorf("missing command")
	case 1:
		command = args[0]
	default:
		return nil, fmt.Errorf("unrecognized arguments %v", args)
	}

	if command == "help" {
		fs.Usage()
		os.Exit(0)
	}

	return &CommandLine{
		Command:             command,
		ProjectRoot:         *projectRoot,
		SpecificationFormat: *format,
		SpecificationSource: *source,
		ServiceConfig:       *serviceConfig,
		Source:              sourceOpts,
		Language:            *language,
		Output:              *output,
		TemplateDir:         *templateDir,
		Codec:               codecOpts,
		DryRun:              *dryrun,
	}, nil
}
