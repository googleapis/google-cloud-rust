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
	"path"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser"
)

// Reruns the generator in one directory, using the configuration parameters
// saved in its `.sidekick.toml` file.
func Refresh(rootConfig *Config, args []string) error {
	fs := flag.NewFlagSet("refresh", flag.ExitOnError)
	var (
		dryrun = fs.Bool("dry-run", false, "do a dry-run: load the configuration, but do not perform any changes.")
	)
	fs.Parse(args)
	args = fs.Args()
	if len(args) != 1 {
		return fmt.Errorf("expected the target directory")
	}
	outDir := args[0]
	config, err := MergeConfig(rootConfig, path.Join(outDir, ".sidekick.toml"))
	if err != nil {
		return err
	}
	if config.General.SpecificationFormat == "" {
		return fmt.Errorf("must provide general.specification-format")
	}
	if config.General.SpecificationSource == "" {
		return fmt.Errorf("must provide general.specification-source")
	}

	specFormat := config.General.SpecificationFormat
	popts := &genclient.ParserOptions{
		Source:        config.General.SpecificationSource,
		ServiceConfig: config.General.ServiceConfig,
		Options:       config.Source,
	}

	copts := &genclient.CodecOptions{
		Language:    config.General.Language,
		OutDir:      outDir,
		TemplateDir: config.General.TemplateDir,
		Options:     config.Codec,
	}

	parser, err := parser.NewParser(specFormat)
	if err != nil {
		return err
	}

	api, err := parser.Parse(*popts)
	if err != nil {
		return err
	}

	codec, err := language.NewCodec(copts)
	if err != nil {
		return err
	}
	if err = codec.Validate(api); err != nil {
		return err
	}
	request := &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      copts.OutDir,
		TemplateDir: copts.TemplateDir,
	}
	if *dryrun {
		return nil
	}
	return genclient.Generate(request)
}
