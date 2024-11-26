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
	"path"
	"strings"
	"time"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	toml "github.com/pelletier/go-toml/v2"
)

// Generate takes some state and applies it to a template to create a client
// library.
func Generate(rootConfig *Config, args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	var (
		format        = fs.String("specification-format", "", "the specification format. Protobuf or OpenAPI v3.")
		source        = fs.String("specification-source", "", "the path to the input data")
		serviceConfig = fs.String("service-config", "", "path to service config")
		parserOpts    = map[string]string{}
		language      = fs.String("language", "", "the generated language")
		output        = fs.String("output", "generated", "the path within project-root to put generated files")
		templateDir   = fs.String("template-dir", "templates/", "the path to the template directory")
		codecOpts     = map[string]string{}
	)

	fs.Func("parser-option", "parser options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid parser option, must be in key=value format (%s)", opt)
		}
		parserOpts[components[0]] = components[1]
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
	fs.Parse(args)

	config := Config{
		General: GeneralConfig{
			SpecificationFormat: *format,
			SpecificationSource: *source,
			ServiceConfig:       *serviceConfig,
			Language:            *language,
			TemplateDir:         *templateDir,
		},
	}
	if len(parserOpts) != 0 {
		config.Source = parserOpts
	}
	if len(codecOpts) != 0 {
		config.Codec = codecOpts
	}
	if _, ok := config.Codec["copyright-year"]; !ok {
		generation_year, _, _ := time.Now().Date()
		if config.Codec == nil {
			config.Codec = map[string]string{}
		}
		config.Codec["copyright-year"] = fmt.Sprintf("%04d", generation_year)
	}
	if err := writeSidekickToml(*output, config); err != nil {
		return err
	}

	// Load the .sidekick.toml file and refresh the code.
	return Refresh(rootConfig, []string{*output})
}

func writeSidekickToml(outDir string, config Config) error {
	if err := os.MkdirAll(outDir, 0777); err != nil {
		return err
	}
	f, err := os.Create(path.Join(outDir, ".sidekick.toml"))
	if err != nil {
		return err
	}
	defer f.Close()

	year := config.Codec["copyright-year"]
	for _, line := range genclient.LicenseHeader(year) {
		if line == "" {
			fmt.Fprintln(f, "#")
		} else {
			fmt.Fprintf(f, "# %s\n", line)
		}
	}
	fmt.Fprintln(f, "")

	t := toml.NewEncoder(f)
	if err := t.Encode(config); err != nil {
		return err
	}
	return f.Close()
}
