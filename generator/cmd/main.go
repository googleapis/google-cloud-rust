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
	"log"
	"path"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser"
)

func main() {
	var (
		format        = flag.String("specification-format", "", "the specification format. Protobuf or OpenAPI v3.")
		source        = flag.String("specification-source", "", "the path to the input data")
		serviceConfig = flag.String("service-config", "", "path to service config")
		parserOpts    = map[string]string{}
		language      = flag.String("language", "", "the generated language")
		projectRoot   = flag.String("project-root", "", "the root of the output project")
		output        = flag.String("output", "generated", "the path within project-root to put generated files")
		templateDir   = flag.String("template-dir", "templates/", "the path to the template directory")
		codecOpts     = map[string]string{}
	)

	flag.Func("parser-option", "parser options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid parser option, must be in key=value format (%s)", opt)
		}
		parserOpts[components[0]] = components[1]
		return nil
	})
	flag.Func("codec-option", "codec options", func(opt string) error {
		components := strings.SplitN(opt, "=", 2)
		if len(components) != 2 {
			return fmt.Errorf("invalid codec option, must be in key=value format (%s)", opt)
		}
		parserOpts[components[0]] = components[1]
		return nil
	})
	flag.Parse()

	if *format == "" {
		log.Fatalf("must provide specification-format")
	}
	if *source == "" {
		log.Fatalf("must provide source")
	}

	popts := genclient.ParserOptions{
		Source:        *source,
		ServiceConfig: *serviceConfig,
		Options:       parserOpts,
	}

	copts := genclient.CodecOptions{
		Language:    *language,
		ProjectRoot: *projectRoot,
		OutDir:      *output,
		TemplateDir: *templateDir,
		Options:     codecOpts,
	}
	err := Generate(*format, &popts, &copts)
	if err != nil {
		log.Fatal(err)
	}
}

// Generate takes some state and applies it to a template to create a client
// library.
func Generate(specFormat string, popts *genclient.ParserOptions, copts *genclient.CodecOptions) error {
	parser, err := parser.NewParser(specFormat)
	if err != nil {
		return err
	}

	api, err := parser.Parse(*popts)
	if err != nil {
		return err
	}

	codec, err := language.NewCodec(copts.Language)
	if err != nil {
		return err
	}
	request := &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      path.Join(copts.ProjectRoot, copts.OutDir),
		TemplateDir: copts.TemplateDir,
	}
	_, err = genclient.Generate(request)
	if err != nil {
		return err
	}
	return nil
}
