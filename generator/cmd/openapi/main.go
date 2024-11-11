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
	"log"
	"log/slog"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/translator/openapi"
)

var (
	language      = flag.String("language", "", "the generated language")
	output        = flag.String("out", "output", "the path to the output directory")
	templateDir   = flag.String("template-dir", "templates/", "the path to the template directory")
	serviceConfig = flag.String("service-config", "testdata/google/cloud/secretmanager/v1/secretmanager_v1.yaml", "path to service config")
	inputPath     = flag.String("input-path", "", "the path to a file with an OpenAPI v3 JSON object")
)

func main() {
	flag.Parse()

	if *inputPath == "" {
		log.Fatalf("must provide input-path")
	}
	opts := &openapi.Options{
		Language:      *language,
		OutDir:        *output,
		TemplateDir:   *templateDir,
		ServiceConfig: *serviceConfig,
	}
	if err := run(*inputPath, opts); err != nil {
		log.Fatal(err)
	}
	slog.Info("Generation Completed Successfully")
}

func run(inputPath string, opts *openapi.Options) error {
	req, err := openapi.Translate(inputPath, opts)
	if err != nil {
		return err
	}
	if _, err := genclient.Generate(req); err != nil {
		return err
	}
	return nil
}
