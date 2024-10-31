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
	"io"
	"log"
	"log/slog"
	"os"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/translator/openapi"
)

func main() {
	inputPath := flag.String("input-path", "", "the path to a file with an OpenAPI v3 JSON object")
	outDir := flag.String("out-dir", "", "the path to the output directory")
	language := flag.String("language", "", "the generated language")
	templateDir := flag.String("template-dir", "templates/", "the path to the template directory")
	flag.Parse()

	if err := run(*inputPath, *language, *outDir, *templateDir); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	slog.Info("Generation Completed Successfully")
}

func run(inputPath, language, outDir, templateDir string) error {
	var (
		contents []byte
		err      error
	)
	if inputPath == "" {
		contents, err = io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
	} else {
		contents, err = os.ReadFile(inputPath)
		if err != nil {
			return err
		}
	}
	return generateFrom(contents, language, outDir, templateDir)
}

func generateFrom(contents []byte, language, outDir, templateDir string) error {
	translator, err := openapi.NewTranslator(contents, &openapi.Options{
		Language:    language,
		OutDir:      outDir,
		TemplateDir: templateDir,
	})
	if err != nil {
		return err
	}
	req, err := translator.Translate()
	if err != nil {
		return err
	}

	if _, err := genclient.Generate(req); err != nil {
		return err
	}

	return nil
}
