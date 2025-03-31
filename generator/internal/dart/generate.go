// Copyright 2025 Google LLC
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

package dart

import (
	"embed"
	"path/filepath"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
)

//go:embed templates
var dartTemplates embed.FS

func Generate(model *api.API, outdir string, config *config.Config) error {
	annotate := newAnnotateModel(model)
	_, err := annotate.annotateModel(config.Codec)
	if err != nil {
		return err
	}

	provider := templatesProvider()
	err = language.GenerateFromModel(outdir, model, provider, generatedFiles(model))
	if err == nil {
		// Check if we're configured to skip formatting.
		skipFormat := config.Codec["skip-format"]
		if skipFormat != "true" {
			err = formatDirectory(outdir)
		}
	}

	return err
}

func templatesProvider() language.TemplateProvider {
	return func(name string) (string, error) {
		contents, err := dartTemplates.ReadFile(name)
		if err != nil {
			return "", err
		}
		return string(contents), nil
	}
}

func generatedFiles(model *api.API) []language.GeneratedFile {
	codec := model.Codec.(*modelAnnotations)
	mainFileName := codec.MainFileName

	files := language.WalkTemplatesDir(dartTemplates, "templates")

	// Look for and replace 'main.dart' with '{servicename}.dart'
	for index, fileInfo := range files {
		if filepath.Base(fileInfo.TemplatePath) == "main.dart.mustache" {
			outDir := filepath.Dir(fileInfo.OutputPath)
			fileInfo.OutputPath = filepath.Join(outDir, mainFileName+".dart")

			files[index] = fileInfo
		}
	}

	return files
}
