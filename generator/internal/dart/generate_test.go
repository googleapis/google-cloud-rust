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
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestGeneratedFiles(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})
	files := generatedFiles(model)
	if len(files) == 0 {
		t.Errorf("expected a non-empty list of template files from generatedFiles()")
	}

	// Validate that main.dart was replaced with {servicename}.dart.
	for _, fileInfo := range files {
		if filepath.Base(fileInfo.OutputPath) == "main.dart" {
			t.Errorf("expected the main.dart template to be generated as {servicename}.dart")
		}
	}
}

func TestTemplatesAvailable(t *testing.T) {
	var count = 0
	fs.WalkDir(dartTemplates, "templates", func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) != ".mustache" {
			return nil
		}
		if strings.Count(d.Name(), ".") == 1 {
			// skip partials
			return nil
		}
		count++
		return nil
	})

	if count == 0 {
		t.Errorf("no dart templates found")
	}
}
