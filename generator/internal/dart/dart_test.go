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
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

func TestDart_GeneratedFiles(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	annotateModel(model, map[string]string{})
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

func TestDart_TemplatesAvailable(t *testing.T) {
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

func TestDart_MessageNames(t *testing.T) {
	r := sample.Replication()
	a := sample.Automatic()
	model := api.NewTestAPI([]*api.Message{r, a}, []*api.Enum{}, []*api.Service{})
	model.PackageName = "test"
	annotateModel(model, map[string]string{})

	for _, test := range []struct {
		message *api.Message
		want    string
	}{
		{message: r, want: "Replication"},
		{message: a, want: "Replication$Automatic"},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := messageName(test.message); got != test.want {
				t.Errorf("mismatched message name, got=%q, want=%q", got, test.want)
			}
		})
	}
}

func TestDart_EnumNames(t *testing.T) {
	parent := &api.Message{
		Name:    "SecretVersion",
		ID:      ".test.SecretVersion",
		Package: "test",
		Fields: []*api.Field{
			{
				Name:     "automatic",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".test.Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &api.Enum{
		Name:    "State",
		ID:      ".test.SecretVersion.State",
		Parent:  parent,
		Package: "test",
	}
	non_nested := &api.Enum{
		Name:    "Code",
		ID:      ".test.Code",
		Package: "test",
	}

	model := api.NewTestAPI([]*api.Message{parent}, []*api.Enum{nested, non_nested}, []*api.Service{})
	model.PackageName = "test"
	annotateModel(model, map[string]string{})

	for _, test := range []struct {
		enum     *api.Enum
		wantEnum string
	}{
		{non_nested, "Code"},
		{nested, "SecretVersion$State"},
	} {
		if got := enumName(test.enum); got != test.wantEnum {
			t.Errorf("c.enumName(%q) = %q; want = %s", test.enum.Name, got, test.wantEnum)
		}
	}
}

func TestDart_EnumValues(t *testing.T) {
	enumValueSimple := &api.EnumValue{
		Name: "NAME",
		ID:   ".test.v1.SomeMessage.SomeEnum.NAME",
	}
	enumValueCompound := &api.EnumValue{
		Name: "ENUM_VALUE",
		ID:   ".test.v1.SomeMessage.SomeEnum.ENUM_VALUE",
	}
	someEnum := &api.Enum{
		Name:    "SomeEnum",
		ID:      ".test.v1.SomeMessage.SomeEnum",
		Values:  []*api.EnumValue{enumValueSimple, enumValueCompound},
		Package: "test.v1",
	}
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{someEnum}, []*api.Service{})
	model.PackageName = "test"
	annotateModel(model, map[string]string{})

	for _, test := range []struct {
		value    *api.EnumValue
		wantName string
	}{
		{enumValueSimple, "name"},
		{enumValueCompound, "enumValue"},
	} {
		if got := enumValueName(test.value); got != test.wantName {
			t.Errorf("c.enumName(%q) = %q; want = %s", test.value.Name, got, test.wantName)
		}
	}
}
