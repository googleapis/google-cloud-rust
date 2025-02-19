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

func TestGeneratedFiles(t *testing.T) {
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

func TestMessageNames(t *testing.T) {
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
		{message: sample.SecretPayload(), want: "SecretPayload"},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := messageName(test.message); got != test.want {
				t.Errorf("mismatched message name, got=%q, want=%q", got, test.want)
			}
		})
	}
}

func TestEnumNames(t *testing.T) {
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

func TestEnumValues(t *testing.T) {
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

func TestMethodInOutTypeName(t *testing.T) {
	message := sample.CreateRequest()
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
	annotateModel(model, map[string]string{})

	for _, test := range []struct {
		typeId string
		want   string
	}{
		{".google.protobuf.Empty", "void"},
		{message.ID, "CreateSecretRequest"},
	} {
		got := methodInOutTypeName(test.typeId, model.State)
		if got != test.want {
			t.Errorf("unexpected type name, got: %s want: %s", got, test.want)
		}
	}
}

func TestFieldType(t *testing.T) {
	// Test simple fields.
	for _, test := range []struct {
		typez api.Typez
		want  string
	}{
		{api.BOOL_TYPE, "bool"},
		{api.INT32_TYPE, "int"},
		{api.INT64_TYPE, "int"},
		{api.UINT32_TYPE, "int"},
		{api.UINT64_TYPE, "int"},
		{api.FLOAT_TYPE, "double"},
		{api.DOUBLE_TYPE, "double"},
		{api.STRING_TYPE, "String"},
		{api.BYTES_TYPE, "Uint8List"},
	} {
		field := &api.Field{
			Name:     "parent",
			JSONName: "parent",
			Typez:    test.typez,
		}
		message := &api.Message{
			Name:          "UpdateSecretRequest",
			ID:            "..UpdateRequest",
			Documentation: "Request message for SecretManagerService.UpdateSecret",
			Package:       sample.Package,
			Fields:        []*api.Field{field},
		}
		model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
		annotateModel(model, map[string]string{})

		got := fieldType(field, model.State, map[string]*dartImport{})
		if got != test.want {
			t.Errorf("unexpected type name, got: %s want: %s", got, test.want)
		}
	}

	// Test message and enum fields.
	sampleMessage := sample.CreateRequest()
	sampleEnum := sample.EnumState()

	field1 := &api.Field{
		Name:     "parent",
		JSONName: "parent",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  sampleMessage.ID,
	}
	field2 := &api.Field{
		Name:     "parent",
		JSONName: "parent",
		Typez:    api.ENUM_TYPE,
		TypezID:  sampleEnum.ID,
	}
	message := &api.Message{
		Name:          "UpdateSecretRequest",
		ID:            "..UpdateRequest",
		Documentation: "Request message for SecretManagerService.UpdateSecret",
		Package:       sample.Package,
		Fields:        []*api.Field{field1, field2},
	}
	model := api.NewTestAPI(
		[]*api.Message{message, sampleMessage},
		[]*api.Enum{sampleEnum},
		[]*api.Service{},
	)
	annotateModel(model, map[string]string{})

	got := fieldType(field1, model.State, map[string]*dartImport{})
	want := "CreateSecretRequest"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	got = fieldType(field2, model.State, map[string]*dartImport{})
	want = "State"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}
}

func TestFieldType_Maps(t *testing.T) {
	map1 := &api.Message{
		Name:  "$map<string, string>",
		ID:    "$map<string, string>",
		IsMap: true,
		Fields: []*api.Field{
			{
				Name:  "key",
				Typez: api.STRING_TYPE,
			},
			{
				Name:  "value",
				Typez: api.INT32_TYPE,
			},
		},
	}
	field := &api.Field{
		Name:     "map",
		JSONName: "map",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  map1.ID,
	}
	model := api.NewTestAPI([]*api.Message{map1}, []*api.Enum{}, []*api.Service{})
	annotateModel(model, map[string]string{})

	got := fieldType(field, model.State, map[string]*dartImport{})
	want := "Map<String, int>"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}
}

func TestFieldType_Bytes(t *testing.T) {
	field := &api.Field{
		Name:     "test",
		JSONName: "test",
		Typez:    api.BYTES_TYPE,
	}
	message := &api.Message{
		Name:   "$test",
		ID:     "$test",
		IsMap:  true,
		Fields: []*api.Field{field},
	}
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
	annotateModel(model, map[string]string{})
	imports := map[string]*dartImport{}

	got := fieldType(field, model.State, imports)
	want := "Uint8List"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	// verify the typed_data import
	if !(len(imports) > 0) {
		t.Errorf("unexpected: no typed_data import added")
	}

	for _, imp := range imports {
		got := imp.DartImport
		want := "dart:typed_data"
		if got != want {
			t.Errorf("unexpected import, got: %s want: %s", got, want)
		}
	}
}

func TestFieldType_Repeated(t *testing.T) {
	// Test repeated simple fields.
	for _, test := range []struct {
		typez api.Typez
		want  string
	}{
		{api.BOOL_TYPE, "List<bool>"},
		{api.INT32_TYPE, "List<int>"},
		{api.INT64_TYPE, "List<int>"},
		{api.UINT32_TYPE, "List<int>"},
		{api.UINT64_TYPE, "List<int>"},
		{api.FLOAT_TYPE, "List<double>"},
		{api.DOUBLE_TYPE, "List<double>"},
		{api.STRING_TYPE, "List<String>"},
	} {
		field := &api.Field{
			Name:     "parent",
			JSONName: "parent",
			Typez:    test.typez,
			Repeated: true,
		}
		message := &api.Message{
			Name:          "UpdateSecretRequest",
			ID:            "..UpdateRequest",
			Documentation: "Request message for SecretManagerService.UpdateSecret",
			Package:       sample.Package,
			Fields:        []*api.Field{field},
		}
		model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
		annotateModel(model, map[string]string{})

		got := fieldType(field, model.State, map[string]*dartImport{})
		if got != test.want {
			t.Errorf("unexpected type name, got: %s want: %s", got, test.want)
		}
	}

	// Test repeated message and enum fields.
	sampleMessage := sample.CreateRequest()
	sampleEnum := sample.EnumState()

	field1 := &api.Field{
		Name:     "parent",
		JSONName: "parent",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  sampleMessage.ID,
		Repeated: true,
	}
	field2 := &api.Field{
		Name:     "parent",
		JSONName: "parent",
		Typez:    api.ENUM_TYPE,
		TypezID:  sampleEnum.ID,
		Repeated: true,
	}
	message := &api.Message{
		Name:          "UpdateSecretRequest",
		ID:            "..UpdateRequest",
		Documentation: "Request message for SecretManagerService.UpdateSecret",
		Package:       sample.Package,
		Fields:        []*api.Field{field1, field2},
	}
	model := api.NewTestAPI(
		[]*api.Message{message, sampleMessage},
		[]*api.Enum{sampleEnum},
		[]*api.Service{},
	)
	annotateModel(model, map[string]string{})

	got := fieldType(field1, model.State, map[string]*dartImport{})
	want := "List<CreateSecretRequest>"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	got = fieldType(field2, model.State, map[string]*dartImport{})
	want = "List<State>"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}
}

func TestWKT(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	annotateModel(model, map[string]string{})

	for _, test := range []struct {
		wktID string
	}{
		{".google.protobuf.Duration"},
		{".google.protobuf.Timestamp"},
	} {
		resolvedType := model.State.MessageByID[test.wktID]

		if resolvedType == nil {
			t.Errorf("no mapping for WKT: %s", test.wktID)
		}
	}
}
