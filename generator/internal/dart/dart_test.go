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

	"github.com/google/go-cmp/cmp"
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
	f := &api.Message{
		Name: "Function",
		ID:   ".google.cloud.functions.v2.Function",
	}
	model := api.NewTestAPI(
		[]*api.Message{r, a, f, sample.CustomerManagedEncryption()},
		[]*api.Enum{},
		[]*api.Service{})
	model.PackageName = "test"
	annotateModel(model, map[string]string{})

	for _, test := range []struct {
		message *api.Message
		want    string
	}{
		{message: r, want: "Replication"},
		{message: a, want: "Replication$Automatic"},
		{message: f, want: "Function$"},
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
		ID:      sample.SecretVersion().ID,
		Package: "test",
		Fields: []*api.Field{
			{
				Name:     "automatic",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  sample.Automatic().ID,
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

	model := api.NewTestAPI(
		[]*api.Message{parent, sample.Automatic(), sample.CustomerManagedEncryption()},
		[]*api.Enum{nested, non_nested},
		[]*api.Service{})
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

func TestResolveTypeName(t *testing.T) {
	message := sample.CreateRequest()
	model := api.NewTestAPI([]*api.Message{
		message, {
			ID:   ".google.protobuf.Duration",
			Name: "Duration",
		}, {
			ID:   ".google.protobuf.Empty",
			Name: "Empty",
		},
		{
			ID:   ".google.protobuf.Timestamp",
			Name: "Timestamp",
		},
	}, []*api.Enum{}, []*api.Service{})

	annotateModel(model, map[string]string{})
	state := model.State

	for _, test := range []struct {
		typeId string
		want   string
	}{
		{message.ID, "CreateSecretRequest"},
		{".google.protobuf.Empty", "void"},
		{".google.protobuf.Timestamp", "Timestamp"},
		{".google.protobuf.Duration", "Duration"},
	} {
		got := resolveTypeName(state.MessageByID[test.typeId], map[string]string{}, map[string]string{})
		if got != test.want {
			t.Errorf("unexpected type name, got: %s want: %s", got, test.want)
		}
	}
}

func TestResolveTypeName_Imports(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{
		{
			ID:      ".google.protobuf.Any",
			Package: "google.protobuf",
		},
		{
			ID:      ".google.rpc.Status",
			Package: "google.rpc",
		},
		{
			ID:      ".google.type.Expr",
			Package: "google.type",
		},
	}, []*api.Enum{}, []*api.Service{})

	annotateModel(model, map[string]string{})
	state := model.State

	packageMapping := map[string]string{
		"google.protobuf": "package:google_cloud_protobuf/protobuf.dart",
		"google.rpc":      "package:google_cloud_rpc/rpc.dart",
		"google.type":     "package:google_cloud_type/type.dart",
	}

	for _, test := range []struct {
		typeId string
		want   string
	}{
		{".google.protobuf.Any", "google.protobuf"},
		{".google.rpc.Status", "google.rpc"},
		{".google.type.Expr", "google.type"},
	} {
		imports := map[string]string{}
		resolveTypeName(state.MessageByID[test.typeId], packageMapping, imports)
		if _, ok := imports[test.want]; !ok {
			t.Errorf("import not added type name, got: %v want: %s", imports, test.want)
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

		got := fieldType(field, model.State, map[string]string{}, map[string]string{})
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

	got := fieldType(field1, model.State, map[string]string{}, map[string]string{})
	want := "CreateSecretRequest"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	got = fieldType(field2, model.State, map[string]string{}, map[string]string{})
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

	got := fieldType(field, model.State, map[string]string{}, map[string]string{})
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
	imports := map[string]string{}

	got := fieldType(field, model.State, map[string]string{}, imports)
	want := "Uint8List"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	// verify the typed_data import
	if !(len(imports) > 0) {
		t.Errorf("unexpected: no typed_data import added")
	}

	for _, got := range imports {
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

		got := fieldType(field, model.State, map[string]string{}, map[string]string{})
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

	got := fieldType(field1, model.State, map[string]string{}, map[string]string{})
	want := "List<CreateSecretRequest>"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	got = fieldType(field2, model.State, map[string]string{}, map[string]string{})
	want = "List<State>"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}
}

func TestFormatDocComments(t *testing.T) {
	input := `Some comments describing the thing.

We want to respect whitespace at the beginning, because it important in Markdown:
- A thing
  - A nested thing
- The next thing
`

	want := []string{
		"/// Some comments describing the thing.",
		"///",
		"/// We want to respect whitespace at the beginning, because it important in Markdown:",
		"/// - A thing",
		"///   - A nested thing",
		"/// - The next thing",
	}
	state := &api.APIState{}
	got := formatDocComments(input, state)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsEmpty(t *testing.T) {
	input := ``

	want := []string{}
	state := &api.APIState{}
	got := formatDocComments(input, state)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsTrimTrailingSpaces(t *testing.T) {
	input := `The next line contains spaces.
  
This line has trailing spaces.  `

	want := []string{
		"/// The next line contains spaces.",
		"///",
		"/// This line has trailing spaces.",
	}
	state := &api.APIState{}
	got := formatDocComments(input, state)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsTrimTrailingEmptyLines(t *testing.T) {
	input := `Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.


`

	want := []string{
		"/// Lorem ipsum dolor sit amet, consectetur adipiscing elit,",
		"/// sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
		"/// Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.",
	}
	state := &api.APIState{}
	got := formatDocComments(input, state)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestHttpPathFmt(t *testing.T) {
	for _, test := range []struct {
		method *api.Method
		want   string
	}{
		{method: sample.MethodCreate(), want: "/v1/${request.parent}/secrets/${request.secretId}"},
		{method: sample.MethodUpdate(), want: "/v1/${request.secret.name}"},
		{method: sample.MethodAddSecretVersion(), want: "/v1/projects/${request.project}/secrets/${request.secret}:addVersion"},
		{method: sample.MethodListSecretVersions(), want: "/v1/projects/${request.parent}/secrets/${request.secret}:listSecretVersions"},
	} {
		t.Run(test.method.Name, func(t *testing.T) {
			if got := httpPathFmt(test.method.PathInfo); got != test.want {
				t.Errorf("unexpected httpPathFmt, got=%q, want=%q", got, test.want)
			}
		})
	}
}
