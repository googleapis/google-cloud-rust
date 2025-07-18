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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

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
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	for _, test := range []struct {
		message *api.Message
		want    string
	}{
		{message: r, want: "Replication"},
		{message: a, want: "Replication_Automatic"},
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
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	for _, test := range []struct {
		enum     *api.Enum
		wantEnum string
	}{
		{non_nested, "Code"},
		{nested, "SecretVersion_State"},
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
	enumReservedName := &api.EnumValue{
		Name: "in",
		ID:   ".test.v1.SomeMessage.SomeEnum.in",
	}
	enumValueCompound := &api.EnumValue{
		Name: "ENUM_VALUE",
		ID:   ".test.v1.SomeMessage.SomeEnum.ENUM_VALUE",
	}
	someEnum := &api.Enum{
		Name:    "SomeEnum",
		ID:      ".test.v1.SomeMessage.SomeEnum",
		Values:  []*api.EnumValue{enumValueSimple, enumReservedName, enumValueCompound},
		Package: "test.v1",
	}
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{someEnum}, []*api.Service{})
	model.PackageName = "test"
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	for _, test := range []struct {
		value    *api.EnumValue
		wantName string
	}{
		{enumValueSimple, "name"},
		{enumReservedName, "in$"},
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
		}, {
			ID:   ".google.protobuf.Timestamp",
			Name: "Timestamp",
		},
	}, []*api.Enum{}, []*api.Service{})

	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})
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
		got := annotate.resolveTypeName(state.MessageByID[test.typeId], true)
		if got != test.want {
			t.Errorf("unexpected type name, got: %s want: %s", got, test.want)
		}
	}
}

func TestResolveTypeName_ImportsMessages(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{
		{
			ID:      ".google.protobuf.Any",
			Package: "google.protobuf",
		}, {
			ID:      ".google.rpc.Status",
			Package: "google.rpc",
		}, {
			ID:      ".google.type.Expr",
			Package: "google.type",
		},
	}, []*api.Enum{}, []*api.Service{})

	// We use an explicit package name here; NewTestAPI will otherwise default to
	// 'google.type' and we won't be able to test that package name below.
	model.PackageName = "google.sample"

	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})
	state := model.State

	annotate.packageMapping = map[string]string{
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
		annotate.imports = map[string]string{}
		annotate.resolveTypeName(state.MessageByID[test.typeId], true)
		if _, ok := annotate.imports[test.want]; !ok {
			t.Errorf("import not added, got: %v want: %s", annotate.imports, test.want)
		}
	}
}

func TestResolveTypeName_ImportsEnum(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{
		{
			ID:      ".google.type.DayOfWeek",
			Package: "google.type",
		},
	}, []*api.Service{})

	// We use an explicit package name here; NewTestAPI will otherwise default to
	// 'google.type' and we won't be able to test that package name below.
	model.PackageName = "google.sample"

	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	annotate.packageMapping = map[string]string{
		"google.type": "package:google_cloud_type/type.dart",
	}

	field := &api.Field{
		Name:    "testField",
		Typez:   api.ENUM_TYPE,
		TypezID: ".google.type.DayOfWeek",
	}
	annotate.imports = map[string]string{}
	annotate.fieldType(field)
	want := "google.type"
	if _, ok := annotate.imports[want]; !ok {
		t.Errorf("import not added, got: %v want: %s", annotate.imports, want)
	}
}

func TestResolveTypeNameImportPrefixes(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{
		{
			ID:      ".google.protobuf.Timestamp",
			Name:    "Timestamp",
			Package: "google.protobuf",
		}, {
			ID:      ".google.protobuf.Duration",
			Name:    "Duration",
			Package: "google.protobuf",
		}, {
			ID:      ".google.rpc.Status",
			Name:    "Status",
			Package: "google.rpc",
		}, {
			ID:      ".google.type.DayOfWeek",
			Name:    "DayOfWeek",
			Package: "google.type",
		},
	}, []*api.Enum{}, []*api.Service{})

	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{
		"prefix:google.protobuf": "protobuf",
		"prefix:google.type":     "type",
	})
	state := model.State

	for _, test := range []struct {
		typeId string
		want   string
	}{
		{".google.rpc.Status", "Status"},
		{".google.protobuf.Timestamp", "protobuf.Timestamp"},
		{".google.protobuf.Duration", "protobuf.Duration"},
		{".google.type.DayOfWeek", "type.DayOfWeek"},
	} {
		t.Run(test.want, func(t *testing.T) {
			got := annotate.resolveTypeName(state.MessageByID[test.typeId], true)
			if got != test.want {
				t.Errorf("unexpected type name, got: %s want: %s", got, test.want)
			}
		})
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
		{api.UINT32_TYPE, "int"},
		{api.FIXED32_TYPE, "int"},
		{api.SFIXED32_TYPE, "int"},
		{api.INT64_TYPE, "int"},
		{api.UINT64_TYPE, "int"},
		{api.FIXED64_TYPE, "int"},
		{api.SFIXED64_TYPE, "int"},
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
		annotate := newAnnotateModel(model)
		annotate.annotateModel(map[string]string{})

		got := annotate.fieldType(field)
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
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	got := annotate.fieldType(field1)
	want := "CreateSecretRequest"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	got = annotate.fieldType(field2)
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
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	got := annotate.fieldType(field)
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
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})
	annotate.imports = map[string]string{}

	{
		got := annotate.fieldType(field)
		want := "Uint8List"
		if got != want {
			t.Errorf("unexpected type name, got: %s want: %s", got, want)
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
		{api.UINT32_TYPE, "List<int>"},
		{api.FIXED32_TYPE, "List<int>"},
		{api.SFIXED32_TYPE, "List<int>"},
		{api.INT64_TYPE, "List<int>"},
		{api.UINT64_TYPE, "List<int>"},
		{api.FIXED64_TYPE, "List<int>"},
		{api.SFIXED64_TYPE, "List<int>"},
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
		annotate := newAnnotateModel(model)
		annotate.annotateModel(map[string]string{})

		got := annotate.fieldType(field)
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
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	got := annotate.fieldType(field1)
	want := "List<CreateSecretRequest>"
	if got != want {
		t.Errorf("unexpected type name, got: %s want: %s", got, want)
	}

	got = annotate.fieldType(field2)
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

func TestFormatDocCommentsRewriteReferences(t *testing.T) {
	state := &api.APIState{}

	for _, test := range []struct {
		testName string
		input    string
		output   string
	}{
		{
			testName: "regular api ref",
			input:    "foo [Code][google.rpc.Code] bar",
			output:   "/// foo `Code` bar",
		},
		{
			testName: "implicit api ref",
			input:    "foo [google.rpc.Code][] bar",
			output:   "/// foo `google.rpc.Code` bar",
		},
		{
			testName: "two on a line",
			input:    "foo [Code][google.rpc.Code] and [AnalyzeSentiment][] bar",
			output:   "/// foo `Code` and `AnalyzeSentiment` bar",
		},
		{
			testName: "multi-line",
			input: `For calls to [AnalyzeSentiment][] or if
[AnnotateTextRequest.Features.extract_document_sentiment][google.cloud.language.v2.AnnotateTextRequest.Features.extract_document_sentiment]
is set to true, this field will contain the sentiment for the sentence.`,
			output: "/// For calls to `AnalyzeSentiment` or if\n" +
				"/// `AnnotateTextRequest.Features.extract_document_sentiment`\n" +
				"/// is set to true, this field will contain the sentiment for the sentence.",
		},
		{
			testName: "no match - spaces",
			input:    "foo [Code ref][google.rpc.Code] bar",
			output:   "/// foo [Code ref][google.rpc.Code] bar",
		},
		{
			testName: "no match - missing brackets",
			input:    "foo [google.rpc.Code] bar",
			output:   "/// foo [google.rpc.Code] bar",
		},
	} {
		t.Run(test.testName, func(t *testing.T) {
			gotLines := formatDocComments(test.input, state)
			got := strings.Join(gotLines, "\n")
			if diff := cmp.Diff(test.output, got); diff != "" {
				t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
			}
		})
	}
}

func TestHttpPathFmt(t *testing.T) {
	for _, test := range []struct {
		method *api.Method
		want   string
	}{
		{method: sample.MethodCreate(), want: "/v1/projects/${request.project}/secrets"},
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
