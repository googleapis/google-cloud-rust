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

package rust

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

func createRustCodec() *codec {
	wkt := &packagez{
		name:        "wkt",
		packageName: "types",
		path:        "../../types",
	}

	return &codec{
		modulePath:    "crate::model",
		extraPackages: []*packagez{wkt},
		packageMapping: map[string]*packagez{
			"google.protobuf": wkt,
		},
	}
}

func TestParseOptionsProtobuf(t *testing.T) {
	options := map[string]string{
		"version":                   "1.2.3",
		"package-name-override":     "test-only",
		"copyright-year":            "2035",
		"module-path":               "alternative::generated",
		"package:wkt":               "package=types,path=src/wkt,source=google.protobuf,source=test-only",
		"package:gax":               "package=gax,path=src/gax,feature=unstable-sdk-client",
		"package:serde_with":        "package=serde_with,version=2.3.4,default-features=false",
		"include-grpc-only-methods": "true",
		"per-service-features":      "true",
	}
	got, err := newCodec(true, options)
	if err != nil {
		t.Fatal(err)
	}
	gp := &packagez{
		name:            "wkt",
		packageName:     "types",
		path:            "src/wkt",
		defaultFeatures: true,
	}
	want := &codec{
		version:             "1.2.3",
		releaseLevel:        "preview",
		packageNameOverride: "test-only",
		generationYear:      "2035",
		modulePath:          "alternative::generated",
		extraPackages: []*packagez{
			gp,
			{
				name:        "gax",
				packageName: "gax",
				path:        "src/gax",
				features: []string{
					"unstable-sdk-client",
				},
				defaultFeatures: true,
			},
			{
				name:            "serde_with",
				packageName:     "serde_with",
				version:         "2.3.4",
				defaultFeatures: false,
			},
		},
		packageMapping: map[string]*packagez{
			"google.protobuf": gp,
			"test-only":       gp,
		},
		systemParameters: []systemParameter{
			{Name: "$alt", Value: "json;enum-encoding=int"},
		},
		includeGrpcOnlyMethods: true,
		perServiceFeatures:     true,
	}
	sort.Slice(want.extraPackages, func(i, j int) bool {
		return want.extraPackages[i].name < want.extraPackages[j].name
	})
	sort.Slice(got.extraPackages, func(i, j int) bool {
		return got.extraPackages[i].name < got.extraPackages[j].name
	})
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(codec{}, packagez{})); diff != "" {
		t.Errorf("codec mismatch (-want, +got):\n%s", diff)
	}
	if want.packageNameOverride != got.packageNameOverride {
		t.Errorf("mismatched in packageNameOverride, want=%s, got=%s", want.packageNameOverride, got.packageNameOverride)
	}
	checkRustPackages(t, got, want)
}

func TestParseOptionsOpenAPI(t *testing.T) {
	options := map[string]string{
		"version":               "1.2.3",
		"package-name-override": "test-only",
		"copyright-year":        "2035",
	}
	got, err := newCodec(false, options)
	if err != nil {
		t.Fatal(err)
	}
	want := &codec{
		version:             "1.2.3",
		releaseLevel:        "preview",
		packageNameOverride: "test-only",
		generationYear:      "2035",
		modulePath:          "crate::model",
		extraPackages:       []*packagez{},
		packageMapping:      map[string]*packagez{},
		systemParameters: []systemParameter{
			{Name: "$alt", Value: "json"},
		},
	}
	sort.Slice(want.extraPackages, func(i, j int) bool {
		return want.extraPackages[i].name < want.extraPackages[j].name
	})
	sort.Slice(got.extraPackages, func(i, j int) bool {
		return got.extraPackages[i].name < got.extraPackages[j].name
	})
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(codec{}, packagez{})); diff != "" {
		t.Errorf("codec mismatch (-want, +got):\n%s", diff)
	}
	if want.packageNameOverride != got.packageNameOverride {
		t.Errorf("mismatched in packageNameOverride, want=%s, got=%s", want.packageNameOverride, got.packageNameOverride)
	}
	checkRustPackages(t, got, want)
}

func TestParseOptionsTemplateOverride(t *testing.T) {
	options := map[string]string{
		"version":               "1.2.3",
		"package-name-override": "test-only",
		"copyright-year":        "2038",
		"template-override":     "templates/fancy-templates",
	}
	got, err := newCodec(false, options)
	if err != nil {
		t.Fatal(err)
	}
	want := &codec{
		version:             "1.2.3",
		releaseLevel:        "preview",
		packageNameOverride: "test-only",
		generationYear:      "2038",
		modulePath:          "crate::model",
		extraPackages:       []*packagez{},
		packageMapping:      map[string]*packagez{},
		systemParameters: []systemParameter{
			{Name: "$alt", Value: "json"},
		},
		templateOverride: "templates/fancy-templates",
	}
	sort.Slice(want.extraPackages, func(i, j int) bool {
		return want.extraPackages[i].name < want.extraPackages[j].name
	})
	sort.Slice(got.extraPackages, func(i, j int) bool {
		return got.extraPackages[i].name < got.extraPackages[j].name
	})
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(codec{}, packagez{})); diff != "" {
		t.Errorf("codec mismatch (-want, +got):\n%s", diff)
	}
	if want.packageNameOverride != got.packageNameOverride {
		t.Errorf("mismatched in packageNameOverride, want=%s, got=%s", want.packageNameOverride, got.packageNameOverride)
	}
	checkRustPackages(t, got, want)
}

func TestPackageName(t *testing.T) {
	rustPackageNameImpl(t, "test-only-overridden", map[string]string{
		"package-name-override": "test-only-overridden",
	}, &api.API{
		Name:        "test-only-name",
		PackageName: "google.cloud.service.v3",
	})
	rustPackageNameImpl(t, "google-cloud-service-v3", nil, &api.API{
		Name:        "test-only-name",
		PackageName: "google.cloud.service.v3",
	})
	rustPackageNameImpl(t, "google-cloud-type", nil, &api.API{
		Name:        "type",
		PackageName: "",
	})
}

func rustPackageNameImpl(t *testing.T, want string, opts map[string]string, api *api.API) {
	t.Helper()
	c, err := newCodec(true, opts)
	if err != nil {
		t.Fatal(err)
	}
	got := PackageName(api, c.packageNameOverride)
	if want != got {
		t.Errorf("mismatch in package name, want=%s, got=%s", want, got)
	}
}

func TestServiceName(t *testing.T) {
	c, err := newCodec(true, map[string]string{
		"name-overrides": ".google.testing.BadName=GoodName,.google.testing.Old=New",
	})
	if err != nil {
		t.Fatal(err)
	}
	testServiceNameImpl(t, c, "BadName", "GoodName")
	testServiceNameImpl(t, c, "Old", "New")
	testServiceNameImpl(t, c, "Unchanged", "Unchanged")

	c2, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	testServiceNameImpl(t, c2, "Unchanged", "Unchanged")
}

func testServiceNameImpl(t *testing.T, c *codec, serviceName string, want string) {
	t.Helper()
	s := &api.Service{
		Name:    serviceName,
		ID:      fmt.Sprintf(".google.testing.%s", serviceName),
		Package: "google.testing",
	}
	got := c.ServiceName(s)
	if want != got {
		t.Errorf("mismatch in service name, want=%s, got=%s", want, got)
	}
}

func TestOneOfEnumName(t *testing.T) {
	c, err := newCodec(true, map[string]string{
		"name-overrides": ".google.testing.Message.conflict=ConflictOneOf",
	})
	if err != nil {
		t.Fatal(err)
	}
	testOneOfEnumNameImpl(t, c, "conflict", "ConflictOneOf")
	testOneOfEnumNameImpl(t, c, "basic_case", "BasicCase")

	c2, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	testOneOfEnumNameImpl(t, c2, "conflict", "Conflict")
	testOneOfEnumNameImpl(t, c2, "basic_case", "BasicCase")
}

func testOneOfEnumNameImpl(t *testing.T, c *codec, name string, want string) {
	t.Helper()
	oneof := &api.OneOf{
		Name: name,
		ID:   fmt.Sprintf(".google.testing.Message.%s", name),
	}
	got := c.OneOfEnumName(oneof)
	if want != got {
		t.Errorf("mismatch in service name, want=%s, got=%s", want, got)
	}
}

func checkRustPackages(t *testing.T, got *codec, want *codec) {
	t.Helper()
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want.extraPackages, got.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("package mismatch (-want, +got):\n%s", diff)
	}
}

func TestWellKnownTypesExist(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	loadWellKnownTypes(model.State)
	for _, name := range []string{"Any", "Duration", "Empty", "FieldMask", "Timestamp"} {
		if _, ok := model.State.MessageByID[fmt.Sprintf(".google.protobuf.%s", name)]; !ok {
			t.Errorf("cannot find well-known message %s in API", name)
		}
	}
}

func TestWellKnownTypesAsMethod(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	loadWellKnownTypes(model.State)

	want := "wkt::Empty"
	got := c.methodInOutTypeName(".google.protobuf.Empty", model.State, model.PackageName)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}
}

func TestGeneratedFiles(t *testing.T) {
	c := codec{
		templateOverride: "templates/mod",
	}
	files := c.generatedFiles(false)
	if len(files) == 0 {
		t.Errorf("expected a non-empty list of template files from generatedFiles(true, false)")
	}
	// No crate for module-only files
	unexpectedGeneratedFile(t, "Cargo.toml", files)

	files = c.generatedFiles(true)
	if len(files) == 0 {
		t.Errorf("expected a non-empty list of template files from generatedFiles(true, true)")
	}
	// No crate for module-only files
	unexpectedGeneratedFile(t, "Cargo.toml", files)

	c.templateOverride = ""
	files = c.generatedFiles(false)
	if len(files) == 0 {
		t.Errorf("expected a non-empty list of template files from generatedFiles(false, false)")
	}
	// Must have crate crate for module-only files
	expectGeneratedFile(t, "Cargo.toml", files)
	// Should not have a client if there are no services.
	unexpectedGeneratedFile(t, "client.rs", files)

	files = c.generatedFiles(true)
	if len(files) == 0 {
		t.Errorf("expected a non-empty list of template files from generatedFiles(false, false)")
	}
	// Must have crate crate for module-only files
	expectGeneratedFile(t, "Cargo.toml", files)
	expectGeneratedFile(t, "client.rs", files)
}

func expectGeneratedFile(t *testing.T, name string, files []language.GeneratedFile) {
	t.Helper()
	for _, g := range files {
		if strings.HasSuffix(g.OutputPath, name) {
			return
		}
	}
	t.Errorf("could not find %s in %v", name, files)
}

func unexpectedGeneratedFile(t *testing.T, name string, files []language.GeneratedFile) {
	t.Helper()
	for _, g := range files {
		if strings.HasSuffix(g.OutputPath, name) {
			t.Errorf("unexpectedly found %s in %v", name, files)
		}
	}
}

func TestMethodInOut(t *testing.T) {
	message := &api.Message{
		Name: "Target",
		ID:   "..Target",
	}
	nested := &api.Message{
		Name:   "Nested",
		ID:     "..Target.Nested",
		Parent: message,
	}
	model := api.NewTestAPI([]*api.Message{message, nested}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	loadWellKnownTypes(model.State)

	want := "crate::model::Target"
	got := c.methodInOutTypeName("..Target", model.State, model.PackageName)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}

	want = "crate::model::target::Nested"
	got = c.methodInOutTypeName("..Target.Nested", model.State, model.PackageName)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}
}

func rustFieldTypesCases() *api.API {
	target := &api.Message{
		Name: "Target",
		ID:   "..Target",
	}
	mapMessage := &api.Message{
		Name:  "$MapMessage",
		ID:    "..$MapMessage",
		IsMap: true,
		Fields: []*api.Field{
			{Name: "key", ID: "..$Message.key", Typez: api.INT32_TYPE},
			{Name: "value", ID: "..$Message.value", Typez: api.INT32_TYPE},
		},
	}
	message := &api.Message{
		Name: "Message",
		ID:   "..Message",
		Fields: []*api.Field{
			{
				Name:     "f_int32",
				Typez:    api.INT32_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_int32_optional",
				Typez:    api.INT32_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_int32_repeated",
				Typez:    api.INT32_TYPE,
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_string",
				Typez:    api.STRING_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_string_optional",
				Typez:    api.STRING_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_string_repeated",
				Typez:    api.STRING_TYPE,
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_msg",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_msg_repeated",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: false,
				Repeated: true,
			},
			{
				Name:      "f_msg_recursive",
				Typez:     api.MESSAGE_TYPE,
				TypezID:   "..Message",
				Optional:  true,
				Repeated:  false,
				Recursive: true,
			},
			{
				Name:      "f_msg_recursive_repeated",
				Typez:     api.MESSAGE_TYPE,
				TypezID:   "..Message",
				Optional:  false,
				Repeated:  true,
				Recursive: true,
			},
			{
				Name:     "f_timestamp",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_timestamp_repeated",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_map",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..$MapMessage",
				Optional: false,
				Repeated: false,
			},
		},
	}
	return api.NewTestAPI([]*api.Message{target, mapMessage, message}, []*api.Enum{}, []*api.Service{})

}

func TestFieldType(t *testing.T) {
	model := rustFieldTypesCases()
	message, ok := model.State.MessageByID["..Message"]
	if !ok {
		t.Fatalf("cannot find message `..Message`")
	}
	expectedTypes := map[string]string{
		"f_int32":                  "i32",
		"f_int32_optional":         "std::option::Option<i32>",
		"f_int32_repeated":         "std::vec::Vec<i32>",
		"f_string":                 "std::string::String",
		"f_string_optional":        "std::option::Option<std::string::String>",
		"f_string_repeated":        "std::vec::Vec<std::string::String>",
		"f_msg":                    "std::option::Option<crate::model::Target>",
		"f_msg_repeated":           "std::vec::Vec<crate::model::Target>",
		"f_msg_recursive":          "std::option::Option<std::boxed::Box<crate::model::Message>>",
		"f_msg_recursive_repeated": "std::vec::Vec<crate::model::Message>",
		"f_timestamp":              "std::option::Option<wkt::Timestamp>",
		"f_timestamp_repeated":     "std::vec::Vec<wkt::Timestamp>",
		"f_map":                    "std::collections::HashMap<i32,i32>",
	}
	expectedPrimitiveTypes := map[string]string{
		"f_int32":                  "i32",
		"f_int32_optional":         "i32",
		"f_int32_repeated":         "i32",
		"f_string":                 "std::string::String",
		"f_string_optional":        "std::string::String",
		"f_string_repeated":        "std::string::String",
		"f_msg":                    "crate::model::Target",
		"f_msg_repeated":           "crate::model::Target",
		"f_msg_recursive":          "crate::model::Message",
		"f_msg_recursive_repeated": "crate::model::Message",
		"f_timestamp":              "wkt::Timestamp",
		"f_timestamp_repeated":     "wkt::Timestamp",
		"f_map":                    "std::collections::HashMap<i32,i32>",
	}
	c := createRustCodec()
	loadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := fieldType(field, model.State, false, c.modulePath, model.PackageName, c.packageMapping)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}

		want, ok = expectedPrimitiveTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got = fieldType(field, model.State, true, c.modulePath, model.PackageName, c.packageMapping)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestOneOfFieldType(t *testing.T) {
	model := rustFieldTypesCases()
	message, ok := model.State.MessageByID["..Message"]
	if !ok {
		t.Fatalf("cannot find message `..Message`")
	}

	expectedTypes := map[string]string{
		"f_int32":                  "i32",
		"f_int32_optional":         "std::option::Option<i32>",
		"f_int32_repeated":         "std::vec::Vec<i32>",
		"f_string":                 "std::string::String",
		"f_string_optional":        "std::option::Option<std::string::String>",
		"f_string_repeated":        "std::vec::Vec<std::string::String>",
		"f_msg":                    "std::boxed::Box<crate::model::Target>",
		"f_msg_repeated":           "std::vec::Vec<crate::model::Target>",
		"f_msg_recursive":          "std::boxed::Box<crate::model::Message>",
		"f_msg_recursive_repeated": "std::vec::Vec<crate::model::Message>",
		"f_timestamp":              "std::boxed::Box<wkt::Timestamp>",
		"f_timestamp_repeated":     "std::vec::Vec<wkt::Timestamp>",
		"f_map":                    "std::collections::HashMap<i32,i32>",
	}
	c := createRustCodec()
	loadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := oneOfFieldType(field, model.State, c.modulePath, model.PackageName, c.packageMapping)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

// Verify rustBaseFieldType works for map types with different value fields.
func TestFieldMapTypeValues(t *testing.T) {
	for _, test := range []struct {
		want  string
		value *api.Field
	}{
		{
			"std::collections::HashMap<i32,std::string::String>",
			&api.Field{Typez: api.STRING_TYPE},
		},
		{
			"std::collections::HashMap<i32,i64>",
			&api.Field{Typez: api.INT64_TYPE},
		},
		{
			"std::collections::HashMap<i32,wkt::Any>",
			&api.Field{Typez: api.MESSAGE_TYPE, TypezID: ".google.protobuf.Any"},
		},
		{
			"std::collections::HashMap<i32,crate::model::OtherMessage>",
			&api.Field{Typez: api.MESSAGE_TYPE, TypezID: ".test.OtherMessage"},
		},
		{
			"std::collections::HashMap<i32,crate::model::Message>",
			&api.Field{Typez: api.MESSAGE_TYPE, TypezID: ".test.Message"},
		},
	} {
		field := &api.Field{
			Name:    "indexed",
			ID:      ".test.Message.indexed",
			Typez:   api.MESSAGE_TYPE,
			TypezID: ".test.$MapThing",
		}
		other_message := &api.Message{
			Name:   "OtherMessage",
			ID:     ".test.OtherMessage",
			IsMap:  true,
			Fields: []*api.Field{},
		}
		message := &api.Message{
			Name:   "Message",
			ID:     ".test.Message",
			IsMap:  true,
			Fields: []*api.Field{field},
		}
		// Complete the value field
		value := test.value
		value.Name = "value"
		value.ID = ".test.$MapThing.value"
		key := &api.Field{
			Name:  "key",
			ID:    ".test.$MapThing.key",
			Typez: api.INT32_TYPE,
		}
		map_thing := &api.Message{
			Name:   "$MapThing",
			ID:     ".test.$MapThing",
			IsMap:  true,
			Fields: []*api.Field{key, value},
		}
		model := api.NewTestAPI([]*api.Message{message, other_message, map_thing}, []*api.Enum{}, []*api.Service{})
		api.LabelRecursiveFields(model)
		c := createRustCodec()
		loadWellKnownTypes(model.State)
		got := fieldType(field, model.State, false, c.modulePath, model.PackageName, c.packageMapping)
		if got != test.want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, test.want)
		}
	}
}

// Verify rustBaseFieldType works for map types with different key fields.
func TestFieldMapTypeKey(t *testing.T) {
	for _, test := range []struct {
		want string
		key  *api.Field
	}{
		{
			"std::collections::HashMap<i32,i64>",
			&api.Field{Typez: api.INT32_TYPE},
		},
		{
			"std::collections::HashMap<std::string::String,i64>",
			&api.Field{Typez: api.STRING_TYPE},
		},
		{
			"std::collections::HashMap<crate::model::EnumType,i64>",
			&api.Field{Typez: api.ENUM_TYPE, TypezID: ".test.EnumType"},
		},
	} {
		field := &api.Field{
			Name:    "indexed",
			ID:      ".test.Message.indexed",
			Typez:   api.MESSAGE_TYPE,
			TypezID: ".test.$MapThing",
		}
		message := &api.Message{
			Name:   "Message",
			ID:     ".test.Message",
			IsMap:  true,
			Fields: []*api.Field{field},
		}
		// Complete the value field
		key := test.key
		key.Name = "key"
		key.ID = ".test.$MapThing.key"
		value := &api.Field{
			Name:  "value",
			ID:    ".test.$MapThing.value",
			Typez: api.INT64_TYPE,
		}
		map_thing := &api.Message{
			Name:   "$MapThing",
			ID:     ".test.$MapThing",
			IsMap:  true,
			Fields: []*api.Field{key, value},
		}
		enum := &api.Enum{
			Name: "EnumType",
			ID:   ".test.EnumType",
		}
		model := api.NewTestAPI([]*api.Message{message, map_thing}, []*api.Enum{enum}, []*api.Service{})
		api.LabelRecursiveFields(model)
		c := createRustCodec()
		loadWellKnownTypes(model.State)
		got := fieldType(field, model.State, false, c.modulePath, model.PackageName, c.packageMapping)
		if got != test.want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, test.want)
		}
	}
}

func TestAsQueryParameter(t *testing.T) {
	options := &api.Message{
		Name:   "Options",
		ID:     "..Options",
		Fields: []*api.Field{},
	}
	optionsField := &api.Field{
		Name:     "options_field",
		JSONName: "optionsField",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  options.ID,
		Optional: true,
	}
	requiredField := &api.Field{
		Name:     "required_field",
		JSONName: "requiredField",
		Typez:    api.STRING_TYPE,
	}
	optionalField := &api.Field{
		Name:     "optional_field",
		JSONName: "optionalField",
		Typez:    api.STRING_TYPE,
		Optional: true,
	}
	repeatedField := &api.Field{
		Name:     "repeated_field",
		JSONName: "repeatedField",
		Typez:    api.STRING_TYPE,
		Repeated: true,
	}

	requiredEnumField := &api.Field{
		Name:     "required_enum_field",
		JSONName: "requiredEnumField",
		Typez:    api.ENUM_TYPE,
	}
	optionalEnumField := &api.Field{
		Name:     "optional_enum_field",
		JSONName: "optionalEnumField",
		Typez:    api.ENUM_TYPE,
		Optional: true,
	}
	repeatedEnumField := &api.Field{
		Name:     "repeated_enum_field",
		JSONName: "repeatedEnumField",
		Typez:    api.ENUM_TYPE,
		Repeated: true,
	}

	requiredFieldMaskField := &api.Field{
		Name:     "required_field_mask",
		JSONName: "requiredFieldMask",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".google.protobuf.FieldMask",
	}
	optionalFieldMaskField := &api.Field{
		Name:     "optional_field_mask",
		JSONName: "optionalFieldMask",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".google.protobuf.FieldMask",
		Optional: true,
	}
	request := &api.Message{
		Name: "TestRequest",
		ID:   "..TestRequest",
		Fields: []*api.Field{
			optionsField,
			requiredField, optionalField, repeatedField,
			requiredEnumField, optionalEnumField, repeatedEnumField,
			requiredFieldMaskField, optionalFieldMaskField,
		},
	}
	model := api.NewTestAPI(
		[]*api.Message{options, request},
		[]*api.Enum{},
		[]*api.Service{})
	loadWellKnownTypes(model.State)

	for _, test := range []struct {
		field *api.Field
		want  string
	}{
		{optionsField, `let builder = req.options_field.as_ref().map(|p| serde_json::to_value(p).map_err(Error::ser) ).transpose()?.into_iter().fold(builder, |builder, v| { use gaxi::query_parameter::QueryParameter; v.add(builder, "optionsField") });`},
		{requiredField, `let builder = builder.query(&[("requiredField", &req.required_field)]);`},
		{optionalField, `let builder = req.optional_field.iter().fold(builder, |builder, p| builder.query(&[("optionalField", p)]));`},
		{repeatedField, `let builder = req.repeated_field.iter().fold(builder, |builder, p| builder.query(&[("repeatedField", p)]));`},
		{requiredEnumField, `let builder = builder.query(&[("requiredEnumField", &req.required_enum_field)]);`},
		{optionalEnumField, `let builder = req.optional_enum_field.iter().fold(builder, |builder, p| builder.query(&[("optionalEnumField", p)]));`},
		{repeatedEnumField, `let builder = req.repeated_enum_field.iter().fold(builder, |builder, p| builder.query(&[("repeatedEnumField", p)]));`},
		{requiredFieldMaskField, `let builder = { use gaxi::query_parameter::QueryParameter; serde_json::to_value(&req.required_field_mask).map_err(Error::ser)?.add(builder, "requiredFieldMask") };`},
		{optionalFieldMaskField, `let builder = req.optional_field_mask.as_ref().map(|p| serde_json::to_value(p).map_err(Error::ser) ).transpose()?.into_iter().fold(builder, |builder, v| { use gaxi::query_parameter::QueryParameter; v.add(builder, "optionalFieldMask") });`},
	} {
		got := addQueryParameter(test.field)
		if test.want != got {
			t.Errorf("mismatched as query parameter for %s\nwant=%s\n got=%s", test.field.Name, test.want, got)
		}
	}
}

func TestOneOfAsQueryParameter(t *testing.T) {
	options := &api.Message{
		Name:   "Options",
		ID:     "..Options",
		Fields: []*api.Field{},
	}
	optionsField := &api.Field{
		Name:     "options_field",
		JSONName: "optionsField",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  options.ID,
		IsOneOf:  true,
	}
	typeField := &api.Field{
		Name:     "type",
		JSONName: "type",
		Typez:    api.INT32_TYPE,
		IsOneOf:  true,
	}
	singularField := &api.Field{
		Name:     "singular_field",
		JSONName: "singularField",
		Typez:    api.STRING_TYPE,
		IsOneOf:  true,
	}
	repeatedField := &api.Field{
		Name:     "repeated_field",
		JSONName: "repeatedField",
		Typez:    api.STRING_TYPE,
		Repeated: true,
		IsOneOf:  true,
	}

	singularEnumField := &api.Field{
		Name:     "singular_enum_field",
		JSONName: "singularEnumField",
		Typez:    api.ENUM_TYPE,
		IsOneOf:  true,
	}
	repeatedEnumField := &api.Field{
		Name:     "repeated_enum_field",
		JSONName: "repeatedEnumField",
		Typez:    api.ENUM_TYPE,
		Repeated: true,
		IsOneOf:  true,
	}

	singularFieldMaskField := &api.Field{
		Name:     "singular_field_mask",
		JSONName: "singularFieldMask",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".google.protobuf.FieldMask",
		IsOneOf:  true,
	}

	fields := []*api.Field{
		typeField,
		optionsField,
		singularField, repeatedField,
		singularEnumField, repeatedEnumField,
		singularFieldMaskField,
	}
	oneof := &api.OneOf{
		Name:   "one_of",
		ID:     "..Request.one_of",
		Fields: fields,
	}
	request := &api.Message{
		Name:   "TestRequest",
		ID:     "..TestRequest",
		Fields: fields,
		OneOfs: []*api.OneOf{oneof},
	}
	model := api.NewTestAPI(
		[]*api.Message{options, request},
		[]*api.Enum{},
		[]*api.Service{})
	api.CrossReference(model)
	loadWellKnownTypes(model.State)

	for _, test := range []struct {
		field *api.Field
		want  string
	}{
		{optionsField, `let builder = req.options_field().map(|p| serde_json::to_value(p).map_err(Error::ser) ).transpose()?.into_iter().fold(builder, |builder, p| { use gaxi::query_parameter::QueryParameter; p.add(builder, "optionsField") });`},
		{typeField, `let builder = req.r#type().iter().fold(builder, |builder, p| builder.query(&[("type", p)]));`},
		{singularField, `let builder = req.singular_field().iter().fold(builder, |builder, p| builder.query(&[("singularField", p)]));`},
		{repeatedField, `let builder = req.repeated_field().iter().fold(builder, |builder, p| builder.query(&[("repeatedField", p)]));`},
		{singularEnumField, `let builder = req.singular_enum_field().iter().fold(builder, |builder, p| builder.query(&[("singularEnumField", p)]));`},
		{repeatedEnumField, `let builder = req.repeated_enum_field().iter().fold(builder, |builder, p| builder.query(&[("repeatedEnumField", p)]));`},
		{singularFieldMaskField, `let builder = req.singular_field_mask().map(|p| serde_json::to_value(p).map_err(Error::ser) ).transpose()?.into_iter().fold(builder, |builder, p| { use gaxi::query_parameter::QueryParameter; p.add(builder, "singularFieldMask") });`},
	} {
		got := addQueryParameter(test.field)
		if test.want != got {
			t.Errorf("mismatched as query parameter for %s\nwant=%s\n got=%s", test.field.Name, test.want, got)
		}
	}
}

type rustCaseConvertTest struct {
	Input    string
	Expected string
}

func TestToSnake(t *testing.T) {
	var snakeConvertTests = []rustCaseConvertTest{
		{"FooBar", "foo_bar"},
		{"foo_bar", "foo_bar"},
		{"data_crc32c", "data_crc32c"},
		{"True", "r#true"},
		{"Static", "r#static"},
		{"Trait", "r#trait"},
		{"Self", "r#self"},
		{"self", "r#self"},
		{"yield", "r#yield"},
	}
	for _, test := range snakeConvertTests {
		if output := toSnake(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestToScreamingSnake(t *testing.T) {
	var snakeConvertTests = []rustCaseConvertTest{
		{"FooBar", "FOO_BAR"},
		{"FOO_BAR", "FOO_BAR"},
		{"week5", "WEEK_5"},
		{"TYPE_INT64", "TYPE_INT64"},
	}
	for _, test := range snakeConvertTests {
		if output := toScreamingSnake(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestToPascal(t *testing.T) {
	var pascalConvertTests = []rustCaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"Self", "r#Self"},
		{"self", "r#Self"},
		{"yield", "Yield"},
		{"IAMPolicy", "IAMPolicy"},
		{"IAMPolicyRequest", "IAMPolicyRequest"},
		{"IAM", "Iam"},
	}
	for _, test := range pascalConvertTests {
		if output := toPascal(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q", output, test.Expected)
		}
	}
}

func TestFormatDocComments(t *testing.T) {
	input := `Some comments describing the thing.

The next line has some extra trailing whitespace:` + "   " + `

We want to respect whitespace at the beginning, because it important in Markdown:

- A thing
  - A nested thing
- The next thing

Now for some fun with block quotes

` + "```" + `
Maybe they wanted to show some JSON:
{
  "foo": "bar"
}
` + "```"

	want := []string{
		"/// Some comments describing the thing.",
		"///",
		"/// The next line has some extra trailing whitespace:",
		"///",
		"/// We want to respect whitespace at the beginning, because it important in Markdown:",
		"///",
		"/// - A thing",
		"///   - A nested thing",
		"/// - The next thing",
		"///",
		"/// Now for some fun with block quotes",
		"///",
		"/// ```norust",
		"/// Maybe they wanted to show some JSON:",
		"/// {",
		`///   "foo": "bar"`,
		"/// }",
		"/// ```",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &codec{}
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsBullets(t *testing.T) {
	input := `In this example, in proto field could take one of the following values:

* full_name for a violation in the full_name value
* email_addresses[1].email for a violation in the email field of the
  first email_addresses message
* email_addresses[3].type[2] for a violation in the second type
  value in the third email_addresses message.)`
	want := []string{
		"/// In this example, in proto field could take one of the following values:",
		"///",
		"/// * full_name for a violation in the full_name value",
		"/// * email_addresses[1].email for a violation in the email field of the",
		"///   first email_addresses message",
		"/// * email_addresses[3].type[2] for a violation in the second type",
		"///   value in the third email_addresses message.)",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsNumbers(t *testing.T) {
	input := `Numbered lists are different:

1.   A simple list item
2.   A number list item
     continued in the next line
3.   A second list item

     with a second paragraph
4.   A third list item

     also with a second paragraph

	 * And some nested list items
	 * and some more
	   even split ones
	 * and more
5.   A fourth list item
    with some bad indentation
`
	want := []string{
		"/// Numbered lists are different:",
		"///",
		"/// 1. A simple list item",
		"///",
		"/// 1. A number list item",
		"///    continued in the next line",
		"///",
		"/// 1. A second list item",
		"///",
		"///    with a second paragraph",
		"///",
		"/// 1. A third list item",
		"///",
		"///    also with a second paragraph",
		"///",
		"///    * And some nested list items",
		"///    * and some more",
		"///      even split ones",
		"///    * and more",
		"/// 1. A fourth list item",
		"///    with some bad indentation",
		"///",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsImplicitBlockQuote(t *testing.T) {
	input := `
Blockquotes come in many forms. They can start with a leading '> ', as in:

> Block quote style 1
> Continues 1 - style 1
> Continues 2 - style 1
> Continues 3 - style 1

They can start with 3 spaces and then '> ', as in:

   > Block quote style 2
   > Continues 1 - style 2
   > Continues 2 - style 2
   > Continues 3 - style 2

Or they can start with just 4 spaces:

    Block quote style 3
    Continues 1 - style 3
    Continues 2 - style 3
    Continues 3 - style 3

Note that four spaces and a leading '> ' makes the '> ' prefix part of the
block:

    > Block quote with arrow.
    > Continues 1 - with arrow
    > Continues 2 - with arrow
    Continues 3 - with arrow

`

	want := []string{
		"/// Blockquotes come in many forms. They can start with a leading '> ', as in:",
		"///",
		"/// Block quote style 1",
		"/// Continues 1 - style 1",
		"/// Continues 2 - style 1",
		"/// Continues 3 - style 1",
		"///",
		"/// They can start with 3 spaces and then '> ', as in:",
		"///",
		"/// Block quote style 2",
		"/// Continues 1 - style 2",
		"/// Continues 2 - style 2",
		"/// Continues 3 - style 2",
		"///",
		"/// Or they can start with just 4 spaces:",
		"///",
		"/// ```norust",
		"/// Block quote style 3",
		"/// Continues 1 - style 3",
		"/// Continues 2 - style 3",
		"/// Continues 3 - style 3",
		"/// ```",
		"///",
		"/// Note that four spaces and a leading '> ' makes the '> ' prefix part of the",
		"/// block:",
		"///",
		"/// ```norust",
		"/// > Block quote with arrow.",
		"/// > Continues 1 - with arrow",
		"/// > Continues 2 - with arrow",
		"/// Continues 3 - with arrow",
		"/// ```",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &codec{}
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsImplicitBlockQuoteClosing(t *testing.T) {
	input := `Blockquotes can appear at the end of the comment:

    they should have a closing element.`

	want := []string{
		"/// Blockquotes can appear at the end of the comment:",
		"///",
		"/// ```norust",
		"/// they should have a closing element.",
		"/// ```",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &codec{}
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsLinkDefinitions(t *testing.T) {
	input := `Link definitions should be added when collapsed links are used.
For example, [google][].
Second [example][].
[Third] example.
[google]: https://www.google.com
[example]: https://www.example.com
[Third]: https://www.third.com`

	want := []string{
		"/// Link definitions should be added when collapsed links are used.",
		"/// For example, [google][].",
		"/// Second [example][].",
		"/// [Third] example.",
		"/// [google]: https://www.google.com",
		"/// [example]: https://www.example.com",
		"/// [Third]: https://www.third.com",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &codec{}
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsCrossLinks(t *testing.T) {
	input := `
[Any][google.protobuf.Any]
[Message][test.v1.SomeMessage]
[Enum][test.v1.SomeMessage.SomeEnum]
[Message][test.v1.SomeMessage] repeated
[Service][test.v1.SomeService] [field][test.v1.SomeMessage.field]
[oneof group][test.v1.SomeMessage.result]
[oneof field][test.v1.SomeMessage.error]
[unmangled field][test.v1.SomeMessage.type] - normally r#type, but not in links
[SomeMessage.error][test.v1.SomeMessage.error]
[ExternalMessage][google.iam.v1.SetIamPolicyRequest]
[ExternalService][google.iam.v1.Iampolicy]
[ENUM_VALUE][test.v1.SomeMessage.SomeEnum.ENUM_VALUE]
[SomeService.CreateFoo][test.v1.SomeService.CreateFoo]
[SomeService.CreateBar][test.v1.SomeService.CreateBar]
[a method][test.v1.YELL.CreateThing]
[the service name][test.v1.YELL]
`
	want := []string{
		"/// [Any][google.protobuf.Any]",
		"/// [Message][test.v1.SomeMessage]",
		"/// [Enum][test.v1.SomeMessage.SomeEnum]",
		"/// [Message][test.v1.SomeMessage] repeated",
		"/// [Service][test.v1.SomeService] [field][test.v1.SomeMessage.field]",
		"/// [oneof group][test.v1.SomeMessage.result]",
		"/// [oneof field][test.v1.SomeMessage.error]",
		"/// [unmangled field][test.v1.SomeMessage.type] - normally r#type, but not in links",
		"/// [SomeMessage.error][test.v1.SomeMessage.error]",
		"/// [ExternalMessage][google.iam.v1.SetIamPolicyRequest]",
		"/// [ExternalService][google.iam.v1.Iampolicy]",
		"/// [ENUM_VALUE][test.v1.SomeMessage.SomeEnum.ENUM_VALUE]",
		"/// [SomeService.CreateFoo][test.v1.SomeService.CreateFoo]",
		"/// [SomeService.CreateBar][test.v1.SomeService.CreateBar]",
		"/// [a method][test.v1.YELL.CreateThing]",
		"/// [the service name][test.v1.YELL]",
		"///",
		"/// [google.iam.v1.Iampolicy]: iam_v1::client::Iampolicy",
		"/// [google.iam.v1.SetIamPolicyRequest]: iam_v1::model::SetIamPolicyRequest",
		"/// [google.protobuf.Any]: wkt::Any",
		"/// [test.v1.SomeMessage]: crate::model::SomeMessage",
		"/// [test.v1.SomeMessage.SomeEnum]: crate::model::some_message::SomeEnum",
		"/// [test.v1.SomeMessage.SomeEnum.ENUM_VALUE]: crate::model::some_message::SomeEnum::EnumValue",
		"/// [test.v1.SomeMessage.error]: crate::model::SomeMessage::result",
		"/// [test.v1.SomeMessage.field]: crate::model::SomeMessage::field",
		"/// [test.v1.SomeMessage.result]: crate::model::SomeMessage::result",
		"/// [test.v1.SomeMessage.type]: crate::model::SomeMessage::type",
		"/// [test.v1.SomeService]: crate::client::SomeService",
		// Skipped because the method is skipped
		// "/// [test.v1.SomeService.CreateBar]: crate::client::SomeService::create_bar",
		"/// [test.v1.SomeService.CreateFoo]: crate::client::SomeService::create_foo",
		// Services named with all uppercase have a different mapping.
		"/// [test.v1.YELL]: crate::client::Yell",
		"/// [test.v1.YELL.CreateThing]: crate::client::Yell::create_thing",
	}

	wkt := &packagez{
		name:        "wkt",
		packageName: "google-cloud-wkt",
		path:        "src/wkt",
	}
	iam := &packagez{
		name:        "iam_v1",
		packageName: "gcp-sdk-iam-v1",
		path:        "src/generated/iam/v1",
	}
	c := &codec{
		modulePath: "crate::model",
		packageMapping: map[string]*packagez{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex model.API instance. Create it
	// in a separate function to make this more readable.
	model := makeApiForRustFormatDocCommentsCrossLinks()
	loadWellKnownTypes(model.State)

	got := c.formatDocComments(input, "test-only-ID", model.State, []string{"test.v1"})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsRelativeCrossLinks(t *testing.T) {
	input := `
[relative link to service][SomeService]
[relative link to method][SomeService.CreateFoo]
[relative link to message][SomeMessage]
[relative link to message field][SomeMessage.field]
[relative link to message oneof group][SomeMessage.result]
[relative link to message oneof field][SomeMessage.error]
[relative link to unmangled field][SomeMessage.type]
[relative link to enum][SomeMessage.SomeEnum]
[relative link to enum value][SomeMessage.SomeEnum.ENUM_VALUE]
`
	want := []string{
		"/// [relative link to service][SomeService]",
		"/// [relative link to method][SomeService.CreateFoo]",
		"/// [relative link to message][SomeMessage]",
		"/// [relative link to message field][SomeMessage.field]",
		"/// [relative link to message oneof group][SomeMessage.result]",
		"/// [relative link to message oneof field][SomeMessage.error]",
		"/// [relative link to unmangled field][SomeMessage.type]",
		"/// [relative link to enum][SomeMessage.SomeEnum]",
		"/// [relative link to enum value][SomeMessage.SomeEnum.ENUM_VALUE]",
		"///",
		"/// [SomeMessage]: crate::model::SomeMessage",
		"/// [SomeMessage.SomeEnum]: crate::model::some_message::SomeEnum",
		"/// [SomeMessage.SomeEnum.ENUM_VALUE]: crate::model::some_message::SomeEnum::EnumValue",
		"/// [SomeMessage.error]: crate::model::SomeMessage::result",
		"/// [SomeMessage.field]: crate::model::SomeMessage::field",
		"/// [SomeMessage.result]: crate::model::SomeMessage::result",
		"/// [SomeMessage.type]: crate::model::SomeMessage::type",
		"/// [SomeService]: crate::client::SomeService",
		"/// [SomeService.CreateFoo]: crate::client::SomeService::create_foo",
	}
	wkt := &packagez{
		name:        "wkt",
		packageName: "google-cloud-wkt",
		path:        "src/wkt",
	}
	iam := &packagez{
		name:        "iam_v1",
		packageName: "gcp-sdk-iam-v1",
		path:        "src/generated/iam/v1",
	}
	c := &codec{
		modulePath: "crate::model",
		packageMapping: map[string]*packagez{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex model.API instance. Create it
	// in a separate function to make this more readable.
	model := makeApiForRustFormatDocCommentsCrossLinks()
	loadWellKnownTypes(model.State)

	got := c.formatDocComments(input, "test-only-ID", model.State, []string{"test.v1"})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsImpliedCrossLinks(t *testing.T) {
	input := `
implied service reference [SomeService][]
implied method reference [SomeService.CreateFoo][]
implied message reference [SomeMessage][]
implied message field reference [SomeMessage.field][]
implied message oneof group reference [SomeMessage.result][]
implied message oneof field reference [SomeMessage.error][]
implied message unmangled field reference [SomeMessage.type][]
implied enum reference [SomeMessage.SomeEnum][]
implied enum value reference [SomeMessage.SomeEnum.ENUM_VALUE][]
`
	want := []string{
		"/// implied service reference [SomeService][]",
		"/// implied method reference [SomeService.CreateFoo][]",
		"/// implied message reference [SomeMessage][]",
		"/// implied message field reference [SomeMessage.field][]",
		"/// implied message oneof group reference [SomeMessage.result][]",
		"/// implied message oneof field reference [SomeMessage.error][]",
		"/// implied message unmangled field reference [SomeMessage.type][]",
		"/// implied enum reference [SomeMessage.SomeEnum][]",
		"/// implied enum value reference [SomeMessage.SomeEnum.ENUM_VALUE][]",
		"///",
		"/// [SomeMessage]: crate::model::SomeMessage",
		"/// [SomeMessage.SomeEnum]: crate::model::some_message::SomeEnum",
		"/// [SomeMessage.SomeEnum.ENUM_VALUE]: crate::model::some_message::SomeEnum::EnumValue",
		"/// [SomeMessage.error]: crate::model::SomeMessage::result",
		"/// [SomeMessage.field]: crate::model::SomeMessage::field",
		"/// [SomeMessage.result]: crate::model::SomeMessage::result",
		"/// [SomeMessage.type]: crate::model::SomeMessage::type",
		"/// [SomeService]: crate::client::SomeService",
		"/// [SomeService.CreateFoo]: crate::client::SomeService::create_foo",
	}
	wkt := &packagez{
		name:        "wkt",
		packageName: "google-cloud-wkt",
		path:        "src/wkt",
	}
	iam := &packagez{
		name:        "iam_v1",
		packageName: "gcp-sdk-iam-v1",
		path:        "src/generated/iam/v1",
	}
	c := &codec{
		modulePath: "crate::model",
		packageMapping: map[string]*packagez{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex model.API instance. Create it
	// in a separate function to make this more readable.
	model := makeApiForRustFormatDocCommentsCrossLinks()
	loadWellKnownTypes(model.State)

	got := c.formatDocComments(input, "test-only-ID", model.State, []string{"test.v1.Message", "test.v1"})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestFormatDocCommentsHTMLTags(t *testing.T) {
	input := `Placeholders placed between angled brackets should be escaped.
	For example, example:<ip address> and another example:<second
	placeholder>.
	Third example: projects/<project>/secrets/<secret>
	Urls remain unchanged <https://www.example.com>
	Hyperlinks <a href=https://www.hyperlink.com>hyperlinked content</a>` + `
	HTML tags within code spans remain unchanged secret ` + "`" + `secrets/<secret>` + "`" + `
	Multiline hyperlinks should not be escaped <a
	href=https://en.wikipedia.org/wiki/Shebang_(Unix) class="external">shebang lines</a>.
	Multiline placeholders should be escaped <a
	placeholder>`

	want := []string{
		"/// Placeholders placed between angled brackets should be escaped.",
		"/// For example, example:\\<ip address\\> and another example:\\<second",
		"/// placeholder\\>.",
		"/// Third example: projects/\\<project\\>/secrets/\\<secret\\>",
		"/// Urls remain unchanged <https://www.example.com>",
		"/// Hyperlinks <a href=https://www.hyperlink.com>hyperlinked content</a>",
		"/// HTML tags within code spans remain unchanged secret `secrets/<secret>`",
		"/// Multiline hyperlinks should not be escaped <a",
		"/// href=https://en.wikipedia.org/wiki/Shebang_(Unix) class=\"external\">shebang lines</a>.",
		"/// Multiline placeholders should be escaped \\<a",
		"/// placeholder\\>",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &codec{}
	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func makeApiForRustFormatDocCommentsCrossLinks() *api.API {
	enumValue := &api.EnumValue{
		Name: "ENUM_VALUE",
		ID:   ".test.v1.SomeMessage.SomeEnum.ENUM_VALUE",
	}
	someEnum := &api.Enum{
		Name:    "SomeEnum",
		ID:      ".test.v1.SomeMessage.SomeEnum",
		Values:  []*api.EnumValue{enumValue},
		Package: "test.v1",
	}
	enumValue.Parent = someEnum
	response := &api.Field{
		Name:    "response",
		ID:      ".test.v1.SomeMessage.response",
		IsOneOf: true,
	}
	errorz := &api.Field{
		Name:    "error",
		ID:      ".test.v1.SomeMessage.error",
		IsOneOf: true,
	}
	typez := &api.Field{
		Name: "type",
		ID:   ".test.v1.SomeMessage.type",
	}
	someMessage := &api.Message{
		Name:    "SomeMessage",
		ID:      ".test.v1.SomeMessage",
		Package: "test.v1",
		Enums:   []*api.Enum{someEnum},
		Fields: []*api.Field{
			{Name: "unused"}, {Name: "field"}, response, errorz, typez,
		},
		OneOfs: []*api.OneOf{
			{
				Name:   "result",
				ID:     ".test.v1.SomeMessage.result",
				Fields: []*api.Field{response, errorz},
			},
		},
	}
	someService := &api.Service{
		Name:    "SomeService",
		ID:      ".test.v1.SomeService",
		Package: "test.v1",
		Methods: []*api.Method{
			{
				Name: "CreateFoo", ID: ".test.v1.SomeService.CreateFoo",
				PathInfo: &api.PathInfo{
					Bindings: []*api.PathBinding{
						{
							Verb: "GET",
							LegacyPathTemplate: []api.LegacyPathSegment{
								api.NewLiteralPathSegment("/v1/foo"),
							},
							PathTemplate: api.NewPathTemplate().
								WithLiteral("v1").
								WithLiteral("foo"),
						},
					},
				},
			},
			{Name: "CreateBar", ID: ".test.v1.SomeService.CreateBar"},
		},
	}
	yellyService := &api.Service{
		Name:    "YELL",
		ID:      ".test.v1.YELL",
		Package: "test.v1",
		Methods: []*api.Method{
			{
				Name: "CreateThing",
				ID:   ".test.v1.YELL.CreateThing",
				PathInfo: &api.PathInfo{
					Bindings: []*api.PathBinding{
						{
							Verb: "GET",
							LegacyPathTemplate: []api.LegacyPathSegment{
								api.NewLiteralPathSegment("/v1/thing"),
							},
							PathTemplate: api.NewPathTemplate().
								WithLiteral("v1").
								WithLiteral("thing"),
						},
					},
				},
			},
		},
	}
	a := api.NewTestAPI(
		[]*api.Message{someMessage},
		[]*api.Enum{someEnum},
		[]*api.Service{someService, yellyService})
	a.PackageName = "test.v1"
	a.State.MessageByID[".google.iam.v1.SetIamPolicyRequest"] = &api.Message{
		Name:    "SetIamPolicyRequest",
		Package: "google.iam.v1",
		ID:      ".google.iam.v1.SetIamPolicyRequest",
	}
	a.State.ServiceByID[".google.iam.v1.Iampolicy"] = &api.Service{
		Name:    "Iampolicy",
		Package: "google.iam.v1",
		ID:      ".google.iam.v1.Iampolicy",
	}
	return a
}

func TestFormatDocCommentsUrls(t *testing.T) {
	input := `
blah blah https://cloud.google.com foo bar
[link](https://example1.com)
<https://example2.com>
<https://example3.com>.
https://example4.com.
https://example5.com https://cloud.google.com something else.
[link definition]: https://example6.com/
not a definition: https://example7.com/
Quoted URL: "https://example8.com"
Trailing Slash https://example9.com/
http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html
http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
https://cloud.google.com/apis/design/design_patterns#integer_types
https://cloud.google.com/apis/design/design_patterns#integer_types.
Hyperlink: <a href="https://hyperlink.com">Content</a>`
	want := []string{
		"/// blah blah <https://cloud.google.com> foo bar",
		"/// [link](https://example1.com)",
		"/// <https://example2.com>",
		"/// <https://example3.com>.",
		"/// <https://example4.com>.",
		"/// <https://example5.com> <https://cloud.google.com> something else.",
		"/// [link definition]: https://example6.com/",
		"/// not a definition: <https://example7.com/>",
		"/// Quoted URL: `https://example8.com`",
		"/// Trailing Slash <https://example9.com/>",
		"/// <http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html>",
		"/// <http://www.unicode.org/reports/tr35/#Unicode_locale_identifier>.",
		"/// <https://cloud.google.com/apis/design/design_patterns#integer_types>",
		"/// <https://cloud.google.com/apis/design/design_patterns#integer_types>.",
		"/// Hyperlink: <a href=\"https://hyperlink.com\">Content</a>",
	}

	wkt := &packagez{
		name:        "wkt",
		packageName: "google-cloud-wkt",
		path:        "src/wkt",
	}
	iam := &packagez{
		name:        "iam_v1",
		packageName: "gcp-sdk-iam-v1",
		path:        "src/generated/iam/v1",
	}
	c := &codec{
		modulePath: "model",
		packageMapping: map[string]*packagez{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex model.API instance. Create it
	// in a separate function to make this more readable.
	model := makeApiForRustFormatDocCommentsCrossLinks()
	loadWellKnownTypes(model.State)

	got := c.formatDocComments(input, "test-only-ID", model.State, []string{})
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestMessageNames(t *testing.T) {
	r := sample.Replication()
	a := sample.Automatic()
	model := api.NewTestAPI([]*api.Message{r, a}, []*api.Enum{}, []*api.Service{})
	model.PackageName = "google.cloud.secretmanager.v1"

	c := createRustCodec()
	for _, test := range []struct {
		m    *api.Message
		want string
	}{
		{
			m:    r,
			want: "crate::model::Replication",
		},
		{
			m:    a,
			want: "crate::model::replication::Automatic",
		},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := fullyQualifiedMessageName(test.m, c.modulePath, model.PackageName, c.packageMapping); got != test.want {
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
	c := createRustCodec()
	for _, test := range []struct {
		enum                 *api.Enum
		wantEnum, wantFQEnum string
	}{
		{nested, "State", "crate::model::secret_version::State"},
		{non_nested, "Code", "crate::model::Code"},
	} {
		if got := enumName(test.enum); got != test.wantEnum {
			t.Errorf("enumName(%q) = %q; want = %s", test.enum.Name, got, test.wantEnum)
		}
		if got := fullyQualifiedEnumName(test.enum, c.modulePath, model.PackageName, c.packageMapping); got != test.wantFQEnum {
			t.Errorf("fullyQualifiedEnumName(%q) = %q; want = %s", test.enum.Name, got, test.wantFQEnum)
		}
	}
}

func TestEnumValueVariantName(t *testing.T) {
	testEnum := &api.Enum{
		Name:    "EnumName",
		ID:      ".test.EnumName",
		Package: "test",
		Values: []*api.EnumValue{
			{Number: 0, Name: "ENUM_NAME_UNSPECIFIED"},
			{Number: 2, Name: "ENUM_NAME_1"},
			{Number: 3, Name: "ENUM_NAME_A"},
			{Number: 4, Name: "ENUM_NAME_PARTIAL"},
			{Number: 5, Name: "ENUM_NAME_GREEN"},
		},
	}

	networkingEnum := &api.Enum{
		Name: "InstancePrivateIpv6GoogleAccess",
		ID:   ".test.InstancePrivateIpv6GoogleAccess",
		Values: []*api.EnumValue{
			{Number: 0, Name: "INSTANCE_PRIVATE_IPV6_GOOGLE_ACCESS_UNSPECIFIED"},
			{Number: 1, Name: "INHERIT_FROM_SUBNETWORK"},
		},
	}

	validationEnum := &api.Enum{
		Name: "Utf8Validation",
		ID:   ".test.Utf8Validation",
		Values: []*api.EnumValue{
			{Number: 0, Name: "UTF8_VALIDATION_UNKNOWN"},
			{Number: 1, Name: "VERIFY"},
		},
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{testEnum, networkingEnum, validationEnum}, []*api.Service{})
	model.PackageName = "test"
	var got []string
	for _, value := range testEnum.Values {
		got = append(got, enumValueVariantName(value))
	}
	want := []string{"Unspecified", "EnumName1", "A", "Partial", "Green"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in enum variant names (-want, +got):\n%s", diff)
	}

	got = []string{}
	for _, value := range networkingEnum.Values {
		got = append(got, enumValueVariantName(value))
	}
	want = []string{"Unspecified", "InheritFromSubnetwork"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in enum variant names (-want, +got):\n%s", diff)
	}

	got = []string{}
	for _, value := range validationEnum.Values {
		got = append(got, enumValueVariantName(value))
	}
	want = []string{"Unknown", "Verify"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in enum variant names (-want, +got):\n%s", diff)
	}
}

func TestPathFmt(t *testing.T) {
	for _, test := range []struct {
		want     string
		template *api.PathTemplate
	}{
		{
			"/v1/fixed",
			api.NewPathTemplate().
				WithLiteral("v1").
				WithLiteral("fixed"),
		},
		{
			"/v1/{}",
			api.NewPathTemplate().
				WithLiteral("v1").
				WithVariableNamed("parent"),
		},
		{
			"/v1/{}",
			api.NewPathTemplate().
				WithLiteral("v1").
				WithVariable(api.NewPathVariable("parent").
					WithLiteral("projects").
					WithMatch().
					WithLiteral("locations").
					WithMatch()),
		},
		{
			"/v1/{}:action",
			api.NewPathTemplate().
				WithLiteral("v1").
				WithVariableNamed("parent").
				WithVerb("action"),
		},
		{
			"/v1/projects/{}/locations/{}/secrets/{}:action",
			api.NewPathTemplate().
				WithLiteral("v1").
				WithLiteral("projects").
				WithVariableNamed("project").
				WithLiteral("locations").
				WithVariableNamed("location").
				WithLiteral("secrets").
				WithVariableNamed("secret").
				WithVerb("action"),
		},
	} {
		got := httpPathFmt(test.template)
		if test.want != got {
			t.Errorf("mismatched path fmt for %v\nwant=%s\n got=%s", test.template, test.want, got)
		}
	}

}

func TestPathArgs(t *testing.T) {
	subMessage := &api.Message{
		Name: "Body",
		ID:   ".test.Body",
		Fields: []*api.Field{
			{Name: "a", Typez: api.STRING_TYPE},
			{Name: "b", Typez: api.STRING_TYPE, Optional: true},
			{Name: "c", Typez: api.ENUM_TYPE},
			{Name: "d", Typez: api.ENUM_TYPE, Optional: true},
		},
	}
	message := &api.Message{
		Name: "CreateResourceRequest",
		ID:   ".test.CreateResourceRequest",
		Fields: []*api.Field{
			{Name: "v", Typez: api.STRING_TYPE},
			{Name: "w", Typez: api.STRING_TYPE, Optional: true},
			{Name: "x", Typez: api.ENUM_TYPE},
			{Name: "y", Typez: api.ENUM_TYPE, Optional: true},
			{Name: "z", Typez: api.MESSAGE_TYPE, TypezID: ".test.Body", Optional: true},
		},
	}
	method := &api.Method{
		Name:        "CreateResource",
		InputTypeID: ".test.CreateResourceRequest",
	}
	service := &api.Service{
		Name:    "TestService",
		ID:      ".test.Service",
		Methods: []*api.Method{method},
	}
	model := api.NewTestAPI([]*api.Message{subMessage, message}, []*api.Enum{}, []*api.Service{service})

	for _, test := range []struct {
		want     []pathArg
		pathInfo *api.PathInfo
	}{
		{
			nil,
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{},
				},
			},
		},
		{
			[]pathArg{{Name: "v", Accessor: ".v", CheckForEmpty: true}},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("v"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name:          "w",
					Accessor:      `.w.as_ref().ok_or_else(|| gaxi::path_parameter::missing("w"))?`,
					CheckForEmpty: true,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("w"),
						},
					},
				},
			},
		},
		{
			[]pathArg{{Name: "x", Accessor: `.x`}},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("x"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name:     "y",
					Accessor: `.y.as_ref().ok_or_else(|| gaxi::path_parameter::missing("y"))?`,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("y"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name:          "z.a",
					Accessor:      `.z.as_ref().ok_or_else(|| gaxi::path_parameter::missing("z"))?.a`,
					CheckForEmpty: true,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("z.a"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name: "z.b",
					Accessor: `.z.as_ref().ok_or_else(|| gaxi::path_parameter::missing("z"))?` +
						`.b.as_ref().ok_or_else(|| gaxi::path_parameter::missing("b"))?`,
					CheckForEmpty: true,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("z.b"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name:     "z.c",
					Accessor: `.z.as_ref().ok_or_else(|| gaxi::path_parameter::missing("z"))?.c`,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("z.c"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name: "z.d",
					Accessor: `.z.as_ref().ok_or_else(|| gaxi::path_parameter::missing("z"))?` +
						`.d.as_ref().ok_or_else(|| gaxi::path_parameter::missing("d"))?`,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("z.d"),
						},
					},
				},
			},
		},
		{
			[]pathArg{
				{
					Name:          "v",
					Accessor:      ".v",
					CheckForEmpty: true,
				},
				{
					Name:          "w",
					Accessor:      `.w.as_ref().ok_or_else(|| gaxi::path_parameter::missing("w"))?`,
					CheckForEmpty: true,
				},
			},
			&api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						LegacyPathTemplate: []api.LegacyPathSegment{
							api.NewLiteralPathSegment("v1"),
							api.NewFieldPathPathSegment("v"),
							api.NewFieldPathPathSegment("w"),
						},
					},
				},
			},
		},
	} {
		// Modify the method to match the test case.
		method.PathInfo = test.pathInfo
		got := httpPathArgs(test.pathInfo, method, model.State)
		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Errorf("mismatched path info args (-want, +got):\n%s", diff)
		}
	}
}
