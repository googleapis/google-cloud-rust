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

package protobuf

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestInfo(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "secretmanager.googleapis.com",
		Title: "Secret Manager API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Stores sensitive data such as API keys, passwords, and certificates.\nProvides convenience while improving security.",
			Overview: "Secret Manager Overview",
		},
	}

	api := makeAPI(serviceConfig, newCodeGeneratorRequest(t, "scalar.proto"))
	if api.Name != "secretmanager" {
		t.Errorf("want = %q; got = %q", "secretmanager", api.Name)
	}
	if api.Title != serviceConfig.Title {
		t.Errorf("want = %q; got = %q", serviceConfig.Title, api.Name)
	}
	if diff := cmp.Diff(api.Description, serviceConfig.Documentation.Summary); len(diff) > 0 {
		t.Errorf("description mismatch (-want, +got):\n%s", diff)
	}
}

func TestScalar(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "scalar.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Documentation: "A singular field tag = 1",
				Name:          "f_double",
				JSONName:      "fDouble",
				ID:            ".test.Fake.f_double",
				Typez:         genclient.DOUBLE_TYPE,
			},
			{
				Documentation: "A singular field tag = 2",
				Name:          "f_float",
				JSONName:      "fFloat",
				ID:            ".test.Fake.f_float",
				Typez:         genclient.FLOAT_TYPE,
			},
			{
				Documentation: "A singular field tag = 3",
				Name:          "f_int64",
				JSONName:      "fInt64",
				ID:            ".test.Fake.f_int64",
				Typez:         genclient.INT64_TYPE,
			},
			{
				Documentation: "A singular field tag = 4",
				Name:          "f_uint64",
				JSONName:      "fUint64",
				ID:            ".test.Fake.f_uint64",
				Typez:         genclient.UINT64_TYPE,
			},
			{
				Documentation: "A singular field tag = 5",
				Name:          "f_int32",
				JSONName:      "fInt32",
				ID:            ".test.Fake.f_int32",
				Typez:         genclient.INT32_TYPE,
			},
			{
				Documentation: "A singular field tag = 6",
				Name:          "f_fixed64",
				JSONName:      "fFixed64",
				ID:            ".test.Fake.f_fixed64",
				Typez:         genclient.FIXED64_TYPE,
			},
			{
				Documentation: "A singular field tag = 7",
				Name:          "f_fixed32",
				JSONName:      "fFixed32",
				ID:            ".test.Fake.f_fixed32",
				Typez:         genclient.FIXED32_TYPE,
			},
			{
				Documentation: "A singular field tag = 8",
				Name:          "f_bool",
				JSONName:      "fBool",
				ID:            ".test.Fake.f_bool",
				Typez:         genclient.BOOL_TYPE,
			},
			{
				Documentation: "A singular field tag = 9",
				Name:          "f_string",
				JSONName:      "fString",
				ID:            ".test.Fake.f_string",
				Typez:         genclient.STRING_TYPE,
			},
			{
				Documentation: "A singular field tag = 12",
				Name:          "f_bytes",
				JSONName:      "fBytes",
				ID:            ".test.Fake.f_bytes",
				Typez:         genclient.BYTES_TYPE,
			},
			{
				Documentation: "A singular field tag = 13",
				Name:          "f_uint32",
				JSONName:      "fUint32",
				ID:            ".test.Fake.f_uint32",
				Typez:         genclient.UINT32_TYPE,
			},
			{
				Documentation: "A singular field tag = 15",
				Name:          "f_sfixed32",
				JSONName:      "fSfixed32",
				ID:            ".test.Fake.f_sfixed32",
				Typez:         genclient.SFIXED32_TYPE,
			},
			{
				Documentation: "A singular field tag = 16",
				Name:          "f_sfixed64",
				JSONName:      "fSfixed64",
				ID:            ".test.Fake.f_sfixed64",
				Typez:         genclient.SFIXED64_TYPE,
			},
			{
				Documentation: "A singular field tag = 17",
				Name:          "f_sint32",
				JSONName:      "fSint32",
				ID:            ".test.Fake.f_sint32",
				Typez:         genclient.SINT32_TYPE,
			},
			{
				Documentation: "A singular field tag = 18",
				Name:          "f_sint64",
				JSONName:      "fSint64",
				ID:            ".test.Fake.f_sint64",
				Typez:         genclient.SINT64_TYPE,
			},
		},
	})
}

func TestScalarArray(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "scalar_array.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 1",
				Name:          "f_double",
				JSONName:      "fDouble",
				ID:            ".test.Fake.f_double",
				Typez:         genclient.DOUBLE_TYPE,
			},
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 3",
				Name:          "f_int64",
				JSONName:      "fInt64",
				ID:            ".test.Fake.f_int64",
				Typez:         genclient.INT64_TYPE,
			},
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 9",
				Name:          "f_string",
				JSONName:      "fString",
				ID:            ".test.Fake.f_string",
				Typez:         genclient.STRING_TYPE,
			},
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 12",
				Name:          "f_bytes",
				JSONName:      "fBytes",
				ID:            ".test.Fake.f_bytes",
				Typez:         genclient.BYTES_TYPE,
			},
		},
	})
}

func TestScalarOptional(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "scalar_optional.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API", "Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Optional:      true,
				Documentation: "An optional field tag = 1",
				Name:          "f_double",
				JSONName:      "fDouble",
				ID:            ".test.Fake.f_double",
				Typez:         genclient.DOUBLE_TYPE,
			},
			{
				Optional:      true,
				Documentation: "An optional field tag = 3",
				Name:          "f_int64",
				JSONName:      "fInt64",
				ID:            ".test.Fake.f_int64",
				Typez:         genclient.INT64_TYPE,
			},
			{
				Optional:      true,
				Documentation: "An optional field tag = 9",
				Name:          "f_string",
				JSONName:      "fString",
				ID:            ".test.Fake.f_string",
				Typez:         genclient.STRING_TYPE,
			},
			{
				Optional:      true,
				Documentation: "An optional field tag = 12",
				Name:          "f_bytes",
				JSONName:      "fBytes",
				ID:            ".test.Fake.f_bytes",
				Typez:         genclient.BYTES_TYPE,
			},
		},
	})
}

func TestComments(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "comments.proto"))

	message, ok := api.State.MessageByID[".test.Request"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Request")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Request",
		ID:            ".test.Request",
		Documentation: "A test message.\n\nWith even more of a description.\nMaybe in more than one line.\nAnd some markdown:\n- An item\n  - A nested item\n- Another item",
		Fields: []*genclient.Field{
			{
				Name:          "parent",
				Documentation: "A field.\n\nWith a longer description.",
				JSONName:      "parent",
				ID:            ".test.Request.parent",
				Typez:         genclient.STRING_TYPE,
			},
		},
	})

	e, ok := api.State.EnumByID[".test.Response.Status"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".test.Response.Status")
	}
	checkEnum(t, *e, genclient.Enum{
		Name:          "Status",
		Documentation: "Some enum.\n\nLine 1.\nLine 2.",
		Values: []*genclient.EnumValue{
			{
				Name:          "NOT_READY",
				Documentation: "The first enum value description.\n\nValue Line 1.\nValue Line 2.",
				Number:        0,
			},
			{
				Name:          "READY",
				Documentation: "The second enum value description.",
				Number:        1,
			},
		},
	})

	service, ok := api.State.ServiceByID[".test.Service"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.Service")
	}
	checkService(t, *service, genclient.Service{
		Name:          "Service",
		ID:            ".test.Service",
		Documentation: "A service.\n\nWith a longer service description.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*genclient.Method{
			{
				Name:          "Create",
				Documentation: "Some RPC.\n\nIt does not do much.",
				InputTypeID:   ".test.Request",
				OutputTypeID:  ".test.Response",
				PathInfo: &genclient.PathInfo{
					Verb: "POST",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewFieldPathPathSegment("parent"),
						genclient.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
			},
		},
	})
}

func TestOneOfs(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "oneofs.proto"))
	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Request")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		ID:            ".test.Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Name:          "field_one",
				Documentation: "A string choice",
				JSONName:      "fieldOne",
				ID:            ".test.Fake.field_one",
				Typez:         genclient.STRING_TYPE,
				IsOneOf:       true,
			},
			{
				Documentation: "An int choice",
				Name:          "field_two",
				ID:            ".test.Fake.field_two",
				Typez:         genclient.INT64_TYPE,
				JSONName:      "fieldTwo",
				IsOneOf:       true,
			},
			{
				Documentation: "Optional is oneof in proto",
				Name:          "field_three",
				ID:            ".test.Fake.field_three",
				Typez:         genclient.STRING_TYPE,
				JSONName:      "fieldThree",
				Optional:      true,
			},
			{
				Documentation: "A normal field",
				Name:          "field_four",
				ID:            ".test.Fake.field_four",
				Typez:         genclient.INT32_TYPE,
				JSONName:      "fieldFour",
			},
		},
		OneOfs: []*genclient.OneOf{
			{
				Name: "choice",
				ID:   ".test.Fake.choice",
				Fields: []*genclient.Field{
					{
						Documentation: "A string choice",
						Name:          "field_one",
						ID:            ".test.Fake.field_one",
						Typez:         9,
						JSONName:      "fieldOne",
						IsOneOf:       true,
					},
					{
						Documentation: "An int choice",
						Name:          "field_two",
						ID:            ".test.Fake.field_two",
						Typez:         3,
						JSONName:      "fieldTwo",
						IsOneOf:       true,
					},
				},
			},
		},
	})
}

func TestObjectFields(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "object_fields.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name: "Fake",
		ID:   ".test.Fake",
		Fields: []*genclient.Field{
			{
				Repeated: false,
				Optional: true,
				Name:     "singular_object",
				JSONName: "singularObject",
				ID:       ".test.Fake.singular_object",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".test.Other",
			},
			{
				Repeated: true,
				Optional: false,
				Name:     "repeated_object",
				JSONName: "repeatedObject",
				ID:       ".test.Fake.repeated_object",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".test.Other",
			},
		},
	})
}

func TestMapFields(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "map_fields.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name: "Fake",
		ID:   ".test.Fake",
		Fields: []*genclient.Field{
			{
				Repeated: false,
				Optional: false,
				Name:     "singular_map",
				JSONName: "singularMap",
				ID:       ".test.Fake.singular_map",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".test.Fake.SingularMapEntry",
			},
		},
	})

	message, ok = api.State.MessageByID[".test.Fake.SingularMapEntry"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:  "SingularMapEntry",
		ID:    ".test.Fake.SingularMapEntry",
		IsMap: true,
		Fields: []*genclient.Field{
			{
				Repeated: false,
				Optional: false,
				Name:     "key",
				JSONName: "key",
				ID:       ".test.Fake.SingularMapEntry.key",
				Typez:    genclient.STRING_TYPE,
			},
			{
				Repeated: false,
				Optional: false,
				Name:     "value",
				JSONName: "value",
				ID:       ".test.Fake.SingularMapEntry.value",
				Typez:    genclient.INT32_TYPE,
			},
		},
	})
}

func TestService(t *testing.T) {
	api := makeAPI(nil, newCodeGeneratorRequest(t, "test_service.proto"))

	service, ok := api.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	checkService(t, *service, genclient.Service{
		Name:          "TestService",
		ID:            ".test.TestService",
		Documentation: "A service to unit test the protobuf translator.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*genclient.Method{
			{
				Name:          "GetFoo",
				Documentation: "Gets a Foo resource.",
				InputTypeID:   ".test.GetFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &genclient.PathInfo{
					Verb: "GET",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewFieldPathPathSegment("name"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "",
				},
			},
			{
				Name:          "CreateFoo",
				Documentation: "Creates a new Foo resource.",
				InputTypeID:   ".test.CreateFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &genclient.PathInfo{
					Verb: "POST",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewFieldPathPathSegment("parent"),
						genclient.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"foo_id": true},
					BodyFieldPath:   "foo",
				},
			},
		},
	})
}

func newCodeGeneratorRequest(t *testing.T, filename string) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	tempFile, err := os.CreateTemp("", "protoc-out-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	var stderr bytes.Buffer
	cmd := exec.Command("protoc",
		"--proto_path", "testdata",
		"--proto_path", "../../../../testdata/googleapis",
		"--include_imports",
		"--include_source_info",
		"--retain_options",
		"--descriptor_set_out", tempFile.Name(),
		filepath.Join("testdata", filename))
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		t.Logf("protoc error: %s", stderr.String())
		t.Fatal(err)
	}

	contents, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	descriptors := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(contents, descriptors); err != nil {
		t.Fatal(err)
	}
	var target *descriptorpb.FileDescriptorProto
	for _, pb := range descriptors.File {
		if *pb.Name == filename {
			target = pb
		}
	}
	request := &pluginpb.CodeGeneratorRequest{
		FileToGenerate:        []string{filename},
		ProtoFile:             []*descriptorpb.FileDescriptorProto{target},
		SourceFileDescriptors: descriptors.File,
		CompilerVersion:       newCompilerVersion(),
	}
	return request
}

func checkMessage(t *testing.T, got genclient.Message, want genclient.Message) {
	t.Helper()
	if want.Name != got.Name {
		t.Errorf("Mismatched message name, got=%q, want=%q", got.Name, want.Name)
	}
	if diff := cmp.Diff(want.Documentation, got.Documentation); len(diff) > 0 {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
	less := func(a, b *genclient.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Fields, got.Fields, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("field mismatch (-want, +got):\n%s", diff)
	}
	// Ignore parent because types are cyclic
	if diff := cmp.Diff(want.OneOfs, got.OneOfs, cmpopts.SortSlices(less), cmpopts.IgnoreFields(genclient.OneOf{}, "Parent")); len(diff) > 0 {
		t.Errorf("oneofs mismatch (-want, +got):\n%s", diff)
	}
}

func checkEnum(t *testing.T, got genclient.Enum, want genclient.Enum) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Enum{}, "Values", "Parent")); len(diff) > 0 {
		t.Errorf("Mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *genclient.EnumValue) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Values, got.Values, cmpopts.SortSlices(less), cmpopts.IgnoreFields(genclient.EnumValue{}, "Parent")); len(diff) > 0 {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}

func checkService(t *testing.T, got genclient.Service, want genclient.Service) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Service{}, "Methods")); len(diff) > 0 {
		t.Errorf("Mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *genclient.Method) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Methods, got.Methods, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}

func newCompilerVersion() *pluginpb.Version {
	var (
		i int32
		s = "test"
	)
	return &pluginpb.Version{
		Major:  &i,
		Minor:  &i,
		Patch:  &i,
		Suffix: &s,
	}
}
