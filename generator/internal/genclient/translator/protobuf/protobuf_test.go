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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bufbuild/protocompile"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestScalar(t *testing.T) {
	api := makeAPI(newCodeGeneratorRequest(t, "scalar.proto"))

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
	api := makeAPI(newCodeGeneratorRequest(t, "scalar_array.proto"))

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
	api := makeAPI(newCodeGeneratorRequest(t, "scalar_optional.proto"))

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
	api := makeAPI(newCodeGeneratorRequest(t, "comments.proto"))

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
		Methods: []*genclient.Method{
			{
				Name:          "Create",
				Documentation: "Some RPC.\n\nIt does not do much.",
				InputTypeID:   ".test.Request",
				OutputTypeID:  ".test.Response",
			},
		},
	})
}

func TestOneOfs(t *testing.T) {
	api := makeAPI(newCodeGeneratorRequest(t, "oneofs.proto"))
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

func newCodeGeneratorRequest(t *testing.T, filename string) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatal(err)
	}
	accessor := protocompile.SourceAccessorFromMap(map[string]string{
		filename: string(contents),
	})
	compiler := protocompile.Compiler{
		Resolver:       &protocompile.SourceResolver{Accessor: accessor},
		MaxParallelism: 1,
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	files, err := compiler.Compile(context.Background(), filename)
	if err != nil {
		t.Fatalf("error compiling proto %q", err)
	}
	if len(files) != 1 {
		t.Errorf("Expected exactly one output descriptor, got=%d", len(files))
	}
	descriptor := protodesc.ToFileDescriptorProto(files[0])
	return &pluginpb.CodeGeneratorRequest{
		FileToGenerate:        []string{filename},
		ProtoFile:             []*descriptorpb.FileDescriptorProto{descriptor},
		SourceFileDescriptors: []*descriptorpb.FileDescriptorProto{descriptor},
		CompilerVersion:       newCompilerVersion(),
	}
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
