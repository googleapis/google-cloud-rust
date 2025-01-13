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

package parser

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestProtobuf_Info(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "secretmanager.googleapis.com",
		Title: "Secret Manager API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Stores sensitive data such as API keys, passwords, and certificates.\nProvides convenience while improving security.",
			Overview: "Secret Manager Overview",
		},
	}

	test := makeAPIForProtobuf(serviceConfig, newTestCodeGeneratorRequest(t, "scalar.proto"))
	if test.Name != "secretmanager" {
		t.Errorf("want = %q; got = %q", "secretmanager", test.Name)
	}
	if test.Title != serviceConfig.Title {
		t.Errorf("want = %q; got = %q", serviceConfig.Title, test.Name)
	}
	if diff := cmp.Diff(test.Description, serviceConfig.Documentation.Summary); diff != "" {
		t.Errorf("description mismatch (-want, +got):\n%s", diff)
	}
}

func TestProtobuf_PartialInfo(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "secretmanager.googleapis.com",
		Title: "Secret Manager API",
	}

	got := makeAPIForProtobuf(serviceConfig, newTestCodeGeneratorRequest(t, "scalar.proto"))
	want := &api.API{
		Name:        "secretmanager",
		Title:       "Secret Manager API",
		Description: "",
	}
	if diff := cmp.Diff(got, want, cmpopts.IgnoreFields(api.API{}, "Services", "Messages", "Enums", "State")); diff != "" {
		t.Errorf("mismatched API attributes (-want, +got):\n%s", diff)
	}
}

func TestProtobuf_Scalar(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "scalar.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Documentation: "A singular field tag = 1",
				Name:          "f_double",
				JSONName:      "fDouble",
				ID:            ".test.Fake.f_double",
				Typez:         api.DOUBLE_TYPE,
			},
			{
				Documentation: "A singular field tag = 2",
				Name:          "f_float",
				JSONName:      "fFloat",
				ID:            ".test.Fake.f_float",
				Typez:         api.FLOAT_TYPE,
			},
			{
				Documentation: "A singular field tag = 3",
				Name:          "f_int64",
				JSONName:      "fInt64",
				ID:            ".test.Fake.f_int64",
				Typez:         api.INT64_TYPE,
			},
			{
				Documentation: "A singular field tag = 4",
				Name:          "f_uint64",
				JSONName:      "fUint64",
				ID:            ".test.Fake.f_uint64",
				Typez:         api.UINT64_TYPE,
			},
			{
				Documentation: "A singular field tag = 5",
				Name:          "f_int32",
				JSONName:      "fInt32",
				ID:            ".test.Fake.f_int32",
				Typez:         api.INT32_TYPE,
			},
			{
				Documentation: "A singular field tag = 6",
				Name:          "f_fixed64",
				JSONName:      "fFixed64",
				ID:            ".test.Fake.f_fixed64",
				Typez:         api.FIXED64_TYPE,
			},
			{
				Documentation: "A singular field tag = 7",
				Name:          "f_fixed32",
				JSONName:      "fFixed32",
				ID:            ".test.Fake.f_fixed32",
				Typez:         api.FIXED32_TYPE,
			},
			{
				Documentation: "A singular field tag = 8",
				Name:          "f_bool",
				JSONName:      "fBool",
				ID:            ".test.Fake.f_bool",
				Typez:         api.BOOL_TYPE,
			},
			{
				Documentation: "A singular field tag = 9",
				Name:          "f_string",
				JSONName:      "fString",
				ID:            ".test.Fake.f_string",
				Typez:         api.STRING_TYPE,
			},
			{
				Documentation: "A singular field tag = 12",
				Name:          "f_bytes",
				JSONName:      "fBytes",
				ID:            ".test.Fake.f_bytes",
				Typez:         api.BYTES_TYPE,
			},
			{
				Documentation: "A singular field tag = 13",
				Name:          "f_uint32",
				JSONName:      "fUint32",
				ID:            ".test.Fake.f_uint32",
				Typez:         api.UINT32_TYPE,
			},
			{
				Documentation: "A singular field tag = 15",
				Name:          "f_sfixed32",
				JSONName:      "fSfixed32",
				ID:            ".test.Fake.f_sfixed32",
				Typez:         api.SFIXED32_TYPE,
			},
			{
				Documentation: "A singular field tag = 16",
				Name:          "f_sfixed64",
				JSONName:      "fSfixed64",
				ID:            ".test.Fake.f_sfixed64",
				Typez:         api.SFIXED64_TYPE,
			},
			{
				Documentation: "A singular field tag = 17",
				Name:          "f_sint32",
				JSONName:      "fSint32",
				ID:            ".test.Fake.f_sint32",
				Typez:         api.SINT32_TYPE,
			},
			{
				Documentation: "A singular field tag = 18",
				Name:          "f_sint64",
				JSONName:      "fSint64",
				ID:            ".test.Fake.f_sint64",
				Typez:         api.SINT64_TYPE,
			},
		},
	})
}

func TestProtobuf_ScalarArray(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "scalar_array.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 1",
				Name:          "f_double",
				JSONName:      "fDouble",
				ID:            ".test.Fake.f_double",
				Typez:         api.DOUBLE_TYPE,
			},
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 3",
				Name:          "f_int64",
				JSONName:      "fInt64",
				ID:            ".test.Fake.f_int64",
				Typez:         api.INT64_TYPE,
			},
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 9",
				Name:          "f_string",
				JSONName:      "fString",
				ID:            ".test.Fake.f_string",
				Typez:         api.STRING_TYPE,
			},
			{
				Repeated:      true,
				Documentation: "A repeated field tag = 12",
				Name:          "f_bytes",
				JSONName:      "fBytes",
				ID:            ".test.Fake.f_bytes",
				Typez:         api.BYTES_TYPE,
			},
		},
	})
}

func TestProtobuf_ScalarOptional(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "scalar_optional.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API", "Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Optional:      true,
				Documentation: "An optional field tag = 1",
				Name:          "f_double",
				JSONName:      "fDouble",
				ID:            ".test.Fake.f_double",
				Typez:         api.DOUBLE_TYPE,
			},
			{
				Optional:      true,
				Documentation: "An optional field tag = 3",
				Name:          "f_int64",
				JSONName:      "fInt64",
				ID:            ".test.Fake.f_int64",
				Typez:         api.INT64_TYPE,
			},
			{
				Optional:      true,
				Documentation: "An optional field tag = 9",
				Name:          "f_string",
				JSONName:      "fString",
				ID:            ".test.Fake.f_string",
				Typez:         api.STRING_TYPE,
			},
			{
				Optional:      true,
				Documentation: "An optional field tag = 12",
				Name:          "f_bytes",
				JSONName:      "fBytes",
				ID:            ".test.Fake.f_bytes",
				Typez:         api.BYTES_TYPE,
			},
		},
	})
}

func TestProtobuf_SkipExternalMessages(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "with_import.proto"))
	// Both `ImportedMessage` and `LocalMessage` should be in the index:
	_, ok := test.State.MessageByID[".away.ImportedMessage"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".away.ImportedMessage")
	}
	message, ok := test.State.MessageByID[".test.LocalMessage"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.LocalMessage")
	}
	checkMessage(t, *message, api.Message{
		Name:          "LocalMessage",
		Package:       "test",
		ID:            ".test.LocalMessage",
		Documentation: "This is a local message, it should be generated.",
		Fields: []*api.Field{
			{
				Name:          "payload",
				JSONName:      "payload",
				ID:            ".test.LocalMessage.payload",
				Documentation: "This field uses an imported message.",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       ".away.ImportedMessage",
				Optional:      true,
			},
			{
				Name:          "value",
				JSONName:      "value",
				ID:            ".test.LocalMessage.value",
				Documentation: "This field uses an imported enum.",
				Typez:         api.ENUM_TYPE,
				TypezID:       ".away.ImportedEnum",
				Optional:      false,
			},
		},
	})
	// Only `LocalMessage` should be found in the messages list:
	for _, msg := range test.Messages {
		if msg.ID == ".test.ImportedMessage" {
			t.Errorf("imported messages should not be in message list %v", msg)
		}
	}
}

func TestProtobuf_SkipExternaEnums(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "with_import.proto"))
	// Both `ImportedEnum` and `LocalEnum` should be in the index:
	_, ok := test.State.EnumByID[".away.ImportedEnum"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".away.ImportedEnum")
	}
	enum, ok := test.State.EnumByID[".test.LocalEnum"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".test.LocalEnum")
	}
	checkEnum(t, *enum, api.Enum{
		Name:          "LocalEnum",
		Package:       "test",
		Documentation: "This is a local enum, it should be generated.",
		Values: []*api.EnumValue{
			{
				Name:   "RED",
				Number: 0,
			},
			{
				Name:   "WHITE",
				Number: 1,
			},
			{
				Name:   "BLUE",
				Number: 2,
			},
		},
	})
	// Only `LocalMessage` should be found in the messages list:
	for _, msg := range test.Messages {
		if msg.ID == ".test.ImportedMessage" {
			t.Errorf("imported messages should not be in message list %v", msg)
		}
	}
}

func TestProtobuf_Comments(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "comments.proto"))
	message, ok := test.State.MessageByID[".test.Request"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Request")
	}
	checkMessage(t, *message, api.Message{
		Name:          "Request",
		Package:       "test",
		ID:            ".test.Request",
		Documentation: "A test message.\n\nWith even more of a description.\nMaybe in more than one line.\nAnd some markdown:\n- An item\n  - A nested item\n- Another item",
		Fields: []*api.Field{
			{
				Name:          "parent",
				Documentation: "A field.\n\nWith a longer description.",
				JSONName:      "parent",
				ID:            ".test.Request.parent",
				Typez:         api.STRING_TYPE,
			},
		},
	})

	message, ok = test.State.MessageByID[".test.Response.Nested"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Response.nested")
	}
	checkMessage(t, *message, api.Message{
		Name:          "Nested",
		Package:       "test",
		ID:            ".test.Response.Nested",
		Documentation: "A nested message.\n\n- Item 1\n  Item 1 continued",
		Fields: []*api.Field{
			{
				Name:          "path",
				Documentation: "Field in a nested message.\n\n* Bullet 1\n  Bullet 1 continued\n* Bullet 2\n  Bullet 2 continued",
				JSONName:      "path",
				ID:            ".test.Response.Nested.path",
				Typez:         api.STRING_TYPE,
			},
		},
	})

	e, ok := test.State.EnumByID[".test.Response.Status"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".test.Response.Status")
	}
	checkEnum(t, *e, api.Enum{
		Name:          "Status",
		Package:       "test",
		Documentation: "Some enum.\n\nLine 1.\nLine 2.",
		Values: []*api.EnumValue{
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

	service, ok := test.State.ServiceByID[".test.Service"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.Service")
	}
	checkService(t, service, &api.Service{
		Name:          "Service",
		ID:            ".test.Service",
		Package:       "test",
		Documentation: "A service.\n\nWith a longer service description.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*api.Method{
			{
				Name:          "Create",
				ID:            ".test.Service.Create",
				Documentation: "Some RPC.\n\nIt does not do much.",
				InputTypeID:   ".test.Request",
				OutputTypeID:  ".test.Response",
				PathInfo: &api.PathInfo{
					Verb: "POST",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
			},
		},
	})
}

func TestProtobuf_OneOfs(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "oneofs.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Request")
	}
	checkMessage(t, *message, api.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Name:          "field_one",
				Documentation: "A string choice",
				JSONName:      "fieldOne",
				ID:            ".test.Fake.field_one",
				Typez:         api.STRING_TYPE,
				IsOneOf:       true,
			},
			{
				Documentation: "An int choice",
				Name:          "field_two",
				ID:            ".test.Fake.field_two",
				Typez:         api.INT64_TYPE,
				JSONName:      "fieldTwo",
				IsOneOf:       true,
			},
			{
				Documentation: "Optional is oneof in proto",
				Name:          "field_three",
				ID:            ".test.Fake.field_three",
				Typez:         api.STRING_TYPE,
				JSONName:      "fieldThree",
				Optional:      true,
			},
			{
				Documentation: "A normal field",
				Name:          "field_four",
				ID:            ".test.Fake.field_four",
				Typez:         api.INT32_TYPE,
				JSONName:      "fieldFour",
			},
		},
		OneOfs: []*api.OneOf{
			{
				Name: "choice",
				ID:   ".test.Fake.choice",
				Fields: []*api.Field{
					{
						Documentation: "A string choice",
						Name:          "field_one",
						ID:            ".test.Fake.field_one",
						Typez:         api.STRING_TYPE,
						JSONName:      "fieldOne",
						IsOneOf:       true,
					},
					{
						Documentation: "An int choice",
						Name:          "field_two",
						ID:            ".test.Fake.field_two",
						Typez:         api.INT64_TYPE,
						JSONName:      "fieldTwo",
						IsOneOf:       true,
					},
				},
			},
		},
	})
}

func TestProtobuf_ObjectFields(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "object_fields.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:    "Fake",
		Package: "test",
		ID:      ".test.Fake",
		Fields: []*api.Field{
			{
				Repeated: false,
				Optional: true,
				Name:     "singular_object",
				JSONName: "singularObject",
				ID:       ".test.Fake.singular_object",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".test.Other",
			},
			{
				Repeated: true,
				Optional: false,
				Name:     "repeated_object",
				JSONName: "repeatedObject",
				ID:       ".test.Fake.repeated_object",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".test.Other",
			},
		},
	})
}

func TestProtobuf_WellKnownTypeFields(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "wkt_fields.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:    "Fake",
		Package: "test",
		ID:      ".test.Fake",
		Fields: []*api.Field{
			{
				Name:     "field_mask",
				JSONName: "fieldMask",
				ID:       ".test.Fake.field_mask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
			{
				Name:     "timestamp",
				JSONName: "timestamp",
				ID:       ".test.Fake.timestamp",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
			},
			{
				Name:     "any",
				JSONName: "any",
				ID:       ".test.Fake.any",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Any",
				Optional: true,
			},
			{
				Name:     "repeated_field_mask",
				JSONName: "repeatedFieldMask",
				ID:       ".test.Fake.repeated_field_mask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Repeated: true,
			},
			{
				Name:     "repeated_timestamp",
				JSONName: "repeatedTimestamp",
				ID:       ".test.Fake.repeated_timestamp",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Repeated: true,
			},
			{
				Name:     "repeated_any",
				JSONName: "repeatedAny",
				ID:       ".test.Fake.repeated_any",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Any",
				Repeated: true,
			},
		},
	})
}

func TestProtobuf_MapFields(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "map_fields.proto"))
	message, ok := test.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:    "Fake",
		Package: "test",
		ID:      ".test.Fake",
		Fields: []*api.Field{
			{
				Repeated: false,
				Optional: false,
				Name:     "singular_map",
				JSONName: "singularMap",
				ID:       ".test.Fake.singular_map",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".test.Fake.SingularMapEntry",
			},
		},
	})

	message, ok = test.State.MessageByID[".test.Fake.SingularMapEntry"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, api.Message{
		Name:    "SingularMapEntry",
		Package: "test",
		ID:      ".test.Fake.SingularMapEntry",
		IsMap:   true,
		Fields: []*api.Field{
			{
				Repeated: false,
				Optional: false,
				Name:     "key",
				JSONName: "key",
				ID:       ".test.Fake.SingularMapEntry.key",
				Typez:    api.STRING_TYPE,
			},
			{
				Repeated: false,
				Optional: false,
				Name:     "value",
				JSONName: "value",
				ID:       ".test.Fake.SingularMapEntry.value",
				Typez:    api.INT32_TYPE,
			},
		},
	})
}

func TestProtobuf_Service(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "test_service.proto"))
	service, ok := test.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	checkService(t, service, &api.Service{
		Name:          "TestService",
		Package:       "test",
		ID:            ".test.TestService",
		Documentation: "A service to unit test the protobuf translator.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*api.Method{
			{
				Name:          "GetFoo",
				ID:            ".test.TestService.GetFoo",
				Documentation: "Gets a Foo resource.",
				InputTypeID:   ".test.GetFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("name"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "",
				},
			},
			{
				Name:          "CreateFoo",
				ID:            ".test.TestService.CreateFoo",
				Documentation: "Creates a new Foo resource.",
				InputTypeID:   ".test.CreateFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &api.PathInfo{
					Verb: "POST",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"foo_id": true},
					BodyFieldPath:   "foo",
				},
			},
			{
				Name:          "UploadFoos",
				ID:            ".test.TestService.UploadFoos",
				Documentation: "A client-side streaming RPC.",
				InputTypeID:   ".test.CreateFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &api.PathInfo{
					Verb:            "POST",
					PathTemplate:    []api.PathSegment{},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
				ClientSideStreaming: true,
			},
			{
				Name:          "DownloadFoos",
				ID:            ".test.TestService.DownloadFoos",
				Documentation: "A server-side streaming RPC.",
				InputTypeID:   ".test.GetFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("name"),
						api.NewVerbPathSegment("Download"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "",
				},
				ServerSideStreaming: true,
			},
			{
				Name:          "ChatLike",
				ID:            ".test.TestService.ChatLike",
				Documentation: "A bidi streaming RPC.",
				InputTypeID:   ".test.Foo",
				OutputTypeID:  ".test.Foo",
				PathInfo: &api.PathInfo{
					Verb:            "POST",
					PathTemplate:    []api.PathSegment{},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
				ClientSideStreaming: true,
				ServerSideStreaming: true,
			},
		},
	})
}

func TestProtobuf_QueryParameters(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "query_parameters.proto"))
	service, ok := test.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	checkService(t, service, &api.Service{
		Name:          "TestService",
		Package:       "test",
		ID:            ".test.TestService",
		Documentation: "A service to unit test the protobuf translator.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*api.Method{
			{
				Name:          "CreateFoo",
				ID:            ".test.TestService.CreateFoo",
				Documentation: "Creates a new `Foo` resource. `Foo`s are containers for `Bar`s.\n\nShows how a `body: \"${field}\"` option works.",
				InputTypeID:   ".test.CreateFooRequest",
				OutputTypeID:  ".test.Foo",
				PathInfo: &api.PathInfo{
					Verb: "POST",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"foo_id": true},
					BodyFieldPath:   "bar",
				},
			},
			{
				Name:          "AddBar",
				ID:            ".test.TestService.AddBar",
				Documentation: "Add a Bar resource.\n\nShows how a `body: \"*\"` option works.",
				InputTypeID:   ".test.AddBarRequest",
				OutputTypeID:  ".test.Bar",
				PathInfo: &api.PathInfo{
					Verb: "POST",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewVerbPathSegment("addFoo"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
			},
		},
	})
}

func TestProtobuf_Enum(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "enum.proto"))
	e, ok := test.State.EnumByID[".test.Code"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".test.Code")
	}
	checkEnum(t, *e, api.Enum{
		Name:          "Code",
		Package:       "test",
		Documentation: "An enum.",
		Values: []*api.EnumValue{
			{
				Name:          "OK",
				Documentation: "Not an error; returned on success.",
				Number:        0,
			},
			{
				Name:          "UNKNOWN",
				Documentation: "Unknown error.",
				Number:        1,
			},
		},
	})
}

func TestProtobuf_TrimLeadingSpacesInDocumentation(t *testing.T) {
	input := ` In this example, in proto field could take one of the following values:

 * full_name for a violation in the full_name value
 * email_addresses[1].email for a violation in the email field of the
   first email_addresses message
 * email_addresses[3].type[2] for a violation in the second type
   value in the third email_addresses message.)`

	want := `In this example, in proto field could take one of the following values:

* full_name for a violation in the full_name value
* email_addresses[1].email for a violation in the email field of the
  first email_addresses message
* email_addresses[3].type[2] for a violation in the second type
  value in the third email_addresses message.)`

	got := trimLeadingSpacesInDocumentation(input)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in trimLeadingSpacesInDocumentation (-want, +got)\n:%s", diff)
	}
}

func TestProtobuf_LocationMixin(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "test.googleapis.com",
		Title: "Test API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Used for testing generation.",
			Overview: "Test Overview",
		},
		Apis: []*apipb.Api{
			{
				Name: "google.cloud.location.Locations",
			},
			{
				Name: "test.googleapis.com.TestService",
			},
		},
		Http: &annotations.Http{
			Rules: []*annotations.HttpRule{
				{
					Selector: "google.cloud.location.Locations.GetLocation",
					Pattern: &annotations.HttpRule_Get{
						Get: "/v1/{name=projects/*/locations/*}",
					},
				},
			},
		},
	}
	test := makeAPIForProtobuf(serviceConfig, newTestCodeGeneratorRequest(t, "test_service.proto"))
	for _, service := range test.Services {
		if service.ID == ".google.cloud.location.Locations" {
			t.Fatalf("Mixin %s should not be in list of services to generate", service.ID)
		}
	}
	service, ok := test.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	if _, ok := test.State.MethodByID[".test.TestService.GetLocation"]; !ok {
		t.Fatal("Cannot find .test.TestService.GetLocation")
	}

	checkMethod(t, service, "GetLocation", &api.Method{
		Documentation: "Provides the [Locations][google.cloud.location.Locations] service functionality in this service.",
		Name:          "GetLocation",
		ID:            ".test.TestService.GetLocation",
		InputTypeID:   ".google.cloud.location.GetLocationRequest",
		OutputTypeID:  ".google.cloud.location.Location",
		PathInfo: &api.PathInfo{
			Verb: "GET",
			PathTemplate: []api.PathSegment{
				api.NewLiteralPathSegment("v1"),
				api.NewFieldPathPathSegment("name"),
			},
			QueryParameters: map[string]bool{},
		},
	})
}

func TestProtobuf_IAMMixin(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "test.googleapis.com",
		Title: "Test API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Used for testing generation.",
			Overview: "Test Overview",
		},
		Apis: []*apipb.Api{
			{
				Name: "google.iam.v1.IAMPolicy",
			},
			{
				Name: "test.googleapis.com.TestService",
			},
		},
		Http: &annotations.Http{
			Rules: []*annotations.HttpRule{
				{
					Selector: "google.iam.v1.IAMPolicy.GetIamPolicy",
					Pattern: &annotations.HttpRule_Post{
						Post: "/v1/{resource=services/*}:getIamPolicy",
					},
					Body: "*",
				},
			},
		},
	}
	test := makeAPIForProtobuf(serviceConfig, newTestCodeGeneratorRequest(t, "test_service.proto"))
	for _, service := range test.Services {
		if service.ID == ".google.iam.v1.IAMPolicy" {
			t.Fatalf("Mixin %s should not be in list of services to generate", service.ID)
		}
	}

	service, ok := test.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	if _, ok := test.State.MethodByID[".test.TestService.GetIamPolicy"]; !ok {
		t.Fatal("Cannot find .test.TestService.GetIamPolicy")
	}
	checkMethod(t, service, "GetIamPolicy", &api.Method{
		Documentation: "Provides the [IAMPolicy][google.iam.v1.IAMPolicy] service functionality in this service.",
		Name:          "GetIamPolicy",
		ID:            ".test.TestService.GetIamPolicy",
		InputTypeID:   ".google.iam.v1.GetIamPolicyRequest",
		OutputTypeID:  ".google.iam.v1.Policy",
		PathInfo: &api.PathInfo{
			Verb: "POST",
			PathTemplate: []api.PathSegment{
				api.NewLiteralPathSegment("v1"),
				api.NewFieldPathPathSegment("resource"),
				api.NewVerbPathSegment("getIamPolicy"),
			},
			QueryParameters: map[string]bool{},
			BodyFieldPath:   "*",
		},
	})
}

func TestProtobuf_Pagination(t *testing.T) {
	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "pagination.proto"))
	service, ok := test.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	checkService(t, service, &api.Service{
		Name:        "TestService",
		ID:          ".test.TestService",
		DefaultHost: "test.googleapis.com",
		Package:     "test",
		Methods: []*api.Method{
			{
				Name:         "ListFoo",
				ID:           ".test.TestService.ListFoo",
				InputTypeID:  ".test.ListFooRequest",
				OutputTypeID: ".test.ListFooResponse",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"page_size": true, "page_token": true},
				},
				IsPageable: true,
			},
			{
				Name:         "ListFooMissingNextPageToken",
				ID:           ".test.TestService.ListFooMissingNextPageToken",
				InputTypeID:  ".test.ListFooRequest",
				OutputTypeID: ".test.ListFooMissingNextPageTokenResponse",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"page_size": true, "page_token": true},
				},
			},
			{
				Name:         "ListFooMissingPageSize",
				ID:           ".test.TestService.ListFooMissingPageSize",
				InputTypeID:  ".test.ListFooMissingPageSizeRequest",
				OutputTypeID: ".test.ListFooResponse",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"page_token": true},
				},
			},
			{
				Name:         "ListFooMissingPageToken",
				ID:           ".test.TestService.ListFooMissingPageToken",
				InputTypeID:  ".test.ListFooMissingPageTokenRequest",
				OutputTypeID: ".test.ListFooResponse",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"page_size": true},
				},
			},
			{
				Name:         "ListFooMissingRepeatedItemToken",
				ID:           ".test.TestService.ListFooMissingRepeatedItemToken",
				InputTypeID:  ".test.ListFooRequest",
				OutputTypeID: ".test.ListFooMissingRepeatedItemResponse",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{"page_size": true, "page_token": true},
				},
			},
		},
	})

	resp, ok := test.State.MessageByID[".test.ListFooResponse"]
	if !ok {
		t.Errorf("missing message (ListFooResponse) in MessageByID index")
		return
	}
	checkMessage(t, *resp, api.Message{
		Name:               "ListFooResponse",
		ID:                 ".test.ListFooResponse",
		Package:            "test",
		IsPageableResponse: true,
		Fields: []*api.Field{
			{
				Name:     "next_page_token",
				ID:       ".test.ListFooResponse.next_page_token",
				Typez:    9,
				JSONName: "nextPageToken",
			},
			{
				Name:     "foos",
				ID:       ".test.ListFooResponse.foos",
				Typez:    11,
				TypezID:  ".test.Foo",
				JSONName: "foos",
				Repeated: true,
			},
			{
				Name:     "total_size",
				ID:       ".test.ListFooResponse.total_size",
				Typez:    5,
				JSONName: "totalSize",
			},
		},
		PageableItem: &api.Field{
			Name:     "foos",
			ID:       ".test.ListFooResponse.foos",
			Typez:    11,
			TypezID:  ".test.Foo",
			JSONName: "foos",
			Repeated: true,
		},
	})
}

func TestProtobuf_OperationMixin(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "test.googleapis.com",
		Title: "Test API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Used for testing generation.",
			Overview: "Test Overview",
			Rules: []*serviceconfig.DocumentationRule{
				{
					Selector:    "google.longrunning.Operations.GetOperation",
					Description: "Custom docs.",
				},
			},
		},
		Apis: []*apipb.Api{
			{
				Name: "google.longrunning.Operations",
			},
			{
				Name: "test.googleapis.com.TestService",
			},
		},
		Http: &annotations.Http{
			Rules: []*annotations.HttpRule{
				{
					Selector: "google.longrunning.Operations.GetOperation",
					Pattern: &annotations.HttpRule_Get{
						Get: "/v2/{name=operations/*}",
					},
					Body: "*",
				},
			},
		},
	}
	test := makeAPIForProtobuf(serviceConfig, newTestCodeGeneratorRequest(t, "test_service.proto"))
	for _, service := range test.Services {
		if service.ID == ".google.longrunning.Operations" {
			t.Fatalf("Mixin %s should not be in list of services to generate", service.ID)
		}
	}
	service, ok := test.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	if _, ok := test.State.MethodByID[".test.TestService.GetOperation"]; !ok {
		t.Fatal("Cannot find .test.TestService.GetOperation")
	}

	checkMethod(t, service, "GetOperation", &api.Method{
		Documentation: "Custom docs.",
		Name:          "GetOperation",
		ID:            ".test.TestService.GetOperation",
		InputTypeID:   ".google.longrunning.GetOperationRequest",
		OutputTypeID:  ".google.longrunning.Operation",
		PathInfo: &api.PathInfo{
			Verb: "GET",
			PathTemplate: []api.PathSegment{
				api.NewLiteralPathSegment("v2"),
				api.NewFieldPathPathSegment("name"),
			},
			QueryParameters: map[string]bool{},
			BodyFieldPath:   "*",
		},
	})
}

func TestProtobuf_OperationInfo(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "test.googleapis.com",
		Title: "Test API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Used for testing generation.",
			Overview: "Test Overview",
			Rules: []*serviceconfig.DocumentationRule{
				{
					Selector:    "google.longrunning.Operations.GetOperation",
					Description: "Custom docs.",
				},
			},
		},
		Apis: []*apipb.Api{
			{
				Name: "google.longrunning.Operations",
			},
			{
				Name: "test.googleapis.com.TestService",
			},
		},
		Http: &annotations.Http{
			Rules: []*annotations.HttpRule{
				{
					Selector: "google.longrunning.Operations.GetOperation",
					Pattern: &annotations.HttpRule_Get{
						Get: "/v2/{name=operations/*}",
					},
					Body: "*",
				},
			},
		},
	}
	test := makeAPIForProtobuf(serviceConfig, newTestCodeGeneratorRequest(t, "test_operation_info.proto"))
	service, ok := test.State.ServiceByID[".test.LroService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.LroService")
	}
	checkService(t, service, &api.Service{
		Documentation: "A service to unit test the protobuf translator.",
		DefaultHost:   "test.googleapis.com",
		Name:          "LroService",
		ID:            ".test.LroService",
		Package:       "test",
		Methods: []*api.Method{
			{
				Documentation: "Creates a new Foo resource.",
				Name:          "CreateFoo",
				ID:            ".test.LroService.CreateFoo",
				InputTypeID:   ".test.CreateFooRequest",
				OutputTypeID:  ".google.longrunning.Operation",
				PathInfo: &api.PathInfo{
					Verb: "POST",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "foo",
				},
				OperationInfo: &api.OperationInfo{
					MetadataTypeID: ".google.protobuf.Empty",
					ResponseTypeID: ".test.Foo",
				},
			},
			{
				Documentation: "Creates a new Foo resource.",
				Name:          "CreateFooWithProgress",
				ID:            ".test.LroService.CreateFooWithProgress",
				InputTypeID:   ".test.CreateFooRequest",
				OutputTypeID:  ".google.longrunning.Operation",
				PathInfo: &api.PathInfo{
					Verb: "POST",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("foos"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "foo",
				},
				OperationInfo: &api.OperationInfo{
					MetadataTypeID: ".test.CreateMetadata",
					ResponseTypeID: ".test.Foo",
				},
			},
			{
				Documentation: "Custom docs.",
				Name:          "GetOperation",
				ID:            ".test.LroService.GetOperation",
				InputTypeID:   ".google.longrunning.GetOperationRequest",
				OutputTypeID:  ".google.longrunning.Operation",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("v2"),
						api.NewFieldPathPathSegment("name"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
			},
		},
	})
}

func newTestCodeGeneratorRequest(t *testing.T, filename string) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	options := map[string]string{
		"googleapis-root":   "../../testdata/googleapis",
		"extra-protos-root": "testdata",
	}
	request, err := newCodeGeneratorRequest(filename, options)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
