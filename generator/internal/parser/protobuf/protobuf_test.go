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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/types/known/apipb"
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

	api := MakeAPI(serviceConfig, newTestCodeGeneratorRequest(t, "scalar.proto"))
	if api.Name != "secretmanager" {
		t.Errorf("want = %q; got = %q", "secretmanager", api.Name)
	}
	if api.Title != serviceConfig.Title {
		t.Errorf("want = %q; got = %q", serviceConfig.Title, api.Name)
	}
	if diff := cmp.Diff(api.Description, serviceConfig.Documentation.Summary); diff != "" {
		t.Errorf("description mismatch (-want, +got):\n%s", diff)
	}
}

func TestScalar(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "scalar.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
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
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "scalar_array.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
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
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "scalar_optional.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API", "Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Package:       "test",
		ID:            ".test.Fake",
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

func TestSkipExternalMessages(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "with_import.proto"))

	// Both `ImportedMessage` and `LocalMessage` should be in the index:
	_, ok := api.State.MessageByID[".away.ImportedMessage"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".away.ImportedMessage")
	}
	message, ok := api.State.MessageByID[".test.LocalMessage"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.LocalMessage")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "LocalMessage",
		Package:       "test",
		ID:            ".test.LocalMessage",
		Documentation: "This is a local message, it should be generated.",
		Fields: []*genclient.Field{
			{
				Name:          "payload",
				JSONName:      "payload",
				ID:            ".test.LocalMessage.payload",
				Documentation: "This field uses an imported message.",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       ".away.ImportedMessage",
				Optional:      true,
			},
			{
				Name:          "value",
				JSONName:      "value",
				ID:            ".test.LocalMessage.value",
				Documentation: "This field uses an imported enum.",
				Typez:         genclient.ENUM_TYPE,
				TypezID:       ".away.ImportedEnum",
				Optional:      false,
			},
		},
	})
	// Only `LocalMessage` should be found in the messages list:
	for _, msg := range api.Messages {
		if msg.ID == ".test.ImportedMessage" {
			t.Errorf("imported messages should not be in message list %v", msg)
		}
	}
}

func TestSkipExternaEnums(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "with_import.proto"))

	// Both `ImportedEnum` and `LocalEnum` should be in the index:
	_, ok := api.State.EnumByID[".away.ImportedEnum"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".away.ImportedEnum")
	}
	enum, ok := api.State.EnumByID[".test.LocalEnum"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".test.LocalEnum")
	}
	checkEnum(t, *enum, genclient.Enum{
		Name:          "LocalEnum",
		Package:       "test",
		Documentation: "This is a local enum, it should be generated.",
		Values: []*genclient.EnumValue{
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
	for _, msg := range api.Messages {
		if msg.ID == ".test.ImportedMessage" {
			t.Errorf("imported messages should not be in message list %v", msg)
		}
	}
}

func TestComments(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "comments.proto"))

	message, ok := api.State.MessageByID[".test.Request"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Request")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Request",
		Package:       "test",
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

	message, ok = api.State.MessageByID[".test.Response.Nested"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Response.nested")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Nested",
		Package:       "test",
		ID:            ".test.Response.Nested",
		Documentation: "A nested message.\n\n- Item 1\n  Item 1 continued",
		Fields: []*genclient.Field{
			{
				Name:          "path",
				Documentation: "Field in a nested message.\n\n* Bullet 1\n  Bullet 1 continued\n* Bullet 2\n  Bullet 2 continued",
				JSONName:      "path",
				ID:            ".test.Response.Nested.path",
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
		Package:       "test",
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
		Package:       "test",
		Documentation: "A service.\n\nWith a longer service description.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*genclient.Method{
			{
				Name:          "Create",
				ID:            ".test.Service.Create",
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
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "oneofs.proto"))
	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Request")
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		Package:       "test",
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
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "object_fields.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:    "Fake",
		Package: "test",
		ID:      ".test.Fake",
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

func TestWellKnownTypeFields(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "wkt_fields.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:    "Fake",
		Package: "test",
		ID:      ".test.Fake",
		Fields: []*genclient.Field{
			{
				Name:     "field_mask",
				JSONName: "fieldMask",
				ID:       ".test.Fake.field_mask",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
			{
				Name:     "timestamp",
				JSONName: "timestamp",
				ID:       ".test.Fake.timestamp",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
			},
			{
				Name:     "any",
				JSONName: "any",
				ID:       ".test.Fake.any",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Any",
				Optional: true,
			},
			{
				Name:     "repeated_field_mask",
				JSONName: "repeatedFieldMask",
				ID:       ".test.Fake.repeated_field_mask",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Repeated: true,
			},
			{
				Name:     "repeated_timestamp",
				JSONName: "repeatedTimestamp",
				ID:       ".test.Fake.repeated_timestamp",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Repeated: true,
			},
			{
				Name:     "repeated_any",
				JSONName: "repeatedAny",
				ID:       ".test.Fake.repeated_any",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Any",
				Repeated: true,
			},
		},
	})
}

func TestMapFields(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "map_fields.proto"))

	message, ok := api.State.MessageByID[".test.Fake"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.Fake")
	}
	checkMessage(t, *message, genclient.Message{
		Name:    "Fake",
		Package: "test",
		ID:      ".test.Fake",
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
		Name:    "SingularMapEntry",
		Package: "test",
		ID:      ".test.Fake.SingularMapEntry",
		IsMap:   true,
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
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "test_service.proto"))

	service, ok := api.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	checkService(t, *service, genclient.Service{
		Name:          "TestService",
		Package:       "test",
		ID:            ".test.TestService",
		Documentation: "A service to unit test the protobuf translator.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*genclient.Method{
			{
				Name:          "GetFoo",
				ID:            ".test.TestService.GetFoo",
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
				ID:            ".test.TestService.CreateFoo",
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

func TestQueryParameters(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "query_parameters.proto"))

	service, ok := api.State.ServiceByID[".test.TestService"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".test.TestService")
	}
	checkService(t, *service, genclient.Service{
		Name:          "TestService",
		Package:       "test",
		ID:            ".test.TestService",
		Documentation: "A service to unit test the protobuf translator.",
		DefaultHost:   "test.googleapis.com",
		Methods: []*genclient.Method{
			{
				Name:          "CreateFoo",
				ID:            ".test.TestService.CreateFoo",
				Documentation: "Creates a new `Foo` resource. `Foo`s are containers for `Bar`s.\n\nShows how a `body: \"${field}\"` option works.",
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
					BodyFieldPath:   "bar",
				},
			},
			{
				Name:          "AddBar",
				ID:            ".test.TestService.AddBar",
				Documentation: "Add a Bar resource.\n\nShows how a `body: \"*\"` option works.",
				InputTypeID:   ".test.AddBarRequest",
				OutputTypeID:  ".test.Bar",
				PathInfo: &genclient.PathInfo{
					Verb: "POST",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewFieldPathPathSegment("parent"),
						genclient.NewVerbPathSegment("addFoo"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
			},
		},
	})
}

func TestEnum(t *testing.T) {
	api := MakeAPI(nil, newTestCodeGeneratorRequest(t, "enum.proto"))
	e, ok := api.State.EnumByID[".test.Code"]
	if !ok {
		t.Fatalf("Cannot find enum %s in API State", ".test.Code")
	}
	checkEnum(t, *e, genclient.Enum{
		Name:          "Code",
		Package:       "test",
		Documentation: "An enum.",
		Values: []*genclient.EnumValue{
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

func TestTrimLeadingSpacesInDocumentation(t *testing.T) {
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
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestLocationMixin(t *testing.T) {
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
	api := MakeAPI(serviceConfig, newTestCodeGeneratorRequest(t, "test_service.proto"))
	service, ok := api.State.ServiceByID[".google.cloud.location.Locations"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".google.cloud.location.Locations")
	}
	checkService(t, *service, genclient.Service{
		Documentation: "Manages location-related information with an API service.",
		DefaultHost:   "cloud.googleapis.com",
		Name:          "Locations",
		ID:            ".google.cloud.location.Locations",
		Package:       "google.cloud.location",
		Methods: []*genclient.Method{
			{
				Documentation: "GetLocation is an RPC method of Locations.",
				Name:          "GetLocation",
				ID:            ".google.cloud.location.Locations.GetLocation",
				InputTypeID:   ".google.cloud.location.GetLocationRequest",
				OutputTypeID:  ".google.cloud.location.Location",
				PathInfo: &genclient.PathInfo{
					Verb: "GET",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewFieldPathPathSegment("name"),
					},
					QueryParameters: map[string]bool{},
				},
			},
		},
	})
}

func TestIAMMixin(t *testing.T) {
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
	api := MakeAPI(serviceConfig, newTestCodeGeneratorRequest(t, "test_service.proto"))
	service, ok := api.State.ServiceByID[".google.iam.v1.IAMPolicy"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".google.iam.v1.IAMPolicy")
	}
	checkService(t, *service, genclient.Service{
		Documentation: "Manages Identity and Access Management (IAM) policies with an API service.",
		DefaultHost:   "iam-meta-api.googleapis.com",
		Name:          "IAMPolicy",
		ID:            ".google.iam.v1.IAMPolicy",
		Package:       "google.iam.v1",
		Methods: []*genclient.Method{
			{
				Documentation: "GetIamPolicy is an RPC method of IAMPolicy.",
				Name:          "GetIamPolicy",
				ID:            ".google.iam.v1.IAMPolicy.GetIamPolicy",
				InputTypeID:   ".google.iam.v1.GetIamPolicyRequest",
				OutputTypeID:  ".google.iam.v1.Policy",
				PathInfo: &genclient.PathInfo{
					Verb: "POST",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewFieldPathPathSegment("resource"),
						genclient.NewVerbPathSegment("getIamPolicy"),
					},
					QueryParameters: map[string]bool{},
					BodyFieldPath:   "*",
				},
			},
		},
	})
}

func TestOperationMixin(t *testing.T) {
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
	api := MakeAPI(serviceConfig, newTestCodeGeneratorRequest(t, "test_service.proto"))
	service, ok := api.State.ServiceByID[".google.longrunning.Operations"]
	if !ok {
		t.Fatalf("Cannot find service %s in API State", ".google.longrunning.Operations")
	}
	checkService(t, *service, genclient.Service{
		Documentation: "Manages long-running operations with an API service.",
		DefaultHost:   "longrunning.googleapis.com",
		Name:          "Operations",
		ID:            ".google.longrunning.Operations",
		Package:       "google.longrunning",
		Methods: []*genclient.Method{
			{
				Documentation: "Custom docs.",
				Name:          "GetOperation",
				ID:            ".google.longrunning.Operations.GetOperation",
				InputTypeID:   ".google.longrunning.GetOperationRequest",
				OutputTypeID:  ".google.longrunning.Operation",
				PathInfo: &genclient.PathInfo{
					Verb: "GET",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v2"),
						genclient.NewFieldPathPathSegment("name"),
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
	popts := genclient.ParserOptions{
		Source: filename,
		Options: map[string]string{
			"googleapis-root": "../../../testdata/googleapis",
			"test-root":       "testdata",
		},
	}
	request, err := NewCodeGeneratorRequest(popts)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func checkMessage(t *testing.T, got genclient.Message, want genclient.Message) {
	t.Helper()
	// Checking Parent, Messages, Fields, and OneOfs requires special handling.
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Message{}, "Fields", "OneOfs", "Parent", "Messages")); diff != "" {
		t.Errorf("message attributes mismatch (-want +got):\n%s", diff)
	}
	less := func(a, b *genclient.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Fields, got.Fields, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("field mismatch (-want, +got):\n%s", diff)
	}
	// Ignore parent because types are cyclic
	if diff := cmp.Diff(want.OneOfs, got.OneOfs, cmpopts.SortSlices(less), cmpopts.IgnoreFields(genclient.OneOf{}, "Parent")); diff != "" {
		t.Errorf("oneofs mismatch (-want, +got):\n%s", diff)
	}
}

func checkEnum(t *testing.T, got genclient.Enum, want genclient.Enum) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Enum{}, "Values", "Parent")); diff != "" {
		t.Errorf("Mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *genclient.EnumValue) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Values, got.Values, cmpopts.SortSlices(less), cmpopts.IgnoreFields(genclient.EnumValue{}, "Parent")); diff != "" {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}

func checkService(t *testing.T, got genclient.Service, want genclient.Service) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Service{}, "Methods")); diff != "" {
		t.Errorf("Mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *genclient.Method) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Methods, got.Methods, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}
