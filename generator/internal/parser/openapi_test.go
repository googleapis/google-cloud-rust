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
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/types/known/apipb"
)

func TestOpenAPI_AllOf(t *testing.T) {
	// A message with AllOf and its transitive closure of dependent messages.
	const messageWithAllOf = `
      "Automatic": {
        "description": "A replication policy that replicates the Secret payload without any restrictions.",
        "type": "object",
        "properties": {
          "customerManagedEncryption": {
            "description": "Optional. The customer-managed encryption configuration of the Secret.",
            "allOf": [{
              "$ref": "#/components/schemas/CustomerManagedEncryption"
            }]
          }
        }
      },
      "CustomerManagedEncryption": {
        "description": "Configuration for encrypting secret payloads using customer-managed\nencryption keys (CMEK).",
        "type": "object",
        "properties": {
          "kmsKeyName": {
            "description": "Required. The resource name of the Cloud KMS CryptoKey used to encrypt secret payloads.",
            "type": "string"
          }
        },
        "required": [
          "kmsKeyName"
        ]
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithAllOf + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	want := sample.Automatic()
	want.Package = ""
	message := test.State.MessageByID[want.ID]
	if message == nil {
		t.Errorf("missing message in MessageByID index")
		return
	}
	checkMessage(t, message, want)
}

func TestOpenAPI_BasicTypes(t *testing.T) {
	// A message with basic types.
	const messageWithBasicTypes = `
      "Fake": {
        "description": "A test message.",
        "type": "object",
        "properties": {
          "fBool":      { "type": "boolean" },
          "fInt64":     { "type": "integer", "format": "int64" },
          "fInt32":     { "type": "integer", "format": "int32" },
          "fUInt32":    { "type": "integer", "format": "int32", "minimum": 0 },
          "fFloat":     { "type": "number", "format": "float" },
          "fDouble":    { "type": "number", "format": "double" },
          "fString":    { "type": "string" },
          "fOptional":  { "type": "string" },
          "fSInt64":    { "type": "string", "format": "int64" },
          "fSUInt64":   { "type": "string", "format": "int64", "minimum": 0 },
          "fDuration":  { "type": "string", "format": "google-duration" },
          "fTimestamp": { "type": "string", "format": "date-time" },
          "fFieldMask": { "type": "string", "format": "google-fieldmask" },
          "fBytes":     { "type": "string", "format": "byte" }
        },
        "required": [
            "fBool", "fInt64", "fInt32", "fUInt32",
            "fFloat", "fDouble",
            "fString",
            "fSInt64", "fSUInt64",
            "fDuration", "fTimestamp", "fFieldMask", "fBytes"
        ]
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithBasicTypes + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}

	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	message := test.State.MessageByID["..Fake"]
	if message == nil {
		t.Errorf("missing message in MessageByID index")
		return
	}
	checkMessage(t, message, &api.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Name:     "fBool",
				JSONName: "fBool",
				Typez:    api.BOOL_TYPE,
				TypezID:  "bool",
			},
			{
				Name:     "fInt64",
				JSONName: "fInt64",
				Typez:    api.INT64_TYPE,
				TypezID:  "int64",
			},
			{
				Name:     "fInt32",
				JSONName: "fInt32",
				Typez:    api.INT32_TYPE,
				TypezID:  "int32",
			},
			{
				Name:     "fUInt32",
				JSONName: "fUInt32",
				Typez:    api.UINT32_TYPE,
				TypezID:  "uint32",
			},
			{
				Name:     "fFloat",
				JSONName: "fFloat",
				Typez:    api.FLOAT_TYPE,
				TypezID:  "float",
			},
			{
				Name:     "fDouble",
				JSONName: "fDouble",
				Typez:    api.DOUBLE_TYPE,
				TypezID:  "double",
			},
			{
				Name:     "fString",
				JSONName: "fString",
				Typez:    api.STRING_TYPE,
				TypezID:  "string",
			},
			{
				Name:     "fOptional",
				JSONName: "fOptional",
				Typez:    api.STRING_TYPE,
				TypezID:  "string",
				Optional: true},
			{
				Name:     "fSInt64",
				JSONName: "fSInt64",
				Typez:    api.INT64_TYPE,
				TypezID:  "int64",
			},
			{
				Name:     "fSUInt64",
				JSONName: "fSUInt64",
				Typez:    api.UINT64_TYPE,
				TypezID:  "uint64",
			},
			{
				Name:     "fDuration",
				JSONName: "fDuration",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Duration",
				Optional: true,
			},
			{
				Name:     "fTimestamp",
				JSONName: "fTimestamp",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
			},
			{
				Name:     "fFieldMask",
				JSONName: "fFieldMask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
			{
				Name:     "fBytes",
				JSONName: "fBytes",
				Typez:    api.BYTES_TYPE,
				TypezID:  "bytes",
			},
		},
	})
}

func TestOpenAPI_ArrayTypes(t *testing.T) {
	// A message with basic types.
	const messageWithBasicTypes = `
      "Fake": {
        "description": "A test message.",
        "type": "object",
        "properties": {
          "fBool":      { "type": "array", "items": { "type": "boolean" }},
          "fInt64":     { "type": "array", "items": { "type": "integer", "format": "int64" }},
          "fInt32":     { "type": "array", "items": { "type": "integer", "format": "int32" }},
          "fUInt32":    { "type": "array", "items": { "type": "integer", "format": "int32", "minimum": 0 }},
          "fString":    { "type": "array", "items": { "type": "string" }},
          "fSInt64":    { "type": "array", "items": { "type": "string", "format": "int64" }},
          "fSUInt64":   { "type": "array", "items": { "type": "string", "format": "int64", "minimum": 0 }},
          "fDuration":  { "type": "array", "items": { "type": "string", "format": "google-duration" }},
          "fTimestamp": { "type": "array", "items": { "type": "string", "format": "date-time" }},
          "fFieldMask": { "type": "array", "items": { "type": "string", "format": "google-fieldmask" }},
          "fBytes":     { "type": "array", "items": { "type": "string", "format": "byte" }},
        }
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithBasicTypes + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	message := test.State.MessageByID["..Fake"]
	if message == nil {
		t.Errorf("missing message in MessageByID index")
		return
	}
	checkMessage(t, message, &api.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Repeated: true,
				Name:     "fBool",
				JSONName: "fBool",
				Typez:    api.BOOL_TYPE,
				TypezID:  "bool"},
			{
				Repeated: true,
				Name:     "fInt64",
				JSONName: "fInt64",
				Typez:    api.INT64_TYPE,
				TypezID:  "int64"},
			{
				Repeated: true,
				Name:     "fInt32",
				JSONName: "fInt32",
				Typez:    api.INT32_TYPE,
				TypezID:  "int32"},
			{
				Repeated: true,
				Name:     "fUInt32",
				JSONName: "fUInt32",
				Typez:    api.UINT32_TYPE,
				TypezID:  "uint32"},
			{
				Repeated: true,
				Name:     "fString",
				JSONName: "fString",
				Typez:    api.STRING_TYPE,
				TypezID:  "string"},
			{
				Repeated: true,
				Name:     "fSInt64",
				JSONName: "fSInt64",
				Typez:    api.INT64_TYPE,
				TypezID:  "int64"},
			{
				Repeated: true,
				Name:     "fSUInt64",
				JSONName: "fSUInt64",
				Typez:    api.UINT64_TYPE,
				TypezID:  "uint64"},
			{
				Repeated: true,
				Name:     "fDuration",
				JSONName: "fDuration",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Duration",
			},
			{
				Repeated: true,
				Name:     "fTimestamp",
				JSONName: "fTimestamp",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
			},
			{
				Repeated: true,
				Name:     "fFieldMask",
				JSONName: "fFieldMask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
			},
			{
				Repeated: true,
				Name:     "fBytes",
				JSONName: "fBytes",
				Typez:    api.BYTES_TYPE,
				TypezID:  "bytes",
			},
		},
	})
}

func TestOpenAPI_SimpleObject(t *testing.T) {
	const messageWithBasicTypes = `
      "Fake": {
        "description": "A test message.",
        "type": "object",
        "properties": {
          "fObject"     : { "type": "object", "description": "An object field.", "allOf": [{ "$ref": "#/components/schemas/Foo" }] },
          "fObjectArray": { "type": "array",  "description": "An object array field.", "items": [{ "$ref": "#/components/schemas/Bar" }] }
        }
      },
      "Foo": {
        "description": "Must have a Foo.",
        "type": "object",
        "properties": {}
      },
      "Bar": {
        "description": "Must have a Bar.",
        "type": "object",
        "properties": {}
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithBasicTypes + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	checkMessage(t, test.Messages[0], &api.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Name:          "fObject",
				JSONName:      "fObject",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "..Foo",
				Documentation: "An object field.",
				Optional:      true,
			},
			{
				Name:          "fObjectArray",
				JSONName:      "fObjectArray",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "..Bar",
				Documentation: "An object array field.",
				Optional:      false,
				Repeated:      true,
			},
		},
	})
}

func TestOpenAPI_Any(t *testing.T) {
	// A message with basic types.
	const messageWithBasicTypes = `
      "Fake": {
        "description": "A test message.",
        "type": "object",
        "properties": {
          "fMap":       { "type": "object", "additionalProperties": { "description": "Test Only." }}
        }
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithBasicTypes + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, test.Messages[0], &api.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{Name: "fMap", JSONName: "fMap", Typez: api.MESSAGE_TYPE, TypezID: ".google.protobuf.Any", Optional: true},
		},
	})
}

func TestOpenAPI_MapString(t *testing.T) {
	// A message with basic types.
	const messageWithBasicTypes = `
      "Fake": {
        "description": "A test message.",
        "type": "object",
        "properties": {
          "fMap":     { "type": "object", "additionalProperties": { "type": "string" }},
          "fMapS32":  { "type": "object", "additionalProperties": { "type": "string", "format": "int32" }},
          "fMapS64":  { "type": "object", "additionalProperties": { "type": "string", "format": "int64" }}
        }
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithBasicTypes + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatal(err)
	}

	checkMessage(t, test.Messages[0], &api.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Name:     "fMap",
				JSONName: "fMap",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "$map<string, string>",
				Map:      true,
			},
			{
				Name:     "fMapS32",
				JSONName: "fMapS32",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "$map<string, int32>",
				Map:      true,
			},
			{
				Name:     "fMapS64",
				JSONName: "fMapS64",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "$map<string, int64>",
				Map:      true,
			},
		},
	})
}

func TestOpenAPI_MapInteger(t *testing.T) {
	// A message with basic types.
	const messageWithBasicTypes = `
      "Fake": {
        "description": "A test message.",
        "type": "object",
        "properties": {
          "fMapI32": { "type": "object", "additionalProperties": { "type": "integer", "format": "int32" }},
          "fMapI64": { "type": "object", "additionalProperties": { "type": "integer", "format": "int64" }}
        }
      },
`
	contents := []byte(openAPISingleMessagePreamble + messageWithBasicTypes + openAPISingleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, test.Messages[0], &api.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*api.Field{
			{
				Name:     "fMapI32",
				JSONName: "fMapI32",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "$map<string, int32>",
				Optional: false,
				Map:      true,
			},
			{
				Name:     "fMapI64",
				JSONName: "fMapI64",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "$map<string, int64>",
				Optional: false,
				Map:      true,
			},
		},
	})
}

func TestOpenAPI_MakeAPI(t *testing.T) {
	contents, err := os.ReadFile("../../testdata/openapi/secretmanager_openapi_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	location := test.State.MessageByID["..Location"]
	if location == nil {
		t.Errorf("missing message (Location) in MessageByID index")
		return
	}
	checkMessage(t, location, &api.Message{
		Documentation: "A resource that represents a Google Cloud location.",
		Name:          "Location",
		ID:            "..Location",
		Fields: []*api.Field{
			{
				Name:          "name",
				JSONName:      "name",
				Documentation: "Resource name for the location, which may vary between implementations." + "\nFor example: `\"projects/example-project/locations/us-east1\"`",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "locationId",
				JSONName:      "locationId",
				Documentation: "The canonical id for this location. For example: `\"us-east1\"`.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "displayName",
				JSONName:      "displayName",
				Documentation: `The friendly name for this location, typically a nearby city name.` + "\n" + `For example, "Tokyo".`,
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "labels",
				JSONName:      "labels",
				Documentation: "Cross-service attributes for the location. For example\n\n    {\"cloud.googleapis.com/region\": \"us-east1\"}",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "$map<string, string>",
				Optional:      false,
				Map:           true,
			},
			{
				Name:          "metadata",
				JSONName:      "metadata",
				Documentation: `Service-specific metadata. For example the available capacity at the given` + "\n" + `location.`,
				Typez:         api.MESSAGE_TYPE,
				TypezID:       ".google.protobuf.Any",
				Optional:      true,
			},
		},
	})

	listLocationsResponse := test.State.MessageByID["..ListLocationsResponse"]
	if listLocationsResponse == nil {
		t.Errorf("missing message (ListLocationsResponse) in MessageByID index")
		return
	}
	checkMessage(t, listLocationsResponse, &api.Message{
		Documentation: "The response message for Locations.ListLocations.",
		Name:          "ListLocationsResponse",
		ID:            "..ListLocationsResponse",
		Fields: []*api.Field{
			{
				Name:          "locations",
				JSONName:      "locations",
				Documentation: "A list of locations that matches the specified filter in the request.",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "..Location",
				Repeated:      true,
			},
			{
				Name:          "nextPageToken",
				JSONName:      "nextPageToken",
				Documentation: "The standard List next-page token.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
		},
		Pagination: &api.PaginationInfo{
			NextPageToken: &api.Field{
				Name:          "nextPageToken",
				JSONName:      "nextPageToken",
				Documentation: "The standard List next-page token.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			PageableItem: &api.Field{
				Name:          "locations",
				JSONName:      "locations",
				Documentation: "A list of locations that matches the specified filter in the request.",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "..Location",
				Repeated:      true,
			},
		},
	})

	// This is a synthetic message, the OpenAPI spec does not contain requests
	// messages for messages without a body.
	listLocationsRequest, ok := test.State.MessageByID["..ListLocationsRequest"]
	if !ok {
		t.Errorf("missing message (ListLocationsRequest) in MessageByID index")
		return
	}
	checkMessage(t, listLocationsRequest, &api.Message{
		Name:          "ListLocationsRequest",
		ID:            "..ListLocationsRequest",
		Documentation: "The request message for ListLocations.",
		Fields: []*api.Field{
			{
				Name:          "project",
				JSONName:      "project",
				Documentation: "The `{project}` component of the target path.\n\nThe full target path will be in the form `/v1/projects/{project}/locations`.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
			{
				Name:     "filter",
				JSONName: "filter",
				Documentation: "A filter to narrow down results to a preferred subset." +
					"\nThe filtering language accepts strings like `\"displayName=tokyo" +
					"\"`, and\nis documented in more detail in [AIP-160](https://google" +
					".aip.dev/160).",
				Typez:     api.STRING_TYPE,
				TypezID:   "string",
				Optional:  true,
				Synthetic: true,
			},
			{
				Name:          "pageSize",
				JSONName:      "pageSize",
				Documentation: "The maximum number of results to return.\nIf not set, the service selects a default.",
				Typez:         api.INT32_TYPE,
				TypezID:       "int32",
				Optional:      true,
				Synthetic:     true,
			},
			{
				Name:          "pageToken",
				JSONName:      "pageToken",
				Documentation: "A page token received from the `next_page_token` field in the response.\nSend that page token to receive the subsequent page.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
				Synthetic:     true,
			},
		},
	})

	// This message has a weirdly named field that gets tricky to serialize.
	sp := sample.SecretPayload()
	got, ok := test.State.MessageByID[sp.ID]
	if !ok {
		t.Errorf("missing message (SecretPayload) in MessageByID index")
		return
	}
	checkMessage(t, got, sp)

	service, ok := test.State.ServiceByID["..Service"]
	if !ok {
		t.Errorf("missing service (Service) in ServiceByID index")
		return
	}

	wantService := sample.Service()
	wantService.Package = ""
	wantService.Name = "Service"
	wantService.ID = "..Service"
	if diff := cmp.Diff(wantService, service, cmpopts.IgnoreFields(api.Service{}, "Methods")); diff != "" {
		t.Errorf("mismatched service attributes (-want, +got):\n%s", diff)
	}

	checkMethod(t, service, "ListLocations", &api.Method{
		Name:          "ListLocations",
		ID:            "..Service.ListLocations",
		Documentation: "Lists information about the supported locations for this service.",
		InputTypeID:   "..ListLocationsRequest",
		OutputTypeID:  "..ListLocationsResponse",
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: "GET",
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewLiteralPathSegment("projects"),
						api.NewFieldPathPathSegment("project"),
						api.NewLiteralPathSegment("locations"),
					},
					QueryParameters: map[string]bool{
						"filter":    true,
						"pageSize":  true,
						"pageToken": true,
					},
				},
			},
		},
		Pagination: &api.Field{
			Name:          "pageToken",
			JSONName:      "pageToken",
			Documentation: "A page token received from the `next_page_token` field in the response.\nSend that page token to receive the subsequent page.",
			Typez:         api.STRING_TYPE,
			TypezID:       "string",
			Optional:      true,
			Synthetic:     true,
		},
	})

	cs := sample.MethodCreate()
	checkMethod(t, service, cs.Name, cs)

	asv := sample.MethodAddSecretVersion()
	checkMethod(t, service, asv.Name, asv)
}

func TestOpenAPI_MakeApiWithServiceConfig(t *testing.T) {
	contents, err := os.ReadFile("../../testdata/openapi/secretmanager_openapi_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	got, err := makeAPIForOpenAPI(sample.ServiceConfig(), model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}
	want := sample.API()
	if diff := cmp.Diff(got, want, cmpopts.IgnoreFields(api.API{}, "Services", "Messages", "Enums", "State")); diff != "" {
		t.Errorf("mismatched API attributes (-want, +got):\n%s", diff)
	}

}

func TestOpenAPI_MakeApiServiceConfigOverridesDescription(t *testing.T) {
	contents, err := os.ReadFile("../../testdata/openapi/secretmanager_openapi_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	serviceConfig := sample.ServiceConfig()
	serviceConfig.Documentation.Summary = "Test Only - Override Description."
	want := sample.API()
	want.Description = serviceConfig.Documentation.Summary
	got, err := makeAPIForOpenAPI(serviceConfig, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}
	if diff := cmp.Diff(got, want, cmpopts.IgnoreFields(api.API{}, "Services", "Messages", "Enums", "State")); diff != "" {
		t.Errorf("mismatched API attributes (-want, +got):\n%s", diff)
	}

}

func TestOpenAPI_SyntheticMessageWithExistingRequest(t *testing.T) {
	contents, err := os.ReadFile("../../testdata/openapi/secretmanager_openapi_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	// This message has a weirdly named field that gets tricky to serialize.
	id := "..SetIamPolicyRequest"
	setIamPolicyRequest, ok := test.State.MessageByID["..SetIamPolicyRequest"]
	if !ok {
		t.Errorf("missing message (%s) in MessageByID index", id)
		return
	}
	checkMessage(t, setIamPolicyRequest, &api.Message{
		Name:          "SetIamPolicyRequest",
		ID:            "..SetIamPolicyRequest",
		Documentation: "Request message for `SetIamPolicy` method.",
		Fields: []*api.Field{
			{
				Name:          "policy",
				JSONName:      "policy",
				Documentation: "REQUIRED: The complete policy to be applied to the `resource`. The size of\nthe policy is limited to a few 10s of KB. An empty policy is a\nvalid policy but certain Google Cloud services (such as Projects)\nmight reject them.",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "..Policy",
				Optional:      true,
			},
			{
				Name:          "updateMask",
				JSONName:      "updateMask",
				Documentation: "OPTIONAL: A FieldMask specifying which fields of the policy to modify. Only\nthe fields in the mask will be modified. If no mask is provided, the\nfollowing default mask is used:\n\n`paths: \"bindings, etag\"`",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       ".google.protobuf.FieldMask",
				Optional:      true,
			},
			{
				Name:          "project",
				JSONName:      "project",
				Documentation: "The `{project}` component of the target path.\n\nThe full target path will be in the form `/v1/projects/{project}/secrets/{secret}:setIamPolicy`.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
			{
				Name:          "secret",
				JSONName:      "secret",
				Documentation: "The `{secret}` component of the target path.\n\nThe full target path will be in the form `/v1/projects/{project}/secrets/{secret}:setIamPolicy`.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
			{
				Name:          "location",
				JSONName:      "location",
				Documentation: "The `{location}` component of the target path.\n\nThe full target path will be in the form `/v1/projects/{project}/locations/{location}/secrets/{secret}:setIamPolicy`.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
		},
	})
}

func TestOpenAPI_Pagination(t *testing.T) {
	contents, err := os.ReadFile("testdata/pagination_openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	service, ok := test.State.ServiceByID["..Service"]
	if !ok {
		t.Errorf("missing service (Service) in ServiceByID index")
		return
	}
	checkService(t, service, &api.Service{
		Name: "Service",
		ID:   "..Service",
		Methods: []*api.Method{
			{
				Name:         "ListFoos",
				ID:           "..Service.ListFoos",
				InputTypeID:  "..ListFoosRequest",
				OutputTypeID: "..ListFoosResponse",
				PathInfo: &api.PathInfo{
					Bindings: []*api.PathBinding{
						{
							Verb: "GET",
							LegacyPathTemplate: []api.LegacyPathSegment{
								api.NewLiteralPathSegment("v1"),
								api.NewLiteralPathSegment("projects"),
								api.NewFieldPathPathSegment("project"),
								api.NewLiteralPathSegment("foos"),
							},
							QueryParameters: map[string]bool{"pageSize": true, "pageToken": true},
						},
					},
				},
				Pagination: &api.Field{
					Name:          "pageToken",
					JSONName:      "pageToken",
					Documentation: "The `{pageToken}` component of the target path.\n\nThe full target path will be in the form `/v1/projects/{project}/foos`.",
					Typez:         api.STRING_TYPE,
					TypezID:       "string",
					Optional:      true,
					Synthetic:     true,
				},
			},
		},
	})
	resp, ok := test.State.MessageByID["..ListFoosResponse"]
	if !ok {
		t.Errorf("missing message (ListFoosResponse) in MessageByID index")
		return
	}
	checkMessage(t, resp, &api.Message{
		Name: "ListFoosResponse",
		ID:   "..ListFoosResponse",
		Fields: []*api.Field{
			{
				Name:     "nextPageToken",
				Typez:    9,
				TypezID:  "string",
				JSONName: "nextPageToken",
				Optional: true,
			},
			{
				Name:     "secrets",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Foo",
				JSONName: "secrets",
				Repeated: true,
			},
		},
		Pagination: &api.PaginationInfo{
			NextPageToken: &api.Field{
				Name:     "nextPageToken",
				Typez:    9,
				TypezID:  "string",
				JSONName: "nextPageToken",
				Optional: true,
			},
			PageableItem: &api.Field{
				Name:     "secrets",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Foo",
				JSONName: "secrets",
				Repeated: true,
			},
		},
	})
}

func TestOpenAPI_AutoPopulated(t *testing.T) {
	var serviceConfig = &serviceconfig.Service{
		Name:  "test",
		Title: "Test API",
		Documentation: &serviceconfig.Documentation{
			Summary:  "Used for testing generation.",
			Overview: "Test Overview",
		},
		Apis: []*apipb.Api{
			{
				Name: "test.TestService",
			},
		},
		Publishing: &annotations.Publishing{
			MethodSettings: []*annotations.MethodSettings{
				{
					Selector: ".test.TestService.CreateFoo",
					AutoPopulatedFields: []string{
						"requestId",
						"requestIdExplicitlyNotRequired",
						"notRequestIdRequired",
						"notRequestIdMissingFormat",
					},
				},
			},
		},
	}

	contents, err := os.ReadFile("testdata/auto_populated_openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(serviceConfig, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	message, ok := test.State.MessageByID[".test.CreateFooRequest"]
	if !ok {
		t.Fatalf("Cannot find message %s in API State", ".test.CreateFooRequest")
	}
	checkMessage(t, message, &api.Message{
		Name:          "CreateFooRequest",
		ID:            ".test.CreateFooRequest",
		Package:       "test",
		Documentation: "The request message for CreateFoo.",
		Fields: []*api.Field{
			{
				Name:          "project",
				JSONName:      "project",
				Documentation: "The `{project}` component of the target path.\n\nThe full target path will be in the form `/v1/projects/{project}/foos`.",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
			{
				Name:          "fooId",
				JSONName:      "fooId",
				Documentation: "Test-only Description",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
			{
				Name:          "requestId",
				JSONName:      "requestId",
				Documentation: "Test-only Description",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Optional:      true,
				AutoPopulated: true,
			},
			{
				Name:          "requestIdExplicitlyNotRequired",
				JSONName:      "requestIdExplicitlyNotRequired",
				Documentation: "Test-only Description",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				Synthetic:     true,
				Optional:      true,
				AutoPopulated: true,
			},
			{
				Name:          "notRequestIdRequired",
				Documentation: "Test-only Description",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				JSONName:      "notRequestIdRequired",
				Synthetic:     true,
				Behavior:      []api.FieldBehavior{api.FIELD_BEHAVIOR_REQUIRED},
			},
			{
				Name:          "notRequestIdMissingFormat",
				Documentation: "Test-only Description",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				JSONName:      "notRequestIdMissingFormat",
				Synthetic:     true,
				Optional:      true,
			},
			{
				Name:          "notRequestIdMissingServiceConfig",
				Documentation: "Test-only Description",
				Typez:         api.STRING_TYPE,
				TypezID:       "string",
				JSONName:      "notRequestIdMissingServiceConfig",
				Synthetic:     true,
				Optional:      true,
			},
		},
	})
}

func TestOpenAPI_Deprecated(t *testing.T) {
	contents, err := os.ReadFile("testdata/deprecated_openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	test, err := makeAPIForOpenAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	service, ok := test.State.ServiceByID["..Service"]
	if !ok {
		t.Errorf("cannot find service %s in model", "..Service.ListFoos")
		return
	}
	checkMethod(t, service, "RpcA", &api.Method{
		Name:         "RpcA",
		ID:           "..Service.RpcA",
		InputTypeID:  "..RpcARequest",
		OutputTypeID: "..Response",
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: "GET",
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewLiteralPathSegment("projects"),
						api.NewFieldPathPathSegment("project"),
						api.NewLiteralPathSegment("rpc"),
						api.NewLiteralPathSegment("a"),
					},
					QueryParameters: map[string]bool{"filter": true},
				},
			},
		},
	})

	checkMethod(t, service, "RpcB", &api.Method{
		Name:         "RpcB",
		ID:           "..Service.RpcB",
		Deprecated:   true,
		InputTypeID:  "..RpcBRequest",
		OutputTypeID: "..Response",
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: "GET",
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewLiteralPathSegment("projects"),
						api.NewFieldPathPathSegment("project"),
						api.NewLiteralPathSegment("rpc"),
						api.NewLiteralPathSegment("b"),
					},
					QueryParameters: map[string]bool{},
				},
			},
		},
	})

	response, ok := test.State.MessageByID["..Response"]
	if !ok {
		t.Errorf("cannot find message %s", "..Response")
		return
	}
	checkMessage(t, response, &api.Message{
		Name: "Response",
		ID:   "..Response",
		Fields: []*api.Field{
			{
				Name:     "name",
				Typez:    api.STRING_TYPE,
				TypezID:  "string",
				JSONName: "name",
				Optional: true,
			},
			{
				Name:       "other",
				Typez:      api.STRING_TYPE,
				TypezID:    "string",
				JSONName:   "other",
				Deprecated: true,
				Optional:   true,
			},
		},
	})

	deprecatedMessage, ok := test.State.MessageByID["..DeprecatedMessage"]
	if !ok {
		t.Errorf("cannot find message %s", "..DeprecatedMessage")
		return
	}
	checkMessage(t, deprecatedMessage, &api.Message{
		Name:       "DeprecatedMessage",
		ID:         "..DeprecatedMessage",
		Deprecated: true,
		Fields: []*api.Field{
			{
				Name:     "name",
				Typez:    api.STRING_TYPE,
				TypezID:  "string",
				JSONName: "name",
				Optional: true,
			},
		},
	})
}

const openAPISingleMessagePreamble = `
{
  "openapi": "3.0.3",
  "info": {
    "title": "Secret Manager API",
    "description": "Stores sensitive data such as API keys, passwords, and certificates. Provides convenience while improving security.",
    "version": "v1"
  },
  "servers": [
    {
      "url": "https://secretmanager.googleapis.com",
      "description": "Global Endpoint"
    }
  ],
  "components": {
    "schemas": {
`

const openAPISingleMessageTrailer = `
    },
  },
  "externalDocs": {
    "description": "Find more info here.",
    "url": "https://cloud.google.com/secret-manager/"
  }
}
`
