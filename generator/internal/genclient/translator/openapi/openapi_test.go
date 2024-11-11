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

package openapi

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

func TestAllOf(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithAllOf + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	message := api.State.MessageByID["..Automatic"]
	if message == nil {
		t.Errorf("missing message in MessageByID index")
		return
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Automatic",
		ID:            "..Automatic",
		Documentation: "A replication policy that replicates the Secret payload without any restrictions.",
		Fields: []*genclient.Field{
			{
				Name:          "customerManagedEncryption",
				JSONName:      "customerManagedEncryption",
				Documentation: "Optional. The customer-managed encryption configuration of the Secret.",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "..CustomerManagedEncryption",
				Optional:      true,
			},
		},
	})
}

func TestBasicTypes(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithBasicTypes + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}

	api, err := makeAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	message := api.State.MessageByID["..Fake"]
	if message == nil {
		t.Errorf("missing message in MessageByID index")
		return
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Name:     "fBool",
				JSONName: "fBool",
				Typez:    genclient.BOOL_TYPE,
				TypezID:  "bool",
			},
			{
				Name:     "fInt64",
				JSONName: "fInt64",
				Typez:    genclient.INT64_TYPE,
				TypezID:  "int64",
			},
			{
				Name:     "fInt32",
				JSONName: "fInt32",
				Typez:    genclient.INT32_TYPE,
				TypezID:  "int32",
			},
			{
				Name:     "fUInt32",
				JSONName: "fUInt32",
				Typez:    genclient.UINT32_TYPE,
				TypezID:  "uint32",
			},
			{
				Name:     "fFloat",
				JSONName: "fFloat",
				Typez:    genclient.FLOAT_TYPE,
				TypezID:  "float",
			},
			{
				Name:     "fDouble",
				JSONName: "fDouble",
				Typez:    genclient.DOUBLE_TYPE,
				TypezID:  "double",
			},
			{
				Name:     "fString",
				JSONName: "fString",
				Typez:    genclient.STRING_TYPE,
				TypezID:  "string",
			},
			{
				Name:     "fOptional",
				JSONName: "fOptional",
				Typez:    genclient.STRING_TYPE,
				TypezID:  "string",
				Optional: true},
			{
				Name:     "fSInt64",
				JSONName: "fSInt64",
				Typez:    genclient.INT64_TYPE,
				TypezID:  "int64",
			},
			{
				Name:     "fSUInt64",
				JSONName: "fSUInt64",
				Typez:    genclient.UINT64_TYPE,
				TypezID:  "uint64",
			},
			{
				Name:     "fDuration",
				JSONName: "fDuration",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Duration",
				Optional: true,
			},
			{
				Name:     "fTimestamp",
				JSONName: "fTimestamp",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
			},
			{
				Name:     "fFieldMask",
				JSONName: "fFieldMask",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
			{
				Name:     "fBytes",
				JSONName: "fBytes",
				Typez:    genclient.BYTES_TYPE,
				TypezID:  "bytes",
			},
		},
	})
}

func TestArrayTypes(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithBasicTypes + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	message := api.State.MessageByID["..Fake"]
	if message == nil {
		t.Errorf("missing message in MessageByID index")
		return
	}
	checkMessage(t, *message, genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Repeated: true,
				Name:     "fBool",
				JSONName: "fBool",
				Typez:    genclient.BOOL_TYPE,
				TypezID:  "bool"},
			{
				Repeated: true,
				Name:     "fInt64",
				JSONName: "fInt64",
				Typez:    genclient.INT64_TYPE,
				TypezID:  "int64"},
			{
				Repeated: true,
				Name:     "fInt32",
				JSONName: "fInt32",
				Typez:    genclient.INT32_TYPE,
				TypezID:  "int32"},
			{
				Repeated: true,
				Name:     "fUInt32",
				JSONName: "fUInt32",
				Typez:    genclient.UINT32_TYPE,
				TypezID:  "uint32"},
			{
				Repeated: true,
				Name:     "fString",
				JSONName: "fString",
				Typez:    genclient.STRING_TYPE,
				TypezID:  "string"},
			{
				Repeated: true,
				Name:     "fSInt64",
				JSONName: "fSInt64",
				Typez:    genclient.INT64_TYPE,
				TypezID:  "int64"},
			{
				Repeated: true,
				Name:     "fSUInt64",
				JSONName: "fSUInt64",
				Typez:    genclient.UINT64_TYPE,
				TypezID:  "uint64"},
			{
				Repeated: true,
				Name:     "fDuration",
				JSONName: "fDuration",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Duration",
			},
			{
				Repeated: true,
				Name:     "fTimestamp",
				JSONName: "fTimestamp",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
			},
			{
				Repeated: true,
				Name:     "fFieldMask",
				JSONName: "fFieldMask",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
			},
			{
				Repeated: true,
				Name:     "fBytes",
				JSONName: "fBytes",
				Typez:    genclient.BYTES_TYPE,
				TypezID:  "bytes",
			},
		},
	})
}

func TestSimpleObject(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithBasicTypes + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Name:          "fObject",
				JSONName:      "fObject",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "..Foo",
				Documentation: "An object field.",
				Optional:      true,
			},
			{
				Name:          "fObjectArray",
				JSONName:      "fObjectArray",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "..Bar",
				Documentation: "An object array field.",
				Optional:      false,
				Repeated:      true,
			},
		},
	})
}

func TestAny(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithBasicTypes + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{Name: "fMap", JSONName: "fMap", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.Any", Optional: true},
		},
	})
}

func TestMapString(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithBasicTypes + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Fatal(err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Name:     "fMap",
				JSONName: "fMap",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "$map<string, string>",
			},
			{
				Name:     "fMapS32",
				JSONName: "fMapS32",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "$map<string, int32>",
			},
			{
				Name:     "fMapS64",
				JSONName: "fMapS64",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "$map<string, int64>",
			},
		},
	})
}

func TestMapInteger(t *testing.T) {
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
	contents := []byte(singleMessagePreamble + messageWithBasicTypes + singleMessageTrailer)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{
				Name:     "fMapI32",
				JSONName: "fMapI32",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "$map<string, int32>",
				Optional: false},
			{
				Name:     "fMapI64",
				JSONName: "fMapI64",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "$map<string, int64>",
				Optional: false},
		},
	})
}

func TestMakeAPI(t *testing.T) {
	contents := []byte(testDocument)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(nil, model)
	if err != nil {
		t.Fatalf("Error in makeAPI() %q", err)
	}

	location := api.State.MessageByID["..Location"]
	if location == nil {
		t.Errorf("missing message (Location) in MessageByID index")
		return
	}
	checkMessage(t, *location, genclient.Message{
		Documentation: "A resource that represents a Google Cloud location.",
		Name:          "Location",
		ID:            "..Location",
		Fields: []*genclient.Field{
			{
				Name:          "name",
				JSONName:      "name",
				Documentation: "Resource name for the location, which may vary between implementations.",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "locationId",
				JSONName:      "locationId",
				Documentation: `The canonical id for this location.`,
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "displayName",
				JSONName:      "displayName",
				Documentation: `The friendly name for this location, typically a nearby city name.`,
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "labels",
				JSONName:      "labels",
				Documentation: "Cross-service attributes for the location.",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "$map<string, string>",
				Optional:      false,
			},
			{
				Name:          "metadata",
				JSONName:      "metadata",
				Documentation: `Service-specific metadata. For example the available capacity at the given location.`,
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       ".google.protobuf.Any",
				Optional:      true,
			},
		},
	})

	listLocationsResponse := api.State.MessageByID["..ListLocationsResponse"]
	if listLocationsResponse == nil {
		t.Errorf("missing message (ListLocationsResponse) in MessageByID index")
		return
	}
	checkMessage(t, *listLocationsResponse, genclient.Message{
		Documentation: "The response message for Locations.ListLocations.",
		Name:          "ListLocationsResponse",
		ID:            "..ListLocationsResponse",
		Fields: []*genclient.Field{
			{
				Name:          "locations",
				JSONName:      "locations",
				Documentation: "A list of locations that matches the specified filter in the request.",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "..Location",
				Repeated:      true,
			},
			{
				Name:          "nextPageToken",
				JSONName:      "nextPageToken",
				Documentation: "The standard List next-page token.",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
		},
	})

	// This is a synthetic message, the OpenAPI spec does not contain requests
	// messages for messages without a body.
	listLocationsRequest, ok := api.State.MessageByID["..ListLocationsRequest"]
	if !ok {
		t.Errorf("missing message (ListLocationsRequest) in MessageByID index")
		return
	}
	checkMessage(t, *listLocationsRequest, genclient.Message{
		Name:          "ListLocationsRequest",
		ID:            "..ListLocationsRequest",
		Documentation: "The request message for ListLocations.",
		Fields: []*genclient.Field{
			{
				Name:          "project",
				JSONName:      "project",
				Documentation: "",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
			},
			{
				Name:          "filter",
				JSONName:      "filter",
				Documentation: "A filter to narrow down results to a preferred subset.",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "pageSize",
				JSONName:      "pageSize",
				Documentation: "The maximum number of results to return.",
				Typez:         genclient.INT32_TYPE,
				TypezID:       "int32",
				Optional:      true,
			},
			{
				Name:          "pageToken",
				JSONName:      "pageToken",
				Documentation: "A page token received from the `next_page_token` field in the response.\nSend that page token to receive the subsequent page.",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
		},
	})

	service, ok := api.State.ServiceByID["..Service"]
	if !ok {
		t.Errorf("missing service (Service) in ServiceByID index")
		return
	}
	checkService(t, *service, genclient.Service{
		Name:          "Service",
		ID:            "..Service",
		Documentation: "Stores sensitive data such as API keys, passwords, and certificates. Provides convenience while improving security.",
		DefaultHost:   "https://secretmanager.googleapis.com",
		Methods: []*genclient.Method{
			{
				Name:          "ListLocations",
				ID:            "ListLocations",
				Documentation: "Lists information about the supported locations for this service.",
				InputTypeID:   "..ListLocationsRequest",
				OutputTypeID:  "..ListLocationsResponse",
				PathInfo: &genclient.PathInfo{
					Verb: "GET",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewLiteralPathSegment("projects"),
						genclient.NewFieldPathPathSegment("project"),
						genclient.NewLiteralPathSegment("locations"),
					},
					QueryParameters: map[string]bool{
						"filter":    true,
						"pageSize":  true,
						"pageToken": true,
					},
				},
			},
			{
				Name:          "CreateSecret",
				ID:            "CreateSecret",
				Documentation: "Creates a new Secret containing no SecretVersions.",
				InputTypeID:   "..Secret",
				OutputTypeID:  "..Secret",
				PathInfo: &genclient.PathInfo{
					Verb:          "POST",
					BodyFieldPath: "*",
					PathTemplate: []genclient.PathSegment{
						genclient.NewLiteralPathSegment("v1"),
						genclient.NewLiteralPathSegment("projects"),
						genclient.NewFieldPathPathSegment("project"),
						genclient.NewLiteralPathSegment("secrets"),
					},
					QueryParameters: map[string]bool{
						"secretId": true,
					},
				},
			},
		},
	})
}

func checkService(t *testing.T, got genclient.Service, want genclient.Service) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Service{}, "Methods")); len(diff) > 0 {
		t.Errorf("Mismatched attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *genclient.Method) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Methods, got.Methods, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("field mismatch (-want, +got):\n%s", diff)
	}
}

func checkMessage(t *testing.T, got genclient.Message, want genclient.Message) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(genclient.Message{}, "Fields")); len(diff) > 0 {
		t.Errorf("Mismatched attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *genclient.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Fields, got.Fields, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("field mismatch (-want, +got):\n%s", diff)
	}
}

const singleMessagePreamble = `
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

const singleMessageTrailer = `
    },
  },
  "externalDocs": {
    "description": "Find more info here.",
    "url": "https://cloud.google.com/secret-manager/"
  }
}
`

// This is a subset of the secret manager OpenAPI v3 spec circa 2023-10.  It is
// just intended to drive some of the initial development and testing.
const testDocument = `
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
  "paths": {
    "/v1/projects/{project}/locations": {
      "parameters": [
        { "$ref": "#/components/parameters/alt"},
        { "$ref": "#/components/parameters/callback"},
        { "$ref": "#/components/parameters/prettyPrint"},
        { "$ref": "#/components/parameters/_.xgafv"}
      ],
      "get": {
        "tags": ["secretmanager"],
        "operationId": "ListLocations",
        "description": "Lists information about the supported locations for this service.",
        "security": [
          {
            "google_oauth_implicit": [
              "https://www.googleapis.com/auth/cloud-platform"
            ]
          },
          {
            "google_oauth_code": [
              "https://www.googleapis.com/auth/cloud-platform"
            ]
          },
          {
            "bearer_auth": []
          }
        ],
        "parameters": [
          {
            "name": "project",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string"
            }
          },
          {
            "name": "filter",
            "description": "A filter to narrow down results to a preferred subset.",
            "in": "query",
            "schema": {
              "type": "string"
            }
          },
          {
            "name": "pageSize",
            "description": "The maximum number of results to return.",
            "in": "query",
            "schema": {
              "type": "integer",
              "format": "int32"
            }
          },
          {
            "name": "pageToken",
            "description": "A page token received from the ` + "`next_page_token`" + ` field in the response.\nSend that page token to receive the subsequent page.",
            "in": "query",
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "default": {
            "description": "Successful operation",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ListLocationsResponse"
                }
              }
            }
          }
        }
      }
    },
    "/v1/projects/{project}/secrets": {
      "parameters": [
        { "$ref": "#/components/parameters/alt"},
        { "$ref": "#/components/parameters/callback"},
        { "$ref": "#/components/parameters/prettyPrint"},
        { "$ref": "#/components/parameters/_.xgafv"}
      ],
      "post": {
        "tags": ["secretmanager"],
        "operationId": "CreateSecret",
        "description": "Creates a new Secret containing no SecretVersions.",
        "security": [
          {
            "google_oauth_implicit": [
              "https://www.googleapis.com/auth/cloud-platform"
            ]
          },
          {
            "google_oauth_code": [
              "https://www.googleapis.com/auth/cloud-platform"
            ]
          },
          {
            "bearer_auth": []
          }
        ],
        "parameters": [
          {
            "name": "project",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string"
            }
          },
          {
            "name": "secretId",
            "description": "Required. This must be unique within the project.",
            "in": "query",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "requestBody": {
          "description": "Required. A Secret with initial field values.",
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/Secret"
              }
            }
          }
        },
        "responses": {
          "default": {
            "description": "Successful operation",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Secret"
                }
              }
            }
          }
        }
      }
    }
  },
  "components": {
    "parameters": {
      "alt": {
        "name": "$alt",
        "description": "Data format for response.",
        "schema": {
          "default": "json",
          "enum": [
            "json",
            "media",
            "proto"
          ],
          "x-google-enum-descriptions": [
            "Responses with Content-Type of application/json",
            "Media download with context-dependent Content-Type",
            "Responses with Content-Type of application/x-protobuf"
          ],
          "type": "string"
        },
        "in": "query"
      },
      "callback": {
        "name": "$callback",
        "description": "JSONP",
        "schema": {
          "type": "string"
        },
        "in": "query"
      },
      "prettyPrint": {
        "name": "$prettyPrint",
        "description": "Returns response with indentations and line breaks.",
        "schema": {
          "default": "true",
          "type": "boolean"
        },
        "in": "query"
      },
      "_.xgafv": {
        "name": "$.xgafv",
        "description": "V1 error format.",
        "schema": {
          "enum": [
            "1",
            "2"
          ],
          "x-google-enum-descriptions": [
            "v1 error format",
            "v2 error format"
          ],
          "type": "string"
        },
        "in": "query"
      }
    },
    "securitySchemes": {
      "google_oauth_implicit": {
        "type": "oauth2",
        "description": "Google Oauth 2.0 implicit authentication flow.",
        "flows": {
          "implicit": {
            "authorizationUrl": "https://accounts.google.com/o/oauth2/v2/auth",
            "scopes": {
              "https://www.googleapis.com/auth/cloud-platform": "See, edit, configure, and delete your Google Cloud data and see the email address for your Google Account."
            }
          }
        }
      },
      "google_oauth_code": {
        "type": "oauth2",
        "description": "Google Oauth 2.0 authorizationCode authentication flow.",
        "flows": {
          "authorizationCode": {
            "authorizationUrl": "https://accounts.google.com/o/oauth2/v2/auth",
            "tokenUrl": "https://oauth2.googleapis.com/token",
            "refreshUrl": "https://oauth2.googleapis.com/token",
            "scopes": {
              "https://www.googleapis.com/auth/cloud-platform": "See, edit, configure, and delete your Google Cloud data and see the email address for your Google Account."
            }
          }
        }
      },
      "bearer_auth": {
        "type": "http",
        "description": "Http bearer authentication.",
        "scheme": "bearer"
      }
    },
    "schemas": {
      "Location": {
        "description": "A resource that represents a Google Cloud location.",
        "type": "object",
        "properties": {
          "name": {
            "description": "Resource name for the location, which may vary between implementations.",
            "type": "string"
          },
          "locationId": {
            "description": "The canonical id for this location.",
            "type": "string"
          },
          "displayName": {
            "description": "The friendly name for this location, typically a nearby city name.",
            "type": "string"
          },
          "labels": {
            "description": "Cross-service attributes for the location.",
            "type": "object",
            "additionalProperties": {
              "type": "string"
            }
          },
          "metadata": {
            "description": "Service-specific metadata. For example the available capacity at the given location.",
            "type": "object",
            "additionalProperties": {
              "description": "Properties of the object. Contains field @type with type URL."
            }
          }
        }
      },
      "ListLocationsResponse": {
        "description": "The response message for Locations.ListLocations.",
        "type": "object",
        "properties": {
          "locations": {
            "description": "A list of locations that matches the specified filter in the request.",
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/Location"
            }
          },
          "nextPageToken": {
            "description": "The standard List next-page token.",
            "type": "string"
          }
        }
      },
      "Secret": {
        "description": "A Secret is a logical secret whose value and versions can\nbe accessed.\n\nA Secret is made up of zero or more SecretVersions that\nrepresent the secret data.",
        "type": "object",
        "properties": {
          "name": {
            "description": "Output only. The resource name of the Secret in the format` + " `projects/_*_/secrets/*` " + `.",
            "readOnly": true,
            "type": "string"
          },
          "createTime": {
            "description": "Output only. The time at which the Secret was created.",
            "readOnly": true,
            "type": "string",
            "format": "date-time"
          },
          "labels": {
            "description": "The labels assigned to this Secret.\n\nLabel keys must be between 1 and 63 characters long",
            "type": "object",
            "additionalProperties": {
              "type": "string"
            }
          },
          "expireTime": {
            "description": "Optional. Timestamp in UTC when the Secret is scheduled to expire. This is\nalways provided on output, regardless of what was sent on input.",
            "type": "string",
            "format": "date-time"
          },
          "ttl": {
            "description": "Input only. The TTL for the Secret.",
            "writeOnly": true,
            "type": "string",
            "format": "google-duration"
          },
          "etag": {
            "description": "Optional. Etag of the currently stored Secret.",
            "type": "string"
          },
          "versionAliases": {
            "description": "Optional. Mapping from version alias to version name.",
            "type": "object",
            "additionalProperties": {
              "type": "string",
              "format": "int64"
            }
          },
          "annotations": {
            "description": "Optional. Custom metadata about the secret.",
            "type": "object",
            "additionalProperties": {
              "type": "string"
            }
          },
          "versionDestroyTtl": {
            "description": "Optional. Secret Version TTL after destruction request\n\nThis is a part of the Delayed secret version destroy feature.\nFor secret with TTL>0, version destruction doesn't happen immediately\non calling destroy instead the version goes to a disabled state and\ndestruction happens after the TTL expires.",
            "type": "string",
            "format": "google-duration"
          }
        }
      }
    }
  }
,
  "externalDocs": {
    "description": "Find more info here.",
    "url": "https://cloud.google.com/secret-manager/"
  }
}
`
