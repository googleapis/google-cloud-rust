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
	api, err := makeAPI(model)
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

	api, err := makeAPI(model)
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
			{Name: "fBool", Typez: genclient.BOOL_TYPE, TypezID: "bool"},
			{Name: "fInt64", Typez: genclient.INT64_TYPE, TypezID: "int64"},
			{Name: "fInt32", Typez: genclient.INT32_TYPE, TypezID: "int32"},
			{Name: "fUInt32", Typez: genclient.UINT32_TYPE, TypezID: "uint32"},
			{Name: "fFloat", Typez: genclient.FLOAT_TYPE, TypezID: "float"},
			{Name: "fDouble", Typez: genclient.DOUBLE_TYPE, TypezID: "double"},
			{Name: "fString", Typez: genclient.STRING_TYPE, TypezID: "string"},
			{Name: "fOptional", Typez: genclient.STRING_TYPE, TypezID: "string", Optional: true},
			{Name: "fSInt64", Typez: genclient.INT64_TYPE, TypezID: "int64"},
			{Name: "fSUInt64", Typez: genclient.UINT64_TYPE, TypezID: "uint64"},
			{Name: "fDuration", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.Duration", Optional: true},
			{Name: "fTimestamp", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.Timestamp", Optional: true},
			{Name: "fFieldMask", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.FieldMask", Optional: true},
			{Name: "fBytes", Typez: genclient.BYTES_TYPE, TypezID: "bytes"},
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
	api, err := makeAPI(model)
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
			{Repeated: true, Name: "fBool", Typez: genclient.BOOL_TYPE, TypezID: "bool"},
			{Repeated: true, Name: "fInt64", Typez: genclient.INT64_TYPE, TypezID: "int64"},
			{Repeated: true, Name: "fInt32", Typez: genclient.INT32_TYPE, TypezID: "int32"},
			{Repeated: true, Name: "fUInt32", Typez: genclient.UINT32_TYPE, TypezID: "uint32"},
			{Repeated: true, Name: "fString", Typez: genclient.STRING_TYPE, TypezID: "string"},
			{Repeated: true, Name: "fSInt64", Typez: genclient.INT64_TYPE, TypezID: "int64"},
			{Repeated: true, Name: "fSUInt64", Typez: genclient.UINT64_TYPE, TypezID: "uint64"},
			{Repeated: true, Name: "fDuration", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.Duration"},
			{Repeated: true, Name: "fTimestamp", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.Timestamp"},
			{Repeated: true, Name: "fFieldMask", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.FieldMask"},
			{Repeated: true, Name: "fBytes", Typez: genclient.BYTES_TYPE, TypezID: "bytes"},
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
	api, err := makeAPI(model)
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
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "..Foo",
				Documentation: "An object field.",
				Optional:      true,
			},
			{
				Name:          "fObjectArray",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "Bar",
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
	api, err := makeAPI(model)
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{Name: "fMap", Typez: genclient.MESSAGE_TYPE, TypezID: ".google.protobuf.Any", Optional: true},
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
	api, err := makeAPI(model)
	if err != nil {
		t.Fatal(err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{Name: "fMap", Typez: genclient.MESSAGE_TYPE, TypezID: "$map<string, string>"},
			{Name: "fMapS32", Typez: genclient.MESSAGE_TYPE, TypezID: "$map<string, int32>"},
			{Name: "fMapS64", Typez: genclient.MESSAGE_TYPE, TypezID: "$map<string, int64>"},
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
	api, err := makeAPI(model)
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Name:          "Fake",
		ID:            "..Fake",
		Documentation: "A test message.",
		Fields: []*genclient.Field{
			{Name: "fMapI32", Typez: genclient.MESSAGE_TYPE, TypezID: "$map<string, int32>", Optional: false},
			{Name: "fMapI64", Typez: genclient.MESSAGE_TYPE, TypezID: "$map<string, int64>", Optional: false},
		},
	})
}

func TestMakeAPI(t *testing.T) {
	contents := []byte(testDocument)
	model, err := createDocModel(contents)
	if err != nil {
		t.Fatal(err)
	}
	api, err := makeAPI(model)
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
				Documentation: "Resource name for the location, which may vary between implementations.",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "locationId",
				Documentation: `The canonical id for this location.`,
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "displayName",
				Documentation: `The friendly name for this location, typically a nearby city name.`,
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
			{
				Name:          "labels",
				Documentation: "Cross-service attributes for the location.",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "$map<string, string>",
				Optional:      false,
			},
			{
				Name:          "metadata",
				Documentation: `Service-specific metadata. For example the available capacity at the given location.`,
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       ".google.protobuf.Any",
				Optional:      true,
			},
		},
	})

	listLocationsResponse := api.State.MessageByID["..ListLocationsResponse"]
	if listLocationsResponse == nil {
		t.Errorf("missing message (listLocationsResponse) in MessageByID index")
		return
	}
	checkMessage(t, *listLocationsResponse, genclient.Message{
		Documentation: "The response message for Locations.ListLocations.",
		Name:          "ListLocationsResponse",
		ID:            "..ListLocationsResponse",
		Fields: []*genclient.Field{
			{
				Name:          "locations",
				Documentation: "A list of locations that matches the specified filter in the request.",
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       "Location",
				Repeated:      true,
			},
			{
				Name:          "nextPageToken",
				Documentation: "The standard List next-page token.",
				Typez:         genclient.STRING_TYPE,
				TypezID:       "string",
				Optional:      true,
			},
		},
	})
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
            "description": "A filter to narrow down results to a preferred subset.\nThe filtering language accepts strings like \"displayName=tokyo\", and\nis documented in more detail in [AIP-160](https://google.aip.dev/160).",
            "in": "query",
            "schema": {
              "type": "string"
            }
          },
          {
            "name": "pageSize",
            "description": "The maximum number of results to return.\nIf not set, the service selects a default.",
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
