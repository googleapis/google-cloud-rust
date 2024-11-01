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

func TestMakeAPI(t *testing.T) {
	tDir := t.TempDir()

	contents := []byte(testDocument)
	translator, err := NewTranslator(contents, &Options{
		Language:    "rust",
		OutDir:      tDir,
		TemplateDir: "not used",
	})
	if err != nil {
		t.Errorf("Error in NewTranslator() %q", err)
	}

	api, err := translator.makeAPI()
	if err != nil {
		t.Errorf("Error in makeAPI() %q", err)
	}

	checkMessage(t, *api.Messages[0], genclient.Message{
		Documentation: "A resource that represents a Google Cloud location.",
		Name:          "Location",
		Fields: []*genclient.Field{
			{
				Name:          "name",
				Documentation: "Resource name for the location, which may vary between implementations.",
				Typez:         genclient.STRING_TYPE,
				Optional:      true,
			},
			{
				Name:          "locationId",
				Documentation: `The canonical id for this location.`,
				Typez:         genclient.STRING_TYPE,
				Optional:      true,
			},
			{
				Name:          "displayName",
				Documentation: `The friendly name for this location, typically a nearby city name.`,
				Typez:         genclient.STRING_TYPE,
				Optional:      true,
			},
			{
				Name:          "labels",
				Documentation: "Cross-service attributes for the location.",
				Typez:         genclient.MESSAGE_TYPE,
				Optional:      true,
			},
			{
				Name:          "metadata",
				Documentation: `Service-specific metadata. For example the available capacity at the given location.`,
				Typez:         genclient.MESSAGE_TYPE,
				Optional:      true,
			},
		},
	})

	checkMessage(t, *api.Messages[1], genclient.Message{
		Documentation: "The response message for Locations.ListLocations.",
		Name:          "ListLocationsResponse",
		Fields: []*genclient.Field{
			{
				Name:          "locations",
				Documentation: "A list of locations that matches the specified filter in the request.",
				Typez:         genclient.MESSAGE_TYPE,
				Repeated:      true,
			},
			{
				Name:          "nextPageToken",
				Documentation: "The standard List next-page token.",
				Typez:         genclient.STRING_TYPE,
				Optional:      true,
			},
		},
	})
}

func checkMessage(t *testing.T, got genclient.Message, want genclient.Message) {
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
}

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
