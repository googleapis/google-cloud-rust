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

// Package sample provides sample data for testing.
package sample

import (
	"net/http"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

const (
	APIName           = "secretmanager"
	APITitle          = "Secret Manager API"
	APIPackageName    = "google.cloud.secretmanager.v1"
	APIDescription    = "Stores sensitive data such as API keys, passwords, and certificates.\nProvides convenience while improving security."
	SpecificationName = "google.cloud.secretmanager.v1"

	ServiceName = "SecretManagerService"
	DefaultHost = "secretmanager.googleapis.com"
	Package     = "google.cloud.secretmanager.v1"
)

func API() *api.API {
	return &api.API{
		Name:        APIName,
		Title:       APITitle,
		PackageName: APIPackageName,
		Description: APIDescription,
		Services:    []*api.Service{Service()},
		Messages: []*api.Message{
			Replication(),
			Automatic(),
		},
		Enums: []*api.Enum{EnumState()},
	}
}

func Service() *api.Service {
	return &api.Service{
		Name:          ServiceName,
		Documentation: APIDescription,
		DefaultHost:   DefaultHost,
		Methods: []*api.Method{
			MethodCreate(),
			MethodUpdate(),
			MethodListSecretVersions(),
		},
		Package: Package,
	}
}

func MethodCreate() *api.Method {
	return &api.Method{
		Name:          "CreateSecret",
		Documentation: "Creates a new Secret containing no SecretVersions.",
		ID:            "..Service.CreateSecret",
		InputTypeID:   CreateRequest().ID,
		OutputTypeID:  Secret().ID,
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: http.MethodPost,
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewLiteralPathSegment("projects"),
						api.NewFieldPathPathSegment("project"),
						api.NewLiteralPathSegment("secrets"),
					},
					PathTemplate: api.NewPathTemplate().
						WithLiteral("v1").
						WithLiteral("projects").
						WithVariableNamed("project").
						WithLiteral("secrets"),
					QueryParameters: map[string]bool{"secretId": true},
				},
			},
			BodyFieldPath: "requestBody",
		},
	}
}

func MethodUpdate() *api.Method {
	return &api.Method{
		Name:          "UpdateSecret",
		Documentation: "Updates metadata of an existing Secret.",
		ID:            "..Service.UpdateSecret",
		InputTypeID:   UpdateRequest().ID,
		OutputTypeID:  ".google.protobuf.Empty",
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: http.MethodPatch,
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewFieldPathPathSegment("secret.name"),
					},
					PathTemplate: api.NewPathTemplate().
						WithLiteral("v1").
						WithVariableNamed("secret", "name"),
					QueryParameters: map[string]bool{
						"field_mask": true,
					},
				},
			},
		},
	}
}

func MethodAddSecretVersion() *api.Method {
	return &api.Method{
		Name:          "AddSecretVersion",
		ID:            "..Service.AddSecretVersion",
		Documentation: "Creates a new SecretVersion containing secret data and attaches\nit to an existing Secret.",
		InputTypeID:   "..AddSecretVersionRequest",
		OutputTypeID:  "..SecretVersion",
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: http.MethodPost,
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewLiteralPathSegment("projects"),
						api.NewFieldPathPathSegment("project"),
						api.NewLiteralPathSegment("secrets"),
						api.NewFieldPathPathSegment("secret"),
						api.NewVerbPathSegment("addVersion"),
					},
					PathTemplate: api.NewPathTemplate().
						WithLiteral("v1").
						WithLiteral("projects").
						WithVariableNamed("project").
						WithLiteral("secrets").
						WithVariableNamed("secret").
						WithVerb("addVersion"),
					QueryParameters: map[string]bool{},
				},
			},
			BodyFieldPath: "*",
		},
	}
}

func MethodListSecretVersions() *api.Method {
	return &api.Method{
		Name:          "ListSecretVersions",
		ID:            "..Service.ListVersion",
		Documentation: "Lists [SecretVersions][google.cloud.secretmanager.v1.SecretVersion]. This call does not return secret data.",
		InputTypeID:   ListSecretVersionsRequest().ID,
		InputType:     ListSecretVersionsRequest(),
		OutputTypeID:  ListSecretVersionsResponse().ID,
		OutputType:    ListSecretVersionsResponse(),
		PathInfo: &api.PathInfo{
			Bindings: []*api.PathBinding{
				{
					Verb: http.MethodPost,
					LegacyPathTemplate: []api.LegacyPathSegment{
						api.NewLiteralPathSegment("v1"),
						api.NewLiteralPathSegment("projects"),
						api.NewFieldPathPathSegment("parent"),
						api.NewLiteralPathSegment("secrets"),
						api.NewFieldPathPathSegment("secret"),
						api.NewVerbPathSegment("listSecretVersions"),
					},
					PathTemplate: api.NewPathTemplate().
						WithLiteral("v1").
						WithLiteral("projects").
						WithVariableNamed("parent").
						WithLiteral("secrets").
						WithVariableNamed("secret").
						WithVerb("listSecretVersions"),
					QueryParameters: map[string]bool{},
				},
			},
			BodyFieldPath: "*",
		},
	}
}

func CreateRequest() *api.Message {
	return &api.Message{
		Name:          "CreateSecretRequest",
		ID:            "..CreateSecretRequest",
		Documentation: "Request message for SecretManagerService.CreateSecret",
		Package:       Package,
		Fields: []*api.Field{
			{
				Name:     "project",
				JSONName: "project",
				Typez:    api.STRING_TYPE,
			},
			{
				Name:     "secret_id",
				JSONName: "secretId",
				Typez:    api.STRING_TYPE,
			},
		},
	}
}

func UpdateRequest() *api.Message {
	return &api.Message{
		Name:          "UpdateSecretRequest",
		ID:            "..UpdateRequest",
		Documentation: "Request message for SecretManagerService.UpdateSecret",
		Package:       Package,
		Fields: []*api.Field{
			{
				Name:     "secret",
				JSONName: "secret",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  Secret().ID,
			},
			{
				Name:     "field_mask",
				JSONName: "fieldMask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
		},
	}
}

func ListSecretVersionsRequest() *api.Message {
	return &api.Message{
		Name:          "ListSecretVersionRequest",
		ID:            "..ListSecretVersionsRequest",
		Documentation: "Lists SecretVersions. This call does not return secret data.",
		Package:       Package,
		Fields: []*api.Field{
			{
				Name:     "parent",
				JSONName: "parent",
				ID:       Secret().ID + ".parent",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  Secret().ID,
			},
		},
	}
}

func ListSecretVersionsResponse() *api.Message {
	return &api.Message{
		Name:    "ListSecretVersionsResponse",
		ID:      "..ListSecretVersionsResponse",
		Package: Package,
		Fields: []*api.Field{
			{
				Name:     "versions",
				JSONName: "versions",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  SecretVersion().ID,
				Repeated: true,
			},
		},
	}
}

func Secret() *api.Message {
	return &api.Message{
		Name:    "Secret",
		ID:      "..Secret",
		Package: Package,
		Fields: []*api.Field{
			{
				Name:     "name",
				JSONName: "name",
				Typez:    api.STRING_TYPE,
			},
			{
				Name:     "replication",
				JSONName: "replication",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  Replication().ID,
			},
		},
	}
}

func SecretVersion() *api.Message {
	return &api.Message{
		Name:    "SecretVersion",
		Package: Package,
		ID:      "google.cloud.secretmanager.v1.SecretVersion",
		Enums:   []*api.Enum{EnumState()},
		Fields: []*api.Field{
			{
				Name:     "name",
				JSONName: "name",
				Typez:    api.STRING_TYPE,
			},
			{
				Name:     "state",
				JSONName: "state",
				Typez:    api.ENUM_TYPE,
				TypezID:  EnumState().ID,
			},
		},
	}
}

func EnumState() *api.Enum {
	var (
		stateEnabled = &api.EnumValue{
			Name:   "Enabled",
			Number: 1,
		}
		stateDisabled = &api.EnumValue{
			Name:   "Disabled",
			Number: 2,
		}
	)
	return &api.Enum{
		Name:    "State",
		ID:      ".test.EnumState",
		Package: Package,
		Values: []*api.EnumValue{
			stateEnabled,
			stateDisabled,
		},
	}
}

func Replication() *api.Message {
	return &api.Message{
		Name:    "Replication",
		Package: Package,
		ID:      "google.cloud.secretmanager.v1.Replication",
		Fields: []*api.Field{
			{
				Name:     "automatic",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
}

func Automatic() *api.Message {
	return &api.Message{
		Name:          "Automatic",
		ID:            "..Automatic",
		Package:       Package,
		Documentation: "A replication policy that replicates the Secret payload without any restrictions.",
		Parent:        Replication(),
		Fields: []*api.Field{
			{
				Name:          "customerManagedEncryption",
				JSONName:      "customerManagedEncryption",
				Documentation: "Optional. The customer-managed encryption configuration of the Secret.",
				Typez:         api.MESSAGE_TYPE,
				TypezID:       "..CustomerManagedEncryption",
				Optional:      true,
			},
		},
	}
}

func CustomerManagedEncryption() *api.Message {
	return &api.Message{
		Name:    "CustomerManagedEncryption",
		ID:      "..CustomerManagedEncryption",
		Package: Package,
	}
}

func SecretPayload() *api.Message {
	return &api.Message{
		Name:          "SecretPayload",
		ID:            "..SecretPayload",
		Documentation: "A secret payload resource in the Secret Manager API. This contains the\nsensitive secret payload that is associated with a SecretVersion.",
		Fields: []*api.Field{
			{
				Name:          "data",
				JSONName:      "data",
				Documentation: "The secret data. Must be no larger than 64KiB.",
				Typez:         api.BYTES_TYPE,
				TypezID:       "bytes",
				Optional:      true,
			},
			{
				Name:          "dataCrc32c",
				JSONName:      "dataCrc32c",
				Documentation: "Optional. If specified, SecretManagerService will verify the integrity of the\nreceived data on SecretManagerService.AddSecretVersion calls using\nthe crc32c checksum and store it to include in future\nSecretManagerService.AccessSecretVersion responses. If a checksum is\nnot provided in the SecretManagerService.AddSecretVersion request, the\nSecretManagerService will generate and store one for you.\n\nThe CRC32C value is encoded as a Int64 for compatibility, and can be\nsafely downconverted to uint32 in languages that support this type.\nhttps://cloud.google.com/apis/design/design_patterns#integer_types",
				Typez:         api.INT64_TYPE,
				TypezID:       "int64",
				Optional:      true,
			},
		},
	}
}
