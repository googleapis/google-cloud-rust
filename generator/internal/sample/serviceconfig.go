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

package sample

import (
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/types/known/apipb"
)

func ServiceConfig() *serviceconfig.Service {
	return &serviceconfig.Service{
		Name:  "secretmanager.googleapis.com",
		Title: "Secret Manager API",
		Apis: []*apipb.Api{
			{
				Name: "google.cloud.location.Locations",
			},
			{
				Name: "google.cloud.secretmanager.v1.SecretManagerService",
			},
		},
		Documentation: &serviceconfig.Documentation{
			Summary: "Stores sensitive data such as API keys, passwords, and certificates.\nProvides convenience while improving security.",
			Rules: []*serviceconfig.DocumentationRule{
				{
					Selector:    "google.cloud.location.Locations.GetLocation",
					Description: "Gets information about a location.",
				},
				{
					Selector:    "google.cloud.location.Locations.ListLocations",
					Description: "Lists information about the supported locations for this service.",
				},
			},
			Overview: "Secret Manager Overview",
		},
		Http: &annotations.Http{
			Rules: []*annotations.HttpRule{
				{
					Selector: "google.cloud.location.Locations.GetLocation",
					Pattern: &annotations.HttpRule_Get{
						Get: "/v1/{name=projects/*/locations/*}",
					},
				},
				{
					Selector: "google.cloud.location.Locations.ListLocations",
					Pattern: &annotations.HttpRule_Get{
						Get: "/v1/{name=projects/*}/locations",
					},
				},
			},
		},
		Authentication: &serviceconfig.Authentication{
			Rules: []*serviceconfig.AuthenticationRule{
				{
					Selector: "google.cloud.location.Locations.GetLocation",
					Oauth:    &serviceconfig.OAuthRequirements{},
				},
				{
					Selector: "google.cloud.location.Locations.ListLocations",
					Oauth:    &serviceconfig.OAuthRequirements{},
				},
				{
					Selector: "google.cloud.secretmanager.v1.SecretManagerService.*",
					Oauth:    &serviceconfig.OAuthRequirements{},
				},
			},
		},
		Publishing: &annotations.Publishing{},
	}
}
