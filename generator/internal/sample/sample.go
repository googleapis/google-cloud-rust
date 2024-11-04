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

// Package sample provides sample data for testing.
package sample

import (
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/types/known/apipb"
)

var ServiceConfig = &serviceconfig.Service{
	Name:  "secretmanager.googleapis.com",
	Title: "Secret Manager API",
	Apis: []*apipb.Api{
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
	Backend: &serviceconfig.Backend{
		Rules: []*serviceconfig.BackendRule{
			{
				Selector: "google.cloud.location.Locations.GetLocation",
				Deadline: 60,
			},
			{
				Selector: "google.cloud.location.Locations.ListLocations",
				Deadline: 60,
			},
			{
				Selector: "google.cloud.secretmanager.v1.SecretManagerService.*",
				Deadline: 60,
			},
		},
	},
	Http: &annotations.Http{
		Rules: []*annotations.HttpRule{
			{
				Selector: "google.cloud.location.Locations.GetLocation",
			},
			{
				Selector: "google.cloud.location.Locations.ListLocations",
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
}
