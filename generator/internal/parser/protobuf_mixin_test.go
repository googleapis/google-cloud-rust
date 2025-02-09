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

package parser

import (
	"testing"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/types/known/apipb"
)

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
