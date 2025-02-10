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

package rust

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestUsedByServicesWithServices(t *testing.T) {
	service := &api.Service{
		Name: "TestService",
		ID:   ".test.Service",
	}
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{service})
	c, err := newCodec(map[string]string{
		"package:tracing":  "used-if=services,package=tracing,version=0.1.41",
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:            "location",
			packageName:     "gcp-sdk-location",
			path:            "src/generated/cloud/location",
			version:         "0.1.0",
			defaultFeatures: true,
		},
		{
			name:            "tracing",
			packageName:     "tracing",
			version:         "0.1.41",
			used:            true,
			usedIf:          []string{"services"},
			defaultFeatures: true,
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestUsedByServicesNoServices(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c, err := newCodec(map[string]string{
		"package:tracing":  "used-if=services,package=tracing,version=0.1.41",
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:            "location",
			packageName:     "gcp-sdk-location",
			path:            "src/generated/cloud/location",
			version:         "0.1.0",
			defaultFeatures: true,
		},
		{
			name:            "tracing",
			packageName:     "tracing",
			version:         "0.1.41",
			usedIf:          []string{"services"},
			defaultFeatures: true,
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestUsedByLROsWithLRO(t *testing.T) {
	method := &api.Method{
		Name:          "CreateResource",
		OperationInfo: &api.OperationInfo{},
	}
	service := &api.Service{
		Name:    "TestService",
		ID:      ".test.Service",
		Methods: []*api.Method{method},
	}
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{service})
	c, err := newCodec(map[string]string{
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
		"package:lro":      "used-if=lro,package=google-cloud-lro,path=src/lro,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:            "location",
			packageName:     "gcp-sdk-location",
			path:            "src/generated/cloud/location",
			version:         "0.1.0",
			defaultFeatures: true,
		},
		{
			name:            "lro",
			packageName:     "google-cloud-lro",
			path:            "src/lro",
			version:         "0.1.0",
			used:            true,
			usedIf:          []string{"lro"},
			defaultFeatures: true,
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestUsedByLROsWithoutLRO(t *testing.T) {
	method := &api.Method{
		Name: "CreateResource",
	}
	service := &api.Service{
		Name:    "TestService",
		ID:      ".test.Service",
		Methods: []*api.Method{method},
	}
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{service})
	c, err := newCodec(map[string]string{
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
		"package:lro":      "used-if=lro,package=google-cloud-lro,path=src/lro,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:            "location",
			packageName:     "gcp-sdk-location",
			path:            "src/generated/cloud/location",
			version:         "0.1.0",
			defaultFeatures: true,
		},
		{
			name:            "lro",
			packageName:     "google-cloud-lro",
			path:            "src/lro",
			version:         "0.1.0",
			used:            false,
			usedIf:          []string{"lro"},
			defaultFeatures: true,
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}
