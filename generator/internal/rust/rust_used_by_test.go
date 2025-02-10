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

func TestRequiredPackages(t *testing.T) {
	outdir := "src/generated/newlib"
	options := map[string]string{
		"package:async-trait": "package=async-trait,version=0.1.83,force-used=true",
		"package:gtype":       "package=gcp-sdk-type,path=src/generated/type,source=google.type,source=test-only",
		"package:gax":         "package=gcp-sdk-gax,path=src/gax,version=1.2.3,force-used=true",
		"package:auth":        "ignore=true",
	}
	c, err := newCodec(options)
	if err != nil {
		t.Fatal(err)
	}
	got := requiredPackages(outdir, c.extraPackages)
	want := []string{
		"async-trait = { version = \"0.1.83\" }",
		"gax        = { version = \"1.2.3\", path = \"../../../src/gax\", package = \"gcp-sdk-gax\" }",
	}
	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched required packages (-want, +got):\n%s", diff)
	}
}

func TestRequiredPackagesLocal(t *testing.T) {
	// This is not a thing we expect to do in the Rust repository, but the
	// behavior is consistent.
	options := map[string]string{
		"package:gtype": "package=types,path=src/generated/type,source=google.type,source=test-only,force-used=true",
	}
	c, err := newCodec(options)
	if err != nil {
		t.Fatal(err)
	}
	got := requiredPackages("", c.extraPackages)
	want := []string{
		"gtype      = { path = \"src/generated/type\", package = \"types\" }",
	}
	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched required packages (-want, +got):\n%s", diff)
	}
}

func TestFindUsedPackages(t *testing.T) {
	service := &api.Service{
		Name:    "LroService",
		ID:      ".test.LroService",
		Package: "test",
		Methods: []*api.Method{
			{
				Name:         "CreateResource",
				ID:           ".test.LroService.CreateResource",
				InputTypeID:  ".test.CreateResourceRequest",
				OutputTypeID: ".google.longrunning.Operation",
				OperationInfo: &api.OperationInfo{
					MetadataTypeID: ".google.cloud.common.OperationMetadata",
					ResponseTypeID: ".test.Resource",
				},
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{
		{Name: "Resource", ID: ".test.Resource"},
		{Name: "CreateResource", ID: ".test.Resource"},
	}, []*api.Enum{}, []*api.Service{service})

	model.State.MessageByID[".google.longrunning.Operation"] = &api.Message{
		Name:    "Operation",
		ID:      ".google.longrunning.Operation",
		Package: "google.longrunning",
	}
	model.State.MessageByID[".google.cloud.common.OperationMetadata"] = &api.Message{
		Name:    "OperationMetadata",
		ID:      ".google.cloud.common.OperationMetadata",
		Package: "google.cloud.common",
	}

	c, err := newCodec(map[string]string{
		"package:common":      "package=google-cloud-common,source=google.cloud.common,path=src/generated/cloud/common,version=0.2",
		"package:longrunning": "package=google-longrunning,source=google.longrunning,path=src/generated/longrunning,version=0.2",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	findUsedPackages(model, c)
	want := []*packagez{
		{
			name:            "common",
			packageName:     "google-cloud-common",
			path:            "src/generated/cloud/common",
			version:         "0.2",
			defaultFeatures: true,
			used:            true,
		},
		{
			name:            "longrunning",
			packageName:     "google-longrunning",
			path:            "src/generated/longrunning",
			version:         "0.2",
			defaultFeatures: true,
			used:            true,
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}
