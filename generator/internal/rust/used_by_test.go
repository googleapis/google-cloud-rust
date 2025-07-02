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
	c, err := newCodec(true, map[string]string{
		"package:tracing":  "used-if=services,package=tracing",
		"package:location": "package=gcp-sdk-location,source=google.cloud.location",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:        "location",
			packageName: "gcp-sdk-location",
		},
		{
			name:        "tracing",
			packageName: "tracing",
			used:        true,
			usedIf:      []string{"services"},
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestUsedByServicesNoServices(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c, err := newCodec(true, map[string]string{
		"package:tracing":  "used-if=services,package=tracing",
		"package:location": "package=gcp-sdk-location,source=google.cloud.location",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:        "location",
			packageName: "gcp-sdk-location",
		},
		{
			name:        "tracing",
			packageName: "tracing",
			usedIf:      []string{"services"},
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
	c, err := newCodec(true, map[string]string{
		"package:location": "package=gcp-sdk-location,source=google.cloud.location",
		"package:lro":      "used-if=lro,package=google-cloud-lro",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:        "location",
			packageName: "gcp-sdk-location",
		},
		{
			name:        "lro",
			packageName: "google-cloud-lro",
			used:        true,
			usedIf:      []string{"lro"},
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
	c, err := newCodec(true, map[string]string{
		"package:location": "package=gcp-sdk-location,source=google.cloud.location",
		"package:lro":      "used-if=lro,package=google-cloud-lro",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	want := []*packagez{
		{
			name:        "location",
			packageName: "gcp-sdk-location",
		},
		{
			name:        "lro",
			packageName: "google-cloud-lro",
			used:        false,
			usedIf:      []string{"lro"},
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestRequiredPackages(t *testing.T) {
	options := map[string]string{
		"package:async-trait": "package=async-trait,force-used=true",
		"package:serde_with":  "package=serde_with,force-used=true,feature=base64,feature=macro,feature=std",
		"package:gtype":       "package=gcp-sdk-type,source=google.type,source=test-only",
		"package:gax":         "package=gcp-sdk-gax,force-used=true",
		"package:auth":        "ignore=true",
	}
	c, err := newCodec(true, options)
	if err != nil {
		t.Fatal(err)
	}
	got := requiredPackages(c.extraPackages)
	want := []string{
		"async-trait.workspace = true",
		"gax.workspace        = true",
		"serde_with           = { workspace = true, features = [\"base64\", \"macro\", \"std\"] }",
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
		"package:gtype": "package=types,source=google.type,source=test-only,force-used=true",
	}
	c, err := newCodec(true, options)
	if err != nil {
		t.Fatal(err)
	}
	got := requiredPackages(c.extraPackages)
	want := []string{
		"gtype.workspace      = true",
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

	c, err := newCodec(true, map[string]string{
		"package:common":      "package=google-cloud-common,source=google.cloud.common",
		"package:longrunning": "package=google-longrunning,source=google.longrunning",
	})
	if err != nil {
		t.Fatal(err)
	}
	loadWellKnownTypes(model.State)
	findUsedPackages(model, c)
	want := []*packagez{
		{
			name:        "common",
			packageName: "google-cloud-common",
			used:        true,
		},
		{
			name:        "longrunning",
			packageName: "google-longrunning",
			used:        true,
		},
	}
	less := func(a, b *packagez) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(packagez{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}
