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

package language

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

func createRustCodec() *rustCodec {
	wkt := &rustPackage{
		name:        "wkt",
		packageName: "types",
		path:        "../../types",
	}

	return &rustCodec{
		modulePath:    "crate::model",
		extraPackages: []*rustPackage{wkt},
		packageMapping: map[string]*rustPackage{
			"google.protobuf": wkt,
		},
	}
}

func TestRust_ParseOptions(t *testing.T) {
	options := map[string]string{
		"version":               "1.2.3",
		"package-name-override": "test-only",
		"copyright-year":        "2035",
		"module-path":           "alternative::generated",
		"package:wkt":           "package=types,path=src/wkt,source=google.protobuf,source=test-only",
		"package:gax":           "package=gax,path=src/gax,feature=unstable-sdk-client",
		"package:serde_with":    "package=serde_with,version=2.3.4,default-features=false",
	}
	got, err := newRustCodec(options)
	if err != nil {
		t.Fatal(err)
	}
	gp := &rustPackage{
		name:            "wkt",
		packageName:     "types",
		path:            "src/wkt",
		defaultFeatures: true,
	}
	want := &rustCodec{
		version:                  "1.2.3",
		packageNameOverride:      "test-only",
		generationYear:           "2035",
		modulePath:               "alternative::generated",
		deserializeWithdDefaults: true,
		extraPackages: []*rustPackage{
			gp,
			{
				name:        "gax",
				packageName: "gax",
				path:        "src/gax",
				features: []string{
					"unstable-sdk-client",
				},
				defaultFeatures: true,
			},
			{
				name:            "serde_with",
				packageName:     "serde_with",
				version:         "2.3.4",
				defaultFeatures: false,
			},
		},
		packageMapping: map[string]*rustPackage{
			"google.protobuf": gp,
			"test-only":       gp,
		},
	}
	sort.Slice(want.extraPackages, func(i, j int) bool {
		return want.extraPackages[i].name < want.extraPackages[j].name
	})
	sort.Slice(got.extraPackages, func(i, j int) bool {
		return got.extraPackages[i].name < got.extraPackages[j].name
	})
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(rustCodec{}, rustPackage{})); diff != "" {
		t.Errorf("codec mismatch (-want, +got):\n%s", diff)
	}
	if want.packageNameOverride != got.packageNameOverride {
		t.Errorf("mismatched in packageNameOverride, want=%s, got=%s", want.packageNameOverride, got.packageNameOverride)
	}
	checkRustPackages(t, got, want)
}

func TestRust_RequiredPackages(t *testing.T) {
	outdir := "src/generated/newlib"
	options := map[string]string{
		"package:async-trait": "package=async-trait,version=0.1.83,force-used=true",
		"package:gtype":       "package=gcp-sdk-type,path=src/generated/type,source=google.type,source=test-only",
		"package:gax":         "package=gcp-sdk-gax,path=src/gax,version=1.2.3,force-used=true",
		"package:auth":        "ignore=true",
	}
	c, err := newRustCodec(options)
	if err != nil {
		t.Fatal(err)
	}
	got := rustRequiredPackages(outdir, c.extraPackages)
	want := []string{
		"async-trait = { version = \"0.1.83\" }",
		"gax        = { version = \"1.2.3\", path = \"../../../src/gax\", package = \"gcp-sdk-gax\" }",
	}
	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched required packages (-want, +got):\n%s", diff)
	}
}

func TestRust_RequiredPackagesLocal(t *testing.T) {
	// This is not a thing we expect to do in the Rust repository, but the
	// behavior is consistent.
	options := map[string]string{
		"package:gtype": "package=types,path=src/generated/type,source=google.type,source=test-only,force-used=true",
	}
	c, err := newRustCodec(options)
	if err != nil {
		t.Fatal(err)
	}
	got := rustRequiredPackages("", c.extraPackages)
	want := []string{
		"gtype      = { path = \"src/generated/type\", package = \"types\" }",
	}
	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched required packages (-want, +got):\n%s", diff)
	}
}

func TestRust_PackageName(t *testing.T) {
	rustPackageNameImpl(t, "test-only-overridden", map[string]string{
		"package-name-override": "test-only-overridden",
	}, &api.API{
		Name:        "test-only-name",
		PackageName: "google.cloud.service.v3",
	})
	rustPackageNameImpl(t, "gcp-sdk-service-v3", nil, &api.API{
		Name:        "test-only-name",
		PackageName: "google.cloud.service.v3",
	})
	rustPackageNameImpl(t, "gcp-sdk-type", nil, &api.API{
		Name:        "type",
		PackageName: "",
	})
}

func rustPackageNameImpl(t *testing.T, want string, opts map[string]string, api *api.API) {
	t.Helper()
	c, err := newRustCodec(opts)
	if err != nil {
		t.Fatal(err)
	}
	got := rustPackageName(api, c.packageNameOverride)
	if want != got {
		t.Errorf("mismatch in package name, want=%s, got=%s", want, got)
	}
}

func checkRustPackages(t *testing.T, got *rustCodec, want *rustCodec) {
	t.Helper()
	less := func(a, b *rustPackage) bool { return a.name < b.name }
	if diff := cmp.Diff(want.extraPackages, got.extraPackages, cmp.AllowUnexported(rustPackage{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("package mismatch (-want, +got):\n%s", diff)
	}
}

func TestRust_Validate(t *testing.T) {
	model := api.NewTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	if err := rustValidate(model, "p1"); err != nil {
		t.Errorf("unexpected error in API validation %q", err)
	}
}

func TestRust_ValidateMessageMismatch(t *testing.T) {
	test := api.NewTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	c := &rustCodec{}
	if err := rustValidate(test, c.sourceSpecificationPackageName); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.sourceSpecificationPackageName)
	}

	test = api.NewTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	c = &rustCodec{}
	if err := rustValidate(test, c.sourceSpecificationPackageName); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.sourceSpecificationPackageName)
	}

	test = api.NewTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	c = &rustCodec{}
	if err := rustValidate(test, c.sourceSpecificationPackageName); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.sourceSpecificationPackageName)
	}
}

func TestWellKnownTypesExist(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	rustLoadWellKnownTypes(model.State)
	for _, name := range []string{"Any", "Duration", "Empty", "FieldMask", "Timestamp"} {
		if _, ok := model.State.MessageByID[fmt.Sprintf(".google.protobuf.%s", name)]; !ok {
			t.Errorf("cannot find well-known message %s in API", name)
		}
	}
}

func TestUsedByServicesWithServices(t *testing.T) {
	service := &api.Service{
		Name: "TestService",
		ID:   ".test.Service",
	}
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{service})
	c, err := newRustCodec(map[string]string{
		"package:tracing":  "used-if=services,package=tracing,version=0.1.41",
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	rustLoadWellKnownTypes(model.State)
	rustResolveUsedPackages(model, c.extraPackages)
	want := []*rustPackage{
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
	less := func(a, b *rustPackage) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(rustPackage{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestUsedByServicesNoServices(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c, err := newRustCodec(map[string]string{
		"package:tracing":  "used-if=services,package=tracing,version=0.1.41",
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	rustLoadWellKnownTypes(model.State)
	rustResolveUsedPackages(model, c.extraPackages)
	want := []*rustPackage{
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
	less := func(a, b *rustPackage) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(rustPackage{}), cmpopts.SortSlices(less)); diff != "" {
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
	c, err := newRustCodec(map[string]string{
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
		"package:lro":      "used-if=lro,package=gcp-sdk-lro,path=src/lro,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	rustLoadWellKnownTypes(model.State)
	rustResolveUsedPackages(model, c.extraPackages)
	want := []*rustPackage{
		{
			name:            "location",
			packageName:     "gcp-sdk-location",
			path:            "src/generated/cloud/location",
			version:         "0.1.0",
			defaultFeatures: true,
		},
		{
			name:            "lro",
			packageName:     "gcp-sdk-lro",
			path:            "src/lro",
			version:         "0.1.0",
			used:            true,
			usedIf:          []string{"lro"},
			defaultFeatures: true,
		},
	}
	less := func(a, b *rustPackage) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(rustPackage{}), cmpopts.SortSlices(less)); diff != "" {
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
	c, err := newRustCodec(map[string]string{
		"package:location": "package=gcp-sdk-location,source=google.cloud.location,path=src/generated/cloud/location,version=0.1.0",
		"package:lro":      "used-if=lro,package=gcp-sdk-lro,path=src/lro,version=0.1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	rustLoadWellKnownTypes(model.State)
	rustResolveUsedPackages(model, c.extraPackages)
	want := []*rustPackage{
		{
			name:            "location",
			packageName:     "gcp-sdk-location",
			path:            "src/generated/cloud/location",
			version:         "0.1.0",
			defaultFeatures: true,
		},
		{
			name:            "lro",
			packageName:     "gcp-sdk-lro",
			path:            "src/lro",
			version:         "0.1.0",
			used:            false,
			usedIf:          []string{"lro"},
			defaultFeatures: true,
		},
	}
	less := func(a, b *rustPackage) bool { return a.name < b.name }
	if diff := cmp.Diff(want, c.extraPackages, cmp.AllowUnexported(rustPackage{}), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestRust_NoStreamingFeature(t *testing.T) {
	codec := &rustCodec{
		extraPackages: []*rustPackage{},
	}
	model := api.NewTestAPI([]*api.Message{
		{Name: "CreateResource", IsPageableResponse: false},
	}, []*api.Enum{}, []*api.Service{})
	rustLoadWellKnownTypes(model.State)
	data := &RustTemplateData{}
	rustAddStreamingFeature(data, model, codec.extraPackages)
	if data.HasFeatures {
		t.Errorf("mismatch in data.HasFeatures, expected `HasFeatures: false`, got=%v", data)
	}
}

func TestRust_StreamingFeature(t *testing.T) {
	location := &rustPackage{
		name:        "location",
		packageName: "gcp-sdk-location",
		path:        "src/generated/location",
		used:        true,
	}
	longrunning := &rustPackage{
		name:        "longrunning",
		packageName: "gcp-sdk-longrunning",
		path:        "src/generated/longrunning",
		used:        true,
	}

	location.used = false
	codec := &rustCodec{
		modulePath:    "model",
		extraPackages: []*rustPackage{location},
		packageMapping: map[string]*rustPackage{
			"google.cloud.location": location,
		},
	}
	checkRustContext(t, codec, `unstable-stream = ["gax/unstable-stream"]`)

	location.used = true
	codec = &rustCodec{
		modulePath:    "model",
		extraPackages: []*rustPackage{location},
		packageMapping: map[string]*rustPackage{
			"google.cloud.location": location,
		},
	}
	checkRustContext(t, codec, `unstable-stream = ["gax/unstable-stream", "location/unstable-stream"]`)

	codec = &rustCodec{
		modulePath:    "model",
		extraPackages: []*rustPackage{longrunning},
		packageMapping: map[string]*rustPackage{
			"google.longrunning": longrunning,
		},
	}
	checkRustContext(t, codec, `unstable-stream = ["gax/unstable-stream", "longrunning/unstable-stream"]`)

	codec = &rustCodec{
		modulePath:    "model",
		extraPackages: []*rustPackage{location, longrunning},
		packageMapping: map[string]*rustPackage{
			"google.cloud.location": location,
			"google.longrunning":    longrunning,
		},
	}
	checkRustContext(t, codec, `unstable-stream = ["gax/unstable-stream", "location/unstable-stream", "longrunning/unstable-stream"]`)

}

func checkRustContext(t *testing.T, codec *rustCodec, wantFeatures string) {
	t.Helper()

	model := api.NewTestAPI([]*api.Message{
		{Name: "ListResources", IsPageableResponse: true},
	}, []*api.Enum{}, []*api.Service{})
	rustLoadWellKnownTypes(model.State)
	data := &RustTemplateData{}
	rustAddStreamingFeature(data, model, codec.extraPackages)
	want := []string{wantFeatures}
	if !data.HasFeatures {
		t.Errorf("mismatch in data.HasFeatures, expected `HasFeatures: true`, got=%v", data)
	}

	if diff := cmp.Diff(data.Features, want); diff != "" {
		t.Errorf("mismatch in checkRustContext (-want, +got)\n:%s", diff)
	}
}

func TestRust_WellKnownTypesAsMethod(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	rustLoadWellKnownTypes(model.State)

	want := "wkt::Empty"
	got := rustMethodInOutTypeName(".google.protobuf.Empty", model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}
}

func TestRust_MethodInOut(t *testing.T) {
	message := &api.Message{
		Name: "Target",
		ID:   "..Target",
	}
	nested := &api.Message{
		Name:   "Nested",
		ID:     "..Target.Nested",
		Parent: message,
	}
	model := api.NewTestAPI([]*api.Message{message, nested}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	rustLoadWellKnownTypes(model.State)

	want := "crate::model::Target"
	got := rustMethodInOutTypeName("..Target", model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}

	want = "crate::model::target::Nested"
	got = rustMethodInOutTypeName("..Target.Nested", model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}
}

func TestRust_FieldAttributes(t *testing.T) {
	message := &api.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*api.Field{
			{
				Name:     "f_int64",
				JSONName: "fInt64",
				Typez:    api.INT64_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_int64_optional",
				JSONName: "fInt64Optional",
				Typez:    api.INT64_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_int64_repeated",
				JSONName: "fInt64Repeated",
				Typez:    api.INT64_TYPE,
				Optional: false,
				Repeated: true,
			},

			{
				Name:     "f_bytes",
				JSONName: "fBytes",
				Typez:    api.BYTES_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_bytes_optional",
				JSONName: "fBytesOptional",
				Typez:    api.BYTES_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_bytes_repeated",
				JSONName: "fBytesRepeated",
				Typez:    api.BYTES_TYPE,
				Optional: false,
				Repeated: true,
			},

			{
				Name:     "f_string",
				JSONName: "fString",
				Typez:    api.STRING_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_string_optional",
				JSONName: "fStringOptional",
				Typez:    api.STRING_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_string_repeated",
				JSONName: "fStringRepeated",
				Typez:    api.STRING_TYPE,
				Optional: false,
				Repeated: true,
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"f_int64":          `#[serde_as(as = "serde_with::DisplayFromStr")]`,
		"f_int64_optional": `#[serde(skip_serializing_if = "std::option::Option::is_none")]` + "\n" + `#[serde_as(as = "std::option::Option<serde_with::DisplayFromStr>")]`,
		"f_int64_repeated": `#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]` + "\n" + `#[serde_as(as = "std::vec::Vec<serde_with::DisplayFromStr>")]`,

		"f_bytes":          `#[serde(skip_serializing_if = "bytes::Bytes::is_empty")]` + "\n" + `#[serde_as(as = "serde_with::base64::Base64")]`,
		"f_bytes_optional": `#[serde(skip_serializing_if = "std::option::Option::is_none")]` + "\n" + `#[serde_as(as = "std::option::Option<serde_with::base64::Base64>")]`,
		"f_bytes_repeated": `#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]` + "\n" + `#[serde_as(as = "std::vec::Vec<serde_with::base64::Base64>")]`,

		"f_string":          `#[serde(skip_serializing_if = "std::string::String::is_empty")]`,
		"f_string_optional": `#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
		"f_string_repeated": `#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`,
	}
	rustLoadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(rustFieldAttributes(field, model.State), "\n")
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestRust_MapFieldAttributes(t *testing.T) {
	target := &api.Message{
		Name: "Target",
		ID:   "..Target",
	}
	map1 := &api.Message{
		Name:  "$map<string, string>",
		ID:    "$map<string, string>",
		IsMap: true,
		Fields: []*api.Field{
			{
				Name:  "key",
				Typez: api.STRING_TYPE,
			},
			{
				Name:  "value",
				Typez: api.STRING_TYPE,
			},
		},
	}
	map2 := &api.Message{
		Name:  "$map<string, int64>",
		ID:    "$map<string, int64>",
		IsMap: true,
		Fields: []*api.Field{
			{
				Name:     "key",
				JSONName: "key",
				Typez:    api.STRING_TYPE,
			},
			{
				Name:     "value",
				JSONName: "value",
				Typez:    api.INT64_TYPE,
			},
		},
	}
	map3 := &api.Message{
		Name:  "$map<int64, string>",
		ID:    "$map<int64, string>",
		IsMap: true,
		Fields: []*api.Field{
			{
				Name:  "key",
				Typez: api.INT64_TYPE,
			},
			{
				Name:  "value",
				Typez: api.STRING_TYPE,
			},
		},
	}
	map4 := &api.Message{
		Name:  "$map<string, bytes>",
		ID:    "$map<string, bytes>",
		IsMap: true,
		Fields: []*api.Field{
			{
				Name:  "key",
				Typez: api.STRING_TYPE,
			},
			{
				Name:  "value",
				Typez: api.BYTES_TYPE,
			},
		},
	}
	message := &api.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*api.Field{
			{
				Name:     "target",
				JSONName: "target",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  target.ID,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "map",
				JSONName: "map",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  map1.ID,
			},
			{
				Name:     "map_i64",
				JSONName: "mapI64",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  map2.ID,
			},
			{
				Name:     "map_i64_key",
				JSONName: "mapI64Key",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  map3.ID,
			},
			{
				Name:     "map_bytes",
				JSONName: "mapBytes",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  map4.ID,
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{target, map1, map2, map3, map4, message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"target":      `#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
		"map":         `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`,
		"map_i64":     `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<_, serde_with::DisplayFromStr>")]`,
		"map_i64_key": `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<serde_with::DisplayFromStr, _>")]`,
		"map_bytes":   `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<_, serde_with::base64::Base64>")]`,
	}
	rustLoadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(rustFieldAttributes(field, model.State), "\n")
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestRust_WktFieldAttributes(t *testing.T) {
	message := &api.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*api.Field{
			{
				Name:     "f_int64",
				JSONName: "fInt64",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Int64Value",
				Optional: true,
			},
			{
				Name:     "f_uint64",
				JSONName: "fUint64",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.UInt64Value",
				Optional: true,
			},
			{
				Name:     "f_bytes",
				JSONName: "fBytes",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.BytesValue",
				Optional: true,
			},
			{
				Name:     "f_string",
				JSONName: "fString",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.StringValue",
				Optional: true,
			},
			{
				Name:     "f_repeated_any",
				JSONName: "fRepeatedAny",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Any",
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_any",
				JSONName: "fAny",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Any",
				Optional: true,
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"f_int64":        `#[serde(skip_serializing_if = "std::option::Option::is_none")]` + "\n" + `#[serde_as(as = "std::option::Option<serde_with::DisplayFromStr>")]`,
		"f_uint64":       `#[serde(skip_serializing_if = "std::option::Option::is_none")]` + "\n" + `#[serde_as(as = "std::option::Option<serde_with::DisplayFromStr>")]`,
		"f_bytes":        `#[serde(skip_serializing_if = "std::option::Option::is_none")]` + "\n" + `#[serde_as(as = "std::option::Option<serde_with::base64::Base64>")]`,
		"f_string":       `#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
		"f_repeated_any": `#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`,
		"f_any":          `#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
	}
	rustLoadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(rustFieldAttributes(field, model.State), "\n")
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestRust_FieldLossyName(t *testing.T) {
	message := &api.Message{
		Name:          "SecretPayload",
		ID:            "..SecretPayload",
		Documentation: "A secret payload resource in the Secret Manager API.",
		Fields: []*api.Field{
			{
				Name:          "data",
				JSONName:      "data",
				Documentation: "The secret data. Must be no larger than 64KiB.",
				Typez:         api.BYTES_TYPE,
				TypezID:       "bytes",
			},
			{
				Name:          "dataCrc32c",
				JSONName:      "dataCrc32c",
				Documentation: "Optional. If specified, SecretManagerService will verify the integrity of the received data.",
				Typez:         api.INT64_TYPE,
				TypezID:       "int64",
				Optional:      true,
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"data": `#[serde(skip_serializing_if = "bytes::Bytes::is_empty")]` + "\n" +
			`#[serde_as(as = "serde_with::base64::Base64")]`,
		"dataCrc32c": `#[serde(rename = "dataCrc32c")]` + "\n" +
			`#[serde(skip_serializing_if = "std::option::Option::is_none")]` + "\n" +
			`#[serde_as(as = "std::option::Option<serde_with::DisplayFromStr>")]`,
	}
	rustLoadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(rustFieldAttributes(field, model.State), "\n")
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestRust_SyntheticField(t *testing.T) {
	message := &api.Message{
		Name: "Unused",
		ID:   "..Unused",
		Fields: []*api.Field{
			{
				Name:     "updateMask",
				JSONName: "updateMask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
			{
				Name:      "project",
				JSONName:  "project",
				Typez:     api.STRING_TYPE,
				TypezID:   "string",
				Synthetic: true,
			},
			{
				Name:      "data_crc32c",
				JSONName:  "dataCrc32c",
				Typez:     api.STRING_TYPE,
				TypezID:   "string",
				Synthetic: true,
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"updateMask":  `#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
		"project":     `#[serde(skip)]`,
		"data_crc32c": `#[serde(skip)]`,
	}
	rustLoadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(rustFieldAttributes(field, model.State), "\n")
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestRust_FieldType(t *testing.T) {
	target := &api.Message{
		Name: "Target",
		ID:   "..Target",
	}
	message := &api.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*api.Field{
			{
				Name:     "f_int32",
				Typez:    api.INT32_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_int32_optional",
				Typez:    api.INT32_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_int32_repeated",
				Typez:    api.INT32_TYPE,
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_string",
				Typez:    api.STRING_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_string_optional",
				Typez:    api.STRING_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_string_repeated",
				Typez:    api.STRING_TYPE,
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_msg",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_msg_repeated",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: false,
				Repeated: true,
			},
			{
				Name:      "f_msg_recursive",
				Typez:     api.MESSAGE_TYPE,
				TypezID:   "..Fake",
				Optional:  true,
				Repeated:  false,
				Recursive: true,
			},
			{
				Name:      "f_msg_recursive_repeated",
				Typez:     api.MESSAGE_TYPE,
				TypezID:   "..Fake",
				Optional:  false,
				Repeated:  true,
				Recursive: true,
			},
			{
				Name:     "f_timestamp",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_timestamp_repeated",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: false,
				Repeated: true,
			},
		},
	}
	model := api.NewTestAPI([]*api.Message{target, message}, []*api.Enum{}, []*api.Service{})

	expectedTypes := map[string]string{
		"f_int32":                  "i32",
		"f_int32_optional":         "std::option::Option<i32>",
		"f_int32_repeated":         "std::vec::Vec<i32>",
		"f_string":                 "std::string::String",
		"f_string_optional":        "std::option::Option<std::string::String>",
		"f_string_repeated":        "std::vec::Vec<std::string::String>",
		"f_msg":                    "std::option::Option<crate::model::Target>",
		"f_msg_repeated":           "std::vec::Vec<crate::model::Target>",
		"f_msg_recursive":          "std::option::Option<std::boxed::Box<crate::model::Fake>>",
		"f_msg_recursive_repeated": "std::vec::Vec<crate::model::Fake>",
		"f_timestamp":              "std::option::Option<wkt::Timestamp>",
		"f_timestamp_repeated":     "std::vec::Vec<wkt::Timestamp>",
	}
	expectedPrimitiveTypes := map[string]string{
		"f_int32":                  "i32",
		"f_int32_optional":         "i32",
		"f_int32_repeated":         "i32",
		"f_string":                 "std::string::String",
		"f_string_optional":        "std::string::String",
		"f_string_repeated":        "std::string::String",
		"f_msg":                    "crate::model::Target",
		"f_msg_repeated":           "crate::model::Target",
		"f_msg_recursive":          "crate::model::Fake",
		"f_msg_recursive_repeated": "crate::model::Fake",
		"f_timestamp":              "wkt::Timestamp",
		"f_timestamp_repeated":     "wkt::Timestamp",
	}
	c := createRustCodec()
	rustLoadWellKnownTypes(model.State)
	for _, field := range message.Fields {
		want, ok := expectedTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := rustFieldType(field, model.State, false, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}

		want, ok = expectedPrimitiveTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got = rustFieldType(field, model.State, true, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

// Verify rustBaseFieldType works for map types with different value fields.
func TestRust_FieldMapTypeValues(t *testing.T) {
	for _, test := range []struct {
		want  string
		value *api.Field
	}{
		{
			"std::collections::HashMap<i32,std::string::String>",
			&api.Field{Typez: api.STRING_TYPE},
		},
		{
			"std::collections::HashMap<i32,i64>",
			&api.Field{Typez: api.INT64_TYPE},
		},
		{
			"std::collections::HashMap<i32,wkt::Any>",
			&api.Field{Typez: api.MESSAGE_TYPE, TypezID: ".google.protobuf.Any"},
		},
		{
			"std::collections::HashMap<i32,crate::model::OtherMessage>",
			&api.Field{Typez: api.MESSAGE_TYPE, TypezID: ".test.OtherMessage"},
		},
		{
			"std::collections::HashMap<i32,crate::model::Message>",
			&api.Field{Typez: api.MESSAGE_TYPE, TypezID: ".test.Message"},
		},
	} {
		field := &api.Field{
			Name:    "indexed",
			ID:      ".test.Message.indexed",
			Typez:   api.MESSAGE_TYPE,
			TypezID: ".test.$MapThing",
		}
		other_message := &api.Message{
			Name:   "OtherMessage",
			ID:     ".test.OtherMessage",
			IsMap:  true,
			Fields: []*api.Field{},
		}
		message := &api.Message{
			Name:   "Message",
			ID:     ".test.Message",
			IsMap:  true,
			Fields: []*api.Field{field},
		}
		// Complete the value field
		value := test.value
		value.Name = "value"
		value.ID = ".test.$MapThing.value"
		key := &api.Field{
			Name:  "key",
			ID:    ".test.$MapThing.key",
			Typez: api.INT32_TYPE,
		}
		map_thing := &api.Message{
			Name:   "$MapThing",
			ID:     ".test.$MapThing",
			IsMap:  true,
			Fields: []*api.Field{key, value},
		}
		model := api.NewTestAPI([]*api.Message{message, other_message, map_thing}, []*api.Enum{}, []*api.Service{})
		api.LabelRecursiveFields(model)
		c := createRustCodec()
		rustLoadWellKnownTypes(model.State)
		got := rustFieldType(field, model.State, false, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
		if got != test.want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, test.want)
		}
	}
}

// Verify rustBaseFieldType works for map types with different key fields.
func TestRust_FieldMapTypeKey(t *testing.T) {
	for _, test := range []struct {
		want string
		key  *api.Field
	}{
		{
			"std::collections::HashMap<i32,i64>",
			&api.Field{Typez: api.INT32_TYPE},
		},
		{
			"std::collections::HashMap<std::string::String,i64>",
			&api.Field{Typez: api.STRING_TYPE},
		},
		{
			"std::collections::HashMap<crate::model::EnumType,i64>",
			&api.Field{Typez: api.ENUM_TYPE, TypezID: ".test.EnumType"},
		},
	} {
		field := &api.Field{
			Name:    "indexed",
			ID:      ".test.Message.indexed",
			Typez:   api.MESSAGE_TYPE,
			TypezID: ".test.$MapThing",
		}
		message := &api.Message{
			Name:   "Message",
			ID:     ".test.Message",
			IsMap:  true,
			Fields: []*api.Field{field},
		}
		// Complete the value field
		key := test.key
		key.Name = "key"
		key.ID = ".test.$MapThing.key"
		value := &api.Field{
			Name:  "value",
			ID:    ".test.$MapThing.value",
			Typez: api.INT64_TYPE,
		}
		map_thing := &api.Message{
			Name:   "$MapThing",
			ID:     ".test.$MapThing",
			IsMap:  true,
			Fields: []*api.Field{key, value},
		}
		enum := &api.Enum{
			Name: "EnumType",
			ID:   ".test.EnumType",
		}
		model := api.NewTestAPI([]*api.Message{message, map_thing}, []*api.Enum{enum}, []*api.Service{})
		api.LabelRecursiveFields(model)
		c := createRustCodec()
		rustLoadWellKnownTypes(model.State)
		got := rustFieldType(field, model.State, false, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
		if got != test.want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, test.want)
		}
	}
}

func TestRust_AsQueryParameter(t *testing.T) {
	options := &api.Message{
		Name:   "Options",
		ID:     "..Options",
		Fields: []*api.Field{},
	}
	optionsField := &api.Field{
		Name:     "options_field",
		JSONName: "optionsField",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  options.ID,
		Optional: true,
	}
	requiredField := &api.Field{
		Name:     "required_field",
		JSONName: "requiredField",
		Typez:    api.STRING_TYPE,
	}
	optionalField := &api.Field{
		Name:     "optional_field",
		JSONName: "optionalField",
		Typez:    api.STRING_TYPE,
		Optional: true,
	}
	repeatedField := &api.Field{
		Name:     "repeated_field",
		JSONName: "repeatedField",
		Typez:    api.STRING_TYPE,
		Repeated: true,
	}

	requiredEnumField := &api.Field{
		Name:     "required_enum_field",
		JSONName: "requiredEnumField",
		Typez:    api.ENUM_TYPE,
	}
	optionalEnumField := &api.Field{
		Name:     "optional_enum_field",
		JSONName: "optionalEnumField",
		Typez:    api.ENUM_TYPE,
		Optional: true,
	}
	repeatedEnumField := &api.Field{
		Name:     "repeated_enum_field",
		JSONName: "repeatedEnumField",
		Typez:    api.ENUM_TYPE,
		Repeated: true,
	}

	requiredFieldMaskField := &api.Field{
		Name:     "required_field_mask",
		JSONName: "requiredFieldMask",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".google.protobuf.FieldMask",
	}
	optionalFieldMaskField := &api.Field{
		Name:     "optional_field_mask",
		JSONName: "optionalFieldMask",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".google.protobuf.FieldMask",
		Optional: true,
	}
	request := &api.Message{
		Name: "TestRequest",
		ID:   "..TestRequest",
		Fields: []*api.Field{
			optionsField,
			requiredField, optionalField, repeatedField,
			requiredEnumField, optionalEnumField, repeatedEnumField,
			requiredFieldMaskField, optionalFieldMaskField,
		},
	}
	model := api.NewTestAPI(
		[]*api.Message{options, request},
		[]*api.Enum{},
		[]*api.Service{})
	rustLoadWellKnownTypes(model.State)

	for _, test := range []struct {
		field *api.Field
		want  string
	}{
		{optionsField, `let builder = req.options_field.as_ref().map(|p| serde_json::to_value(p).map_err(Error::serde) ).transpose()?.into_iter().fold(builder, |builder, v| { use gax::query_parameter::QueryParameter; v.add(builder, "optionsField") });`},
		{requiredField, `let builder = builder.query(&[("requiredField", &req.required_field)]);`},
		{optionalField, `let builder = req.optional_field.iter().fold(builder, |builder, p| builder.query(&[("optionalField", p)]));`},
		{repeatedField, `let builder = req.repeated_field.iter().fold(builder, |builder, p| builder.query(&[("repeatedField", p)]));`},
		{requiredEnumField, `let builder = builder.query(&[("requiredEnumField", &req.required_enum_field.value())]);`},
		{optionalEnumField, `let builder = req.optional_enum_field.iter().fold(builder, |builder, p| builder.query(&[("optionalEnumField", p.value())]));`},
		{repeatedEnumField, `let builder = req.repeated_enum_field.iter().fold(builder, |builder, p| builder.query(&[("repeatedEnumField", p.value())]));`},
		{requiredFieldMaskField, `let builder = { use gax::query_parameter::QueryParameter; serde_json::to_value(&req.required_field_mask).map_err(Error::serde)?.add(builder, "requiredFieldMask") };`},
		{optionalFieldMaskField, `let builder = req.optional_field_mask.as_ref().map(|p| serde_json::to_value(p).map_err(Error::serde) ).transpose()?.into_iter().fold(builder, |builder, v| { use gax::query_parameter::QueryParameter; v.add(builder, "optionalFieldMask") });`},
	} {
		got := rustAddQueryParameter(test.field)
		if test.want != got {
			t.Errorf("mismatched as query parameter for %s\nwant=%s\n got=%s", test.field.Name, test.want, got)
		}
	}
}

type rustCaseConvertTest struct {
	Input    string
	Expected string
}

func TestRust_ToSnake(t *testing.T) {
	var snakeConvertTests = []rustCaseConvertTest{
		{"FooBar", "foo_bar"},
		{"foo_bar", "foo_bar"},
		{"data_crc32c", "data_crc32c"},
		{"True", "r#true"},
		{"Static", "r#static"},
		{"Trait", "r#trait"},
		{"Self", "r#self"},
		{"self", "r#self"},
		{"yield", "r#yield"},
	}
	for _, test := range snakeConvertTests {
		if output := rustToSnake(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestRust_ToScreamingSnake(t *testing.T) {
	var snakeConvertTests = []rustCaseConvertTest{
		{"FooBar", "FOO_BAR"},
		{"FOO_BAR", "FOO_BAR"},
		{"week5", "WEEK_5"},
		{"TYPE_INT64", "TYPE_INT64"},
	}
	for _, test := range snakeConvertTests {
		if output := rustToScreamingSnake(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestRust_ToPascal(t *testing.T) {
	var pascalConvertTests = []rustCaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"Self", "r#Self"},
		{"self", "r#Self"},
		{"yield", "Yield"},
		{"IAMPolicy", "IAMPolicy"},
		{"IAMPolicyRequest", "IAMPolicyRequest"},
		{"IAM", "Iam"},
	}
	for _, test := range pascalConvertTests {
		if output := rustToPascal(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q", output, test.Expected)
		}
	}
}

func TestRust_FormatDocComments(t *testing.T) {
	input := `Some comments describing the thing.

The next line has some extra trailing whitespace:` + "   " + `

We want to respect whitespace at the beginning, because it important in Markdown:

- A thing
  - A nested thing
- The next thing

Now for some fun with block quotes

` + "```" + `
Maybe they wanted to show some JSON:
{
  "foo": "bar"
}
` + "```"

	want := []string{
		"/// Some comments describing the thing.",
		"///",
		"/// The next line has some extra trailing whitespace:",
		"///",
		"/// We want to respect whitespace at the beginning, because it important in Markdown:",
		"///",
		"/// - A thing",
		"///   - A nested thing",
		"/// - The next thing",
		"///",
		"/// Now for some fun with block quotes",
		"///",
		"/// ```norust",
		"/// Maybe they wanted to show some JSON:",
		"/// {",
		`///   "foo": "bar"`,
		"/// }",
		"/// ```",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &rustCodec{}
	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_FormatDocCommentsBullets(t *testing.T) {
	input := `In this example, in proto field could take one of the following values:

* full_name for a violation in the full_name value
* email_addresses[1].email for a violation in the email field of the
  first email_addresses message
* email_addresses[3].type[2] for a violation in the second type
  value in the third email_addresses message.)`
	want := []string{
		"/// In this example, in proto field could take one of the following values:",
		"///",
		"/// * full_name for a violation in the full_name value",
		"/// * email_addresses[1].email for a violation in the email field of the",
		"///   first email_addresses message",
		"/// * email_addresses[3].type[2] for a violation in the second type",
		"///   value in the third email_addresses message.)",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_FormatDocCommentsImplicitBlockQuote(t *testing.T) {
	input := `
Blockquotes come in many forms. They can start with a leading '> ', as in:

> Block quote style 1
> Continues 1 - style 1
> Continues 2 - style 1
> Continues 3 - style 1

They can start with 3 spaces and then '> ', as in:

   > Block quote style 2
   > Continues 1 - style 2
   > Continues 2 - style 2
   > Continues 3 - style 2

Or they can start with just 4 spaces:

    Block quote style 3
    Continues 1 - style 3
    Continues 2 - style 3
    Continues 3 - style 3

Note that four spaces and a leading '> ' makes the '> ' prefix part of the
block:

    > Block quote with arrow.
    > Continues 1 - with arrow
    > Continues 2 - with arrow
    Continues 3 - with arrow

`

	want := []string{
		"/// Blockquotes come in many forms. They can start with a leading '> ', as in:",
		"///",
		"/// Block quote style 1",
		"/// Continues 1 - style 1",
		"/// Continues 2 - style 1",
		"/// Continues 3 - style 1",
		"///",
		"/// They can start with 3 spaces and then '> ', as in:",
		"///",
		"/// Block quote style 2",
		"/// Continues 1 - style 2",
		"/// Continues 2 - style 2",
		"/// Continues 3 - style 2",
		"///",
		"/// Or they can start with just 4 spaces:",
		"///",
		"/// ```norust",
		"/// Block quote style 3",
		"/// Continues 1 - style 3",
		"/// Continues 2 - style 3",
		"/// Continues 3 - style 3",
		"/// ```",
		"///",
		"/// Note that four spaces and a leading '> ' makes the '> ' prefix part of the",
		"/// block:",
		"///",
		"/// ```norust",
		"/// > Block quote with arrow.",
		"/// > Continues 1 - with arrow",
		"/// > Continues 2 - with arrow",
		"/// Continues 3 - with arrow",
		"/// ```",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &rustCodec{}
	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_FormatDocCommentsImplicitBlockQuoteClosing(t *testing.T) {
	input := `Blockquotes can appear at the end of the comment:

    they should have a closing element.`

	want := []string{
		"/// Blockquotes can appear at the end of the comment:",
		"///",
		"/// ```norust",
		"/// they should have a closing element.",
		"/// ```",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &rustCodec{}
	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_FormatDocCommentsCrossLinks(t *testing.T) {
	input := `
[Any][google.protobuf.Any]
[Message][test.v1.SomeMessage]
[Enum][test.v1.SomeMessage.SomeEnum]
[Message][test.v1.SomeMessage] repeated
[Service][test.v1.SomeService] [field][test.v1.SomeMessage.field]
[SomeMessage.error][test.v1.SomeMessage.error]
[ExternalMessage][google.iam.v1.SetIamPolicyRequest]
[ExternalService][google.iam.v1.Iampolicy]
[ENUM_VALUE][test.v1.SomeMessage.SomeEnum.ENUM_VALUE]
[SomeService.CreateFoo][test.v1.SomeService.CreateFoo]
[SomeService.CreateBar][test.v1.SomeService.CreateBar]
[a method][test.v1.YELL.CreateThing]
[the service name][test.v1.YELL]
`
	want := []string{
		"/// [Any][google.protobuf.Any]",
		"/// [Message][test.v1.SomeMessage]",
		"/// [Enum][test.v1.SomeMessage.SomeEnum]",
		"/// [Message][test.v1.SomeMessage] repeated",
		"/// [Service][test.v1.SomeService] [field][test.v1.SomeMessage.field]",
		"/// [SomeMessage.error][test.v1.SomeMessage.error]",
		"/// [ExternalMessage][google.iam.v1.SetIamPolicyRequest]",
		"/// [ExternalService][google.iam.v1.Iampolicy]",
		"/// [ENUM_VALUE][test.v1.SomeMessage.SomeEnum.ENUM_VALUE]",
		"/// [SomeService.CreateFoo][test.v1.SomeService.CreateFoo]",
		"/// [SomeService.CreateBar][test.v1.SomeService.CreateBar]",
		"/// [a method][test.v1.YELL.CreateThing]",
		"/// [the service name][test.v1.YELL]",
		"///",
		"/// [google.iam.v1.Iampolicy]: iam_v1::client::Iampolicy",
		"/// [google.iam.v1.SetIamPolicyRequest]: iam_v1::model::SetIamPolicyRequest",
		"/// [google.protobuf.Any]: wkt::Any",
		"/// [test.v1.SomeMessage]: crate::model::SomeMessage",
		"/// [test.v1.SomeMessage.SomeEnum]: crate::model::some_message::SomeEnum",
		"/// [test.v1.SomeMessage.SomeEnum.ENUM_VALUE]: crate::model::some_message::some_enum::ENUM_VALUE",
		"/// [test.v1.SomeMessage.error]: crate::model::SomeMessage::result",
		"/// [test.v1.SomeMessage.field]: crate::model::SomeMessage::field",
		"/// [test.v1.SomeService]: crate::client::SomeService",
		// Skipped because the method is skipped
		// "/// [test.v1.SomeService.CreateBar]: crate::client::SomeService::create_bar",
		"/// [test.v1.SomeService.CreateFoo]: crate::client::SomeService::create_foo",
		// Services named with all uppercase have a different mapping.
		"/// [test.v1.YELL]: crate::client::Yell",
		"/// [test.v1.YELL.CreateThing]: crate::client::Yell::create_thing",
	}

	wkt := &rustPackage{
		name:        "wkt",
		packageName: "gcp-sdk-wkt",
		path:        "src/wkt",
	}
	iam := &rustPackage{
		name:        "iam_v1",
		packageName: "gcp-sdk-iam-v1",
		path:        "src/generated/iam/v1",
	}
	c := &rustCodec{
		modulePath: "crate::model",
		packageMapping: map[string]*rustPackage{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex model.API instance. Create it
	// in a separate function to make this more readable.
	model := makeApiForRustFormatDocCommentsCrossLinks()
	rustLoadWellKnownTypes(model.State)

	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_FormatDocCommentsLinkDefinitions(t *testing.T) {
	input := `Link definitions should be added when collapsed links are used.
For example, [google][].
Second [example][].
[Third] example.
[google]: https://www.google.com
[example]: https://www.example.com
[Third]: https://www.third.com`

	want := []string{
		"/// Link definitions should be added when collapsed links are used.",
		"/// For example, [google][].",
		"/// Second [example][].",
		"/// [Third] example.",
		"/// [google]: https://www.google.com",
		"/// [example]: https://www.example.com",
		"/// [Third]: https://www.third.com",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &rustCodec{}
	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_FormatDocCommentsHTMLTags(t *testing.T) {
	input := `Placeholders placed between angled brackets should be escaped.
	For example, example:<ip address> and another example:<second
	placeholder>.
	Third example: projects/<project>/secrets/<secret>
	Urls remain unchanged <https://www.example.com>
	Hyperlinks remain unchanged <a href=https://www.hyperlink.com>hyperlined content</a>` + `
	HTML tags within code spans remain unchanged secret ` + "`" + `secrets/<secret>` + "`"

	want := []string{
		"/// Placeholders placed between angled brackets should be escaped.",
		"/// For example, example:\\<ip address\\> and another example:\\<second",
		"/// placeholder\\>.",
		"/// Third example: projects/\\<project\\>/secrets/\\<secret\\>",
		"/// Urls remain unchanged <https://www.example.com>",
		"/// Hyperlinks remain unchanged <a href=<https://www.hyperlink.com>>hyperlined content</a>",
		"/// HTML tags within code spans remain unchanged secret `secrets/<secret>`",
	}

	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &rustCodec{}
	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func makeApiForRustFormatDocCommentsCrossLinks() *api.API {
	enumValue := &api.EnumValue{
		Name: "ENUM_VALUE",
		ID:   ".test.v1.SomeMessage.SomeEnum.ENUM_VALUE",
	}
	someEnum := &api.Enum{
		Name:   "SomeEnum",
		ID:     ".test.v1.SomeMessage.SomeEnum",
		Values: []*api.EnumValue{enumValue},
	}
	enumValue.Parent = someEnum
	response := &api.Field{
		Name:    "response",
		ID:      ".test.v1.SomeMessage.response",
		IsOneOf: true,
	}
	errorz := &api.Field{
		Name:    "error",
		ID:      ".test.v1.SomeMessage.error",
		IsOneOf: true,
	}
	someMessage := &api.Message{
		Name:  "SomeMessage",
		ID:    ".test.v1.SomeMessage",
		Enums: []*api.Enum{someEnum},
		Fields: []*api.Field{
			{Name: "unused"}, {Name: "field"}, response, errorz,
		},
		OneOfs: []*api.OneOf{
			{
				Name:   "result",
				ID:     ".test.v1.SomeMessage.result",
				Fields: []*api.Field{response, errorz},
			},
		},
	}
	someService := &api.Service{
		Name: "SomeService",
		ID:   ".test.v1.SomeService",
		Methods: []*api.Method{
			{
				Name: "CreateFoo", ID: ".test.v1.SomeService.CreateFoo",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("/v1/foo"),
					},
				},
			},
			{Name: "CreateBar", ID: ".test.v1.SomeService.CreateBar"},
		},
	}
	yellyService := &api.Service{
		Name: "YELL",
		ID:   ".test.v1.YELL",
		Methods: []*api.Method{
			{
				Name: "CreateThing",
				ID:   ".test.v1.YELL.CreateThing",
				PathInfo: &api.PathInfo{
					Verb: "GET",
					PathTemplate: []api.PathSegment{
						api.NewLiteralPathSegment("/v1/thing"),
					},
				},
			},
		},
	}
	a := api.NewTestAPI(
		[]*api.Message{someMessage},
		[]*api.Enum{someEnum},
		[]*api.Service{someService, yellyService})
	a.PackageName = "test.v1"
	a.State.MessageByID[".google.iam.v1.SetIamPolicyRequest"] = &api.Message{
		Name:    "SetIamPolicyRequest",
		Package: "google.iam.v1",
		ID:      ".google.iam.v1.SetIamPolicyRequest",
	}
	a.State.ServiceByID[".google.iam.v1.Iampolicy"] = &api.Service{
		Name:    "Iampolicy",
		Package: "google.iam.v1",
		ID:      ".google.iam.v1.Iampolicy",
	}
	return a
}

func TestRust_FormatDocCommentsUrls(t *testing.T) {
	input := `
blah blah https://cloud.google.com foo bar
[link](https://example1.com)
<https://example2.com>
<https://example3.com>.
https://example4.com.
https://example5.com https://cloud.google.com something else.
[link definition]: https://example6.com/
not a definition: https://example7.com/
Quoted URL: "https://example8.com"
Trailing Slash https://example9.com/
http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html
http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
https://cloud.google.com/apis/design/design_patterns#integer_types
https://cloud.google.com/apis/design/design_patterns#integer_types.`
	want := []string{
		"/// blah blah <https://cloud.google.com> foo bar",
		"/// [link](https://example1.com)",
		"/// <https://example2.com>",
		"/// <https://example3.com>.",
		"/// <https://example4.com>.",
		"/// <https://example5.com> <https://cloud.google.com> something else.",
		"/// [link definition]: https://example6.com/",
		"/// not a definition: <https://example7.com/>",
		"/// Quoted URL: `https://example8.com`",
		"/// Trailing Slash <https://example9.com/>",
		"/// <http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html>",
		"/// <http://www.unicode.org/reports/tr35/#Unicode_locale_identifier>.",
		"/// <https://cloud.google.com/apis/design/design_patterns#integer_types>",
		"/// <https://cloud.google.com/apis/design/design_patterns#integer_types>.",
	}

	wkt := &rustPackage{
		name:        "wkt",
		packageName: "gcp-sdk-wkt",
		path:        "src/wkt",
	}
	iam := &rustPackage{
		name:        "iam_v1",
		packageName: "gcp-sdk-iam-v1",
		path:        "src/generated/iam/v1",
	}
	c := &rustCodec{
		modulePath: "model",
		packageMapping: map[string]*rustPackage{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex model.API instance. Create it
	// in a separate function to make this more readable.
	model := makeApiForRustFormatDocCommentsCrossLinks()
	rustLoadWellKnownTypes(model.State)

	got := rustFormatDocComments(input, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_MessageNames(t *testing.T) {
	r := sample.Replication()
	a := sample.Automatic()
	model := api.NewTestAPI([]*api.Message{r, a}, []*api.Enum{}, []*api.Service{})
	model.PackageName = "test"

	c := createRustCodec()
	c.sourceSpecificationPackageName = model.Messages[0].Package
	if err := rustValidate(model, c.sourceSpecificationPackageName); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		m    *api.Message
		want string
	}{
		{
			m:    r,
			want: "crate::model::Replication",
		},
		{
			m:    a,
			want: "crate::model::replication::Automatic",
		},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := rustFQMessageName(test.m, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping); got != test.want {
				t.Errorf("mismatched message name, got=%q, want=%q", got, test.want)
			}
		})
	}
}

func TestRust_EnumNames(t *testing.T) {
	parent := &api.Message{
		Name:    "SecretVersion",
		ID:      ".test.SecretVersion",
		Package: "test",
		Fields: []*api.Field{
			{
				Name:     "automatic",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".test.Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &api.Enum{
		Name:    "State",
		ID:      ".test.SecretVersion.State",
		Parent:  parent,
		Package: "test",
	}
	non_nested := &api.Enum{
		Name:    "Code",
		ID:      ".test.Code",
		Package: "test",
	}

	model := api.NewTestAPI([]*api.Message{parent}, []*api.Enum{nested, non_nested}, []*api.Service{})
	model.PackageName = "test"
	c := createRustCodec()
	c.sourceSpecificationPackageName = model.Messages[0].Package
	if err := rustValidate(model, c.sourceSpecificationPackageName); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		enum                 *api.Enum
		wantEnum, wantFQEnum string
	}{
		{nested, "State", "crate::model::secret_version::State"},
		{non_nested, "Code", "crate::model::Code"},
	} {
		if got := rustEnumName(test.enum); got != test.wantEnum {
			t.Errorf("c.enumName(%q) = %q; want = %s", test.enum.Name, got, test.wantEnum)
		}
		if got := rustFQEnumName(test.enum, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping); got != test.wantFQEnum {
			t.Errorf("c.fqEnumName(%q) = %q; want = %s", test.enum.Name, got, test.wantFQEnum)
		}
	}
}

func Test_RustPathFmt(t *testing.T) {
	for _, test := range []struct {
		want     string
		pathInfo *api.PathInfo
	}{
		{
			"/v1/fixed",
			&api.PathInfo{
				PathTemplate: []api.PathSegment{api.NewLiteralPathSegment("v1"), api.NewLiteralPathSegment("fixed")},
			},
		},
		{
			"/v1/{}",
			&api.PathInfo{
				PathTemplate: []api.PathSegment{api.NewLiteralPathSegment("v1"), api.NewFieldPathPathSegment("parent")},
			},
		},
		{
			"/v1/{}:action",
			&api.PathInfo{
				PathTemplate: []api.PathSegment{api.NewLiteralPathSegment("v1"), api.NewFieldPathPathSegment("parent"), api.NewVerbPathSegment("action")},
			},
		},
		{
			"/v1/projects/{}/locations/{}/secrets/{}:action",
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewLiteralPathSegment("projects"),
					api.NewFieldPathPathSegment("project"),
					api.NewLiteralPathSegment("locations"),
					api.NewFieldPathPathSegment("location"),
					api.NewLiteralPathSegment("secrets"),
					api.NewFieldPathPathSegment("secret"),
					api.NewVerbPathSegment("action"),
				},
			},
		},
	} {
		got := rustHTTPPathFmt(test.pathInfo)
		if test.want != got {
			t.Errorf("mismatched path info fmt for %v\nwant=%s\n got=%s", test.pathInfo, test.want, got)
		}
	}

}

func Test_RustPathArgs(t *testing.T) {
	subMessage := &api.Message{
		Name: "Body",
		ID:   ".test.Body",
		Fields: []*api.Field{
			{Name: "a", Typez: api.STRING_TYPE},
			{Name: "b", Typez: api.STRING_TYPE, Optional: true},
			{Name: "c", Typez: api.ENUM_TYPE},
			{Name: "d", Typez: api.ENUM_TYPE, Optional: true},
		},
	}
	message := &api.Message{
		Name: "CreateResourceRequest",
		ID:   ".test.CreateResourceRequest",
		Fields: []*api.Field{
			{Name: "a", Typez: api.STRING_TYPE},
			{Name: "b", Typez: api.STRING_TYPE, Optional: true},
			{Name: "c", Typez: api.ENUM_TYPE},
			{Name: "d", Typez: api.ENUM_TYPE, Optional: true},
			{Name: "e", Typez: api.MESSAGE_TYPE, TypezID: ".test.Body", Optional: true},
		},
	}
	method := &api.Method{
		Name:        "CreateResource",
		InputTypeID: ".test.CreateResourceRequest",
	}
	service := &api.Service{
		Name:    "TestService",
		ID:      ".test.Service",
		Methods: []*api.Method{method},
	}
	model := api.NewTestAPI([]*api.Message{subMessage, message}, []*api.Enum{}, []*api.Service{service})

	for _, test := range []struct {
		want     []string
		pathInfo *api.PathInfo
	}{
		{
			nil,
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
				},
			},
		},
		{
			[]string{".a"},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("a")},
			},
		},
		{
			[]string{`.b.as_ref().ok_or_else(|| gax::path_parameter::missing("b"))?`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("b"),
				},
			},
		},
		{
			[]string{`.c.value()`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("c"),
				},
			},
		},
		{
			[]string{`.d.as_ref().ok_or_else(|| gax::path_parameter::missing("d"))?.value()`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("d"),
				},
			},
		},
		{
			[]string{`.e.as_ref().ok_or_else(|| gax::path_parameter::missing("e"))?.a`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("e.a"),
				},
			},
		},
		{
			[]string{`.e.as_ref().ok_or_else(|| gax::path_parameter::missing("e"))?` +
				`.b.as_ref().ok_or_else(|| gax::path_parameter::missing("b"))?`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("e.b"),
				},
			},
		},
		{
			[]string{`.e.as_ref().ok_or_else(|| gax::path_parameter::missing("e"))?` +
				`.c.value()`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("e.c"),
				},
			},
		},
		{
			[]string{`.e.as_ref().ok_or_else(|| gax::path_parameter::missing("e"))?` +
				`.d.as_ref().ok_or_else(|| gax::path_parameter::missing("d"))?` +
				`.value()`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("e.d"),
				},
			},
		},
		{
			[]string{".a", `.b.as_ref().ok_or_else(|| gax::path_parameter::missing("b"))?`},
			&api.PathInfo{
				PathTemplate: []api.PathSegment{
					api.NewLiteralPathSegment("v1"),
					api.NewFieldPathPathSegment("a"),
					api.NewFieldPathPathSegment("b"),
				},
			},
		},
	} {
		// Modify the method to match the test case.
		method.PathInfo = test.pathInfo
		got := rustHTTPPathArgs(test.pathInfo, method, model.State)
		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Errorf("mismatched path info args (-want, +got):\n%s", diff)
		}
	}
}
