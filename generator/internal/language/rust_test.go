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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func createRustCodec() *RustCodec {
	wkt := &RustPackage{
		Name:    "gax_wkt",
		Package: "types",
		Path:    "../../types",
	}

	return &RustCodec{
		ModulePath:    "model",
		ExtraPackages: []*RustPackage{wkt},
		PackageMapping: map[string]*RustPackage{
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
	}
	codec, err := NewRustCodec("", options)
	if err != nil {
		t.Fatal(err)
	}
	gp := &RustPackage{
		Name:    "wkt",
		Package: "types",
		Path:    "src/wkt",
	}
	want := &RustCodec{
		Version:                  "1.2.3",
		PackageNameOverride:      "test-only",
		GenerationYear:           "2035",
		ModulePath:               "alternative::generated",
		DeserializeWithdDefaults: true,
		ExtraPackages: []*RustPackage{
			gp,
			{
				Name:    "gax",
				Package: "gax",
				Path:    "src/gax",
				Features: []string{
					"unstable-sdk-client",
				},
			},
		},
		PackageMapping: map[string]*RustPackage{
			"google.protobuf": gp,
			"test-only":       gp,
		},
	}
	if diff := cmp.Diff(want, codec, cmpopts.IgnoreFields(RustCodec{}, "ExtraPackages", "PackageMapping")); diff != "" {
		t.Errorf("codec mismatch (-want, +got):\n%s", diff)
	}
	if want.PackageNameOverride != codec.PackageNameOverride {
		t.Errorf("mismatched in packageNameOverride, want=%s, got=%s", want.PackageNameOverride, codec.PackageNameOverride)
	}
	checkRustPackages(t, codec, want)
}

func TestRust_RequiredPackages(t *testing.T) {
	outdir := "src/generated/newlib"
	options := map[string]string{
		"package:gtype": "package=types,path=src/generated/type,source=google.type,source=test-only",
		"package:gax":   "package=gax,path=src/gax,version=1.2.3,force-used=true",
		"package:auth":  "ignore=true",
	}
	codec, err := NewRustCodec(outdir, options)
	if err != nil {
		t.Fatal(err)
	}
	got := codec.RequiredPackages()
	want := []string{
		"gax        = { version = \"1.2.3\", path = \"../../../src/gax\", package = \"gax\" }",
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
	codec, err := NewRustCodec("", options)
	if err != nil {
		t.Fatal(err)
	}
	got := codec.RequiredPackages()
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
	codec, err := NewRustCodec("", opts)
	if err != nil {
		t.Fatal(err)
	}
	got := codec.PackageName(api)
	if want != got {
		t.Errorf("mismatch in package name, want=%s, got=%s", want, got)
	}

}

func checkRustPackages(t *testing.T, got *RustCodec, want *RustCodec) {
	t.Helper()
	less := func(a, b *RustPackage) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.ExtraPackages, got.ExtraPackages, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("package mismatch (-want, +got):\n%s", diff)
	}
}

func TestRust_Validate(t *testing.T) {
	api := newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	c := &RustCodec{}
	if err := c.Validate(api); err != nil {
		t.Errorf("unexpected error in API validation %q", err)
	}
	if c.SourceSpecificationPackageName != "p1" {
		t.Errorf("mismatched source package name, want=p1, got=%s", c.SourceSpecificationPackageName)
	}
}

func TestRust_ValidateMessageMismatch(t *testing.T) {
	test := newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	c := &RustCodec{}
	if err := c.Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}

	test = newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*api.Service{{Name: "s1", Package: "p1"}})
	c = &RustCodec{}
	if err := c.Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}

	test = newTestAPI(
		[]*api.Message{{Name: "m1", Package: "p1"}},
		[]*api.Enum{{Name: "e1", Package: "p1"}},
		[]*api.Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	c = &RustCodec{}
	if err := c.Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}
}

func TestWellKnownTypesExist(t *testing.T) {
	api := newTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &RustCodec{}
	c.LoadWellKnownTypes(api.State)
	for _, name := range []string{"Any", "Duration", "Empty", "FieldMask", "Timestamp"} {
		if _, ok := api.State.MessageByID[fmt.Sprintf(".google.protobuf.%s", name)]; !ok {
			t.Errorf("cannot find well-known message %s in API", name)
		}
	}
}

func TestRust_WellKnownTypesAsMethod(t *testing.T) {
	api := newTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)

	want := "gax_wkt::Empty"
	got := c.MethodInOutTypeName(".google.protobuf.Empty", api.State)
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
	api := newTestAPI([]*api.Message{message, nested}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)

	want := "crate::model::Target"
	got := c.MethodInOutTypeName("..Target", api.State)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}

	want = "crate::model::target::Nested"
	got = c.MethodInOutTypeName("..Target.Nested", api.State)
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
	api := newTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"f_int64":          `#[serde_as(as = "serde_with::DisplayFromStr")]`,
		"f_int64_optional": `#[serde_as(as = "Option<serde_with::DisplayFromStr>")]`,
		"f_int64_repeated": `#[serde_as(as = "Vec<serde_with::DisplayFromStr>")]`,

		"f_bytes":          `#[serde_as(as = "serde_with::base64::Base64")]`,
		"f_bytes_optional": `#[serde_as(as = "Option<serde_with::base64::Base64>")]`,
		"f_bytes_repeated": `#[serde_as(as = "Vec<serde_with::base64::Base64>")]`,

		"f_string":          ``,
		"f_string_optional": ``,
		"f_string_repeated": ``,
	}
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(c.FieldAttributes(field, api.State), "\n")
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
	api := newTestAPI([]*api.Message{target, map1, map2, map3, map4, message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"target":      ``,
		"map":         `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`,
		"map_i64":     `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<_, serde_with::DisplayFromStr>")]`,
		"map_i64_key": `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<serde_with::DisplayFromStr, _>")]`,
		"map_bytes":   `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<_, serde_with::base64::Base64>")]`,
	}
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(c.FieldAttributes(field, api.State), "\n")
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
		},
	}
	api := newTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"f_int64":  `#[serde_as(as = "Option<serde_with::DisplayFromStr>")]`,
		"f_uint64": `#[serde_as(as = "Option<serde_with::DisplayFromStr>")]`,
		"f_bytes":  `#[serde_as(as = "Option<serde_with::base64::Base64>")]`,
		"f_string": ``,
	}
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(c.FieldAttributes(field, api.State), "\n")
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
	api := newTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"data":       `#[serde_as(as = "serde_with::base64::Base64")]`,
		"dataCrc32c": `#[serde(rename = "dataCrc32c")]` + "\n" + `#[serde_as(as = "Option<serde_with::DisplayFromStr>")]`,
	}
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(c.FieldAttributes(field, api.State), "\n")
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
	api := newTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})

	expectedAttributes := map[string]string{
		"updateMask":  ``,
		"project":     `#[serde(skip)]`,
		"data_crc32c": `#[serde(skip)]`,
	}
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedAttributes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := strings.Join(c.FieldAttributes(field, api.State), "\n")
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
	api := newTestAPI([]*api.Message{target, message}, []*api.Enum{}, []*api.Service{})

	expectedTypes := map[string]string{
		"f_int32":              "i32",
		"f_int32_optional":     "Option<i32>",
		"f_int32_repeated":     "Vec<i32>",
		"f_msg":                "Option<crate::model::Target>",
		"f_msg_repeated":       "Vec<crate::model::Target>",
		"f_timestamp":          "Option<gax_wkt::Timestamp>",
		"f_timestamp_repeated": "Vec<gax_wkt::Timestamp>",
	}
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)
	for _, field := range message.Fields {
		want, ok := expectedTypes[field.Name]
		if !ok {
			t.Fatalf("missing expected value for %s", field.Name)
		}
		got := c.FieldType(field, api.State)
		if got != want {
			t.Errorf("mismatched field type for %s, got=%s, want=%s", field.Name, got, want)
		}
	}
}

func TestRust_QueryParams(t *testing.T) {
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
	}
	anotherField := &api.Field{
		Name:     "another_field",
		JSONName: "anotherField",
		Typez:    api.STRING_TYPE,
		TypezID:  options.ID,
	}
	request := &api.Message{
		Name: "TestRequest",
		ID:   "..TestRequest",
		Fields: []*api.Field{
			optionsField, anotherField,
			{
				Name: "unused",
			},
		},
	}
	method := &api.Method{
		Name:         "Test",
		ID:           "..TestService.Test",
		InputTypeID:  request.ID,
		OutputTypeID: ".google.protobuf.Empty",
		PathInfo: &api.PathInfo{
			Verb: "GET",
			QueryParameters: map[string]bool{
				"options_field": true,
				"another_field": true,
			},
		},
	}
	test := newTestAPI(
		[]*api.Message{options, request},
		[]*api.Enum{},
		[]*api.Service{
			{
				Name:    "TestService",
				ID:      "..TestService",
				Methods: []*api.Method{method},
			},
		})
	c := createRustCodec()
	c.LoadWellKnownTypes(test.State)

	got := c.QueryParams(method, test.State)
	want := []*api.Field{optionsField, anotherField}
	less := func(a, b *api.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
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
	}
	anotherField := &api.Field{
		Name:     "another_field",
		JSONName: "anotherField",
		Typez:    api.STRING_TYPE,
		TypezID:  options.ID,
	}
	request := &api.Message{
		Name:   "TestRequest",
		ID:     "..TestRequest",
		Fields: []*api.Field{optionsField, anotherField},
	}
	api := newTestAPI(
		[]*api.Message{options, request},
		[]*api.Enum{},
		[]*api.Service{})
	c := createRustCodec()
	c.LoadWellKnownTypes(api.State)

	want := "&serde_json::to_value(&req.options_field).map_err(Error::serde)?"
	got := c.AsQueryParameter(optionsField, api.State)
	if want != got {
		t.Errorf("mismatched as query parameter for options_field, want=%s, got=%s", want, got)
	}

	want = "&req.another_field"
	got = c.AsQueryParameter(anotherField, api.State)
	if want != got {
		t.Errorf("mismatched as query parameter for another_field, want=%s, got=%s", want, got)
	}

}

type rustCaseConvertTest struct {
	Input    string
	Expected string
}

func TestRust_ToSnake(t *testing.T) {
	c := &RustCodec{}
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
		if output := c.ToSnake(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q, input=%s", output, test.Expected, test.Input)
		}
	}
}

func TestRust_ToPascal(t *testing.T) {
	c := &RustCodec{}
	var pascalConvertTests = []rustCaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"Self", "r#Self"},
		{"self", "r#Self"},
		{"yield", "Yield"},
		{"IAMPolicy", "IAMPolicy"},
		{"IAMPolicyRequest", "IAMPolicyRequest"},
	}
	for _, test := range pascalConvertTests {
		if output := c.ToPascal(test.Input); output != test.Expected {
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

	api := newTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &RustCodec{}
	got := c.FormatDocComments(input, api.State)
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

	api := newTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := createRustCodec()
	got := c.FormatDocComments(input, api.State)
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
		"///",
		"/// Blockquotes come in many forms. They can start with a leading '> ', as in:",
		"///",
		"/// ```norust",
		"/// Block quote style 1",
		"/// Continues 1 - style 1",
		"/// Continues 2 - style 1",
		"/// Continues 3 - style 1",
		"/// ```",
		"///",
		"/// They can start with 3 spaces and then '> ', as in:",
		"///",
		"/// ```norust",
		"/// Block quote style 2",
		"/// Continues 1 - style 2",
		"/// Continues 2 - style 2",
		"/// Continues 3 - style 2",
		"/// ```",
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
		"///",
		"///",
	}

	api := newTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &RustCodec{}
	got := c.FormatDocComments(input, api.State)
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

	api := newTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	c := &RustCodec{}
	got := c.FormatDocComments(input, api.State)
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
[ExternalMessage][google.iam.v1.SetIamPolicyRequest]
[ExternalService][google.iam.v1.Iampolicy]
[ENUM_VALUE][test.v1.SomeMessage.SomeEnum.ENUM_VALUE]
[SomeService.CreateFoo][test.v1.SomeService.CreateFoo]
`
	want := []string{
		"///",
		"/// [Any][google.protobuf.Any]",
		"/// [Message][test.v1.SomeMessage]",
		"/// [Enum][test.v1.SomeMessage.SomeEnum]",
		"/// [Message][test.v1.SomeMessage] repeated",
		"/// [Service][test.v1.SomeService] [field][test.v1.SomeMessage.field]",
		"/// [ExternalMessage][google.iam.v1.SetIamPolicyRequest]",
		"/// [ExternalService][google.iam.v1.Iampolicy]",
		"/// [ENUM_VALUE][test.v1.SomeMessage.SomeEnum.ENUM_VALUE]",
		"/// [SomeService.CreateFoo][test.v1.SomeService.CreateFoo]",
		"///",
		"///",
		"/// [google.iam.v1.Iampolicy]: iam_v1::traits::Iampolicy",
		"/// [google.iam.v1.SetIamPolicyRequest]: iam_v1::model::SetIamPolicyRequest",
		"/// [google.protobuf.Any]: wkt::Any",
		"/// [test.v1.SomeMessage]: crate::model::SomeMessage",
		"/// [test.v1.SomeMessage.SomeEnum]: crate::model::some_message::SomeEnum",
		"/// [test.v1.SomeMessage.SomeEnum.ENUM_VALUE]: crate::model::some_message::some_enum::ENUM_VALUE",
		"/// [test.v1.SomeMessage.field]: crate::model::SomeMessage::field",
		"/// [test.v1.SomeService]: crate::traits::SomeService",
		"/// [test.v1.SomeService.CreateFoo]: crate::traits::SomeService::create_foo",
	}

	wkt := &RustPackage{
		Name:    "wkt",
		Package: "gcp-sdk-wkt",
		Path:    "src/wkt",
	}
	iam := &RustPackage{
		Name:    "iam_v1",
		Package: "gcp-sdk-iam-v1",
		Path:    "src/generated/iam/v1",
	}
	c := &RustCodec{
		ModulePath:    "model",
		ExtraPackages: []*RustPackage{wkt, iam},
		PackageMapping: map[string]*RustPackage{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex api.API instance. Create it
	// in a separate function to make this more readable.
	apiz := makeApiForRustFormatDocCommentsCrossLinks()
	c.LoadWellKnownTypes(apiz.State)

	got := c.FormatDocComments(input, apiz.State)
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
	someMessage := &api.Message{
		Name:  "SomeMessage",
		ID:    ".test.v1.SomeMessage",
		Enums: []*api.Enum{someEnum},
		Fields: []*api.Field{
			{Name: "unused"}, {Name: "field"},
		},
	}
	someService := &api.Service{
		Name: "SomeService",
		ID:   ".test.v1.SomeService",
		Methods: []*api.Method{
			{Name: "CreateFoo", ID: ".test.v1.SomeService.CreateFoo"},
		},
	}
	a := newTestAPI(
		[]*api.Message{someMessage},
		[]*api.Enum{someEnum},
		[]*api.Service{someService})
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
http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html
http://www.unicode.org/reports/tr35/#Unicode_locale_identifier.
https://cloud.google.com/apis/design/design_patterns#integer_types
https://cloud.google.com/apis/design/design_patterns#integer_types.`
	want := []string{
		"///",
		"/// blah blah <https://cloud.google.com> foo bar",
		"/// [link](https://example1.com)",
		"/// <https://example2.com>",
		"/// <https://example3.com>.",
		"/// <https://example4.com>.",
		"/// <https://example5.com> <https://cloud.google.com> something else.",
		"/// [link definition]: https://example6.com/",
		"/// <http://www.unicode.org/cldr/charts/30/supplemental/territory_information.html>",
		"/// <http://www.unicode.org/reports/tr35/#Unicode_locale_identifier>.",
		"/// <https://cloud.google.com/apis/design/design_patterns#integer_types>",
		"/// <https://cloud.google.com/apis/design/design_patterns#integer_types>.",
	}

	wkt := &RustPackage{
		Name:    "wkt",
		Package: "gcp-sdk-wkt",
		Path:    "src/wkt",
	}
	iam := &RustPackage{
		Name:    "iam_v1",
		Package: "gcp-sdk-iam-v1",
		Path:    "src/generated/iam/v1",
	}
	c := &RustCodec{
		ModulePath:    "model",
		ExtraPackages: []*RustPackage{wkt, iam},
		PackageMapping: map[string]*RustPackage{
			"google.protobuf": wkt,
			"google.iam.v1":   iam,
		},
	}

	// To test the mappings we need a fairly complex api.API instance. Create it
	// in a separate function to make this more readable.
	apiz := makeApiForRustFormatDocCommentsCrossLinks()
	c.LoadWellKnownTypes(apiz.State)

	got := c.FormatDocComments(input, apiz.State)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestRust_MessageNames(t *testing.T) {
	message := &api.Message{
		Name: "Replication",
		ID:   "..Replication",
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
	nested := &api.Message{
		Name:   "Automatic",
		ID:     "..Replication.Automatic",
		Parent: message,
	}

	api := newTestAPI([]*api.Message{message, nested}, []*api.Enum{}, []*api.Service{})

	c := createRustCodec()
	if got := c.MessageName(message, api.State); got != "Replication" {
		t.Errorf("mismatched message name, got=%s, want=Replication", got)
	}
	if got := c.FQMessageName(message, api.State); got != "crate::model::Replication" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::Replication", got)
	}

	if got := c.MessageName(nested, api.State); got != "Automatic" {
		t.Errorf("mismatched message name, got=%s, want=Automatic", got)
	}
	if got := c.FQMessageName(nested, api.State); got != "crate::model::replication::Automatic" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::replication::Automatic", got)
	}
}

func TestRust_EnumNames(t *testing.T) {
	message := &api.Message{
		Name: "SecretVersion",
		ID:   "..SecretVersion",
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
	nested := &api.Enum{
		Name:   "State",
		ID:     "..SecretVersion.State",
		Parent: message,
	}

	api := newTestAPI([]*api.Message{message}, []*api.Enum{nested}, []*api.Service{})

	c := createRustCodec()
	if got := c.EnumName(nested, api.State); got != "State" {
		t.Errorf("mismatched message name, got=%s, want=Automatic", got)
	}
	if got := c.FQEnumName(nested, api.State); got != "crate::model::secret_version::State" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::secret_version::State", got)
	}
}
