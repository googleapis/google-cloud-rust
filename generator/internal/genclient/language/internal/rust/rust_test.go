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
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

func testCodec() *Codec {
	wkt := &RustPackage{
		Name:    "gax_wkt",
		Package: "types",
		Path:    "../../types",
	}

	return &Codec{
		ExtraPackages: []*RustPackage{wkt},
		PackageMapping: map[string]*RustPackage{
			"google.protobuf": wkt,
		},
	}
}

func TestParseOptions(t *testing.T) {
	copts := &genclient.CodecOptions{
		Language: "rust",
		Options: map[string]string{
			"package-name-override": "test-only",
			"copyright-year":        "2035",
			"package:wkt":           "package=types,path=../../types,source=google.protobuf,source=test-only",
			"package:gax":           "package=gax,path=../../gax,feature=sdk_client",
		},
	}
	codec, err := NewCodec(copts)
	if err != nil {
		t.Fatal(err)
	}
	gp := &RustPackage{
		Name:    "wkt",
		Package: "types",
		Path:    "../../types",
	}
	want := &Codec{
		PackageNameOverride: "test-only",
		GenerationYear:      "2035",
		ExtraPackages: []*RustPackage{
			gp,
			{
				Name:    "gax",
				Package: "gax",
				Path:    "../../gax",
				Features: []string{
					"sdk_client",
				},
			},
		},
		PackageMapping: map[string]*RustPackage{
			"google.protobuf": gp,
			"test-only":       gp,
		},
	}
	if diff := cmp.Diff(want, codec, cmpopts.IgnoreFields(Codec{}, "ExtraPackages", "PackageMapping")); len(diff) > 0 {
		t.Errorf("codec mismatch (-want, +got):\n%s", diff)
	}
	if want.PackageNameOverride != codec.PackageNameOverride {
		t.Errorf("mismatched in packageNameOverride, want=%s, got=%s", want.PackageNameOverride, codec.PackageNameOverride)
	}
	checkPackages(t, codec, want)
}

func TestRequiredPackages(t *testing.T) {
	copts := &genclient.CodecOptions{
		Language: "rust",
		OutDir:   "src/generated/newlib",
		Options: map[string]string{
			"package:gtype": "package=types,path=src/generated/type,source=google.type,source=test-only",
			"package:gax":   "package=gax,path=src/gax,version=1.2.3",
		},
	}
	codec, err := NewCodec(copts)
	if err != nil {
		t.Fatal(err)
	}
	got := codec.RequiredPackages()
	want := []string{
		"gtype = { path = \"../../../src/generated/type\", package = \"types\" }",
		"gax = { version = \"1.2.3\", path = \"../../../src/gax\", package = \"gax\" }",
	}
	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("mismatched required packages (-want, +got):\n%s", diff)
	}
}

func TestRequiredPackagesLocal(t *testing.T) {
	// This is not a thing we expect to do in the Rust repository, but the
	// behavior is consistent.
	copts := &genclient.CodecOptions{
		Language: "rust",
		OutDir:   "",
		Options: map[string]string{
			"package:gtype": "package=types,path=src/generated/type,source=google.type,source=test-only",
		},
	}
	codec, err := NewCodec(copts)
	if err != nil {
		t.Fatal(err)
	}
	got := codec.RequiredPackages()
	want := []string{
		"gtype = { path = \"src/generated/type\", package = \"types\" }",
	}
	less := func(a, b string) bool { return a < b }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("mismatched required packages (-want, +got):\n%s", diff)
	}
}

func TestPackageName(t *testing.T) {
	packageNameImpl(t, "test-only-overridden", &genclient.CodecOptions{
		Language: "rust",
		Options: map[string]string{
			"package-name-override": "test-only-overridden",
		},
	})
	packageNameImpl(t, "test-only-default", &genclient.CodecOptions{
		Language: "rust",
	})

}

func packageNameImpl(t *testing.T, want string, copts *genclient.CodecOptions) {
	t.Helper()
	api := &genclient.API{
		Name: "test-only-default",
	}
	codec, err := NewCodec(copts)
	if err != nil {
		t.Fatal(err)
	}
	got := codec.PackageName(api)
	if want != got {
		t.Errorf("mismatch in package name, want=%s, got=%s", want, got)
	}

}

func checkPackages(t *testing.T, got *Codec, want *Codec) {
	t.Helper()
	less := func(a, b *RustPackage) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.ExtraPackages, got.ExtraPackages, cmpopts.SortSlices(less)); len(diff) > 0 {
		t.Errorf("package mismatch (-want, +got):\n%s", diff)
	}
}

func TestValidate(t *testing.T) {
	api := genclient.NewTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}})
	c := &Codec{}
	if err := c.Validate(api); err != nil {
		t.Errorf("unexpected error in API validation %q", err)
	}
	if c.SourceSpecificationPackageName != "p1" {
		t.Errorf("mismatched source package name, want=p1, got=%s", c.SourceSpecificationPackageName)
	}
}

func TestValidateMessageMismatch(t *testing.T) {
	api := genclient.NewTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}})
	c := &Codec{}
	if err := c.Validate(api); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}

	api = genclient.NewTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}})
	c = &Codec{}
	if err := c.Validate(api); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}

	api = genclient.NewTestAPI(
		[]*genclient.Message{{Name: "m1", Package: "p1"}},
		[]*genclient.Enum{{Name: "e1", Package: "p1"}},
		[]*genclient.Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	c = &Codec{}
	if err := c.Validate(api); err == nil {
		t.Errorf("expected an error in API validation got=%s", c.SourceSpecificationPackageName)
	}
}

func TestWellKnownTypesExist(t *testing.T) {
	api := genclient.NewTestAPI([]*genclient.Message{}, []*genclient.Enum{}, []*genclient.Service{})
	c := &Codec{}
	c.LoadWellKnownTypes(api.State)
	for _, name := range []string{"Any", "Duration", "Empty", "FieldMask", "Timestamp"} {
		if _, ok := api.State.MessageByID[fmt.Sprintf(".google.protobuf.%s", name)]; !ok {
			t.Errorf("cannot find well-known message %s in API", name)
		}
	}
}

func TestWellKnownTypesAsMethod(t *testing.T) {
	api := genclient.NewTestAPI([]*genclient.Message{}, []*genclient.Enum{}, []*genclient.Service{})
	c := testCodec()
	c.LoadWellKnownTypes(api.State)

	want := "gax_wkt::Empty"
	got := c.MethodInOutTypeName(".google.protobuf.Empty", api.State)
	if want != got {
		t.Errorf("mismatched well-known type name as method argument or response, want=%s, got=%s", want, got)
	}
}

func TestMethodInOut(t *testing.T) {
	message := &genclient.Message{
		Name: "Target",
		ID:   "..Target",
	}
	nested := &genclient.Message{
		Name:   "Nested",
		ID:     "..Target.Nested",
		Parent: message,
	}
	api := genclient.NewTestAPI([]*genclient.Message{message, nested}, []*genclient.Enum{}, []*genclient.Service{})
	c := &Codec{}
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

func TestFieldAttributes(t *testing.T) {
	message := &genclient.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*genclient.Field{
			{
				Name:     "f_int64",
				Typez:    genclient.INT64_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_int64_optional",
				Typez:    genclient.INT64_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_int64_repeated",
				Typez:    genclient.INT64_TYPE,
				Optional: false,
				Repeated: true,
			},

			{
				Name:     "f_bytes",
				Typez:    genclient.BYTES_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_bytes_optional",
				Typez:    genclient.BYTES_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_bytes_repeated",
				Typez:    genclient.BYTES_TYPE,
				Optional: false,
				Repeated: true,
			},

			{
				Name:     "f_string",
				Typez:    genclient.STRING_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_string_optional",
				Typez:    genclient.STRING_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_string_repeated",
				Typez:    genclient.STRING_TYPE,
				Optional: false,
				Repeated: true,
			},
		},
	}
	api := genclient.NewTestAPI([]*genclient.Message{message}, []*genclient.Enum{}, []*genclient.Service{})

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
	c := testCodec()
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

func TestMapFieldAttributes(t *testing.T) {
	target := &genclient.Message{
		Name: "Target",
		ID:   "..Target",
	}
	map1 := &genclient.Message{
		Name:  "$map<string, string>",
		ID:    "$map<string, string>",
		IsMap: true,
		Fields: []*genclient.Field{
			{
				Name:  "key",
				Typez: genclient.STRING_TYPE,
			},
			{
				Name:  "value",
				Typez: genclient.STRING_TYPE,
			},
		},
	}
	map2 := &genclient.Message{
		Name:  "$map<string, int64>",
		ID:    "$map<string, int64>",
		IsMap: true,
		Fields: []*genclient.Field{
			{
				Name:  "key",
				Typez: genclient.STRING_TYPE,
			},
			{
				Name:  "value",
				Typez: genclient.INT64_TYPE,
			},
		},
	}
	map3 := &genclient.Message{
		Name:  "$map<int64, string>",
		ID:    "$map<int64, string>",
		IsMap: true,
		Fields: []*genclient.Field{
			{
				Name:  "key",
				Typez: genclient.INT64_TYPE,
			},
			{
				Name:  "value",
				Typez: genclient.STRING_TYPE,
			},
		},
	}
	map4 := &genclient.Message{
		Name:  "$map<string, bytes>",
		ID:    "$map<string, bytes>",
		IsMap: true,
		Fields: []*genclient.Field{
			{
				Name:  "key",
				Typez: genclient.STRING_TYPE,
			},
			{
				Name:  "value",
				Typez: genclient.BYTES_TYPE,
			},
		},
	}
	message := &genclient.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*genclient.Field{
			{
				Name:     "target",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  target.ID,
				Optional: true,
				Repeated: false,
			},
			{
				Name:    "map",
				Typez:   genclient.MESSAGE_TYPE,
				TypezID: map1.ID,
			},
			{
				Name:    "map_i64",
				Typez:   genclient.MESSAGE_TYPE,
				TypezID: map2.ID,
			},
			{
				Name:    "map_i64_key",
				Typez:   genclient.MESSAGE_TYPE,
				TypezID: map3.ID,
			},
			{
				Name:    "map_bytes",
				Typez:   genclient.MESSAGE_TYPE,
				TypezID: map4.ID,
			},
		},
	}
	api := genclient.NewTestAPI([]*genclient.Message{target, map1, map2, map3, map4, message}, []*genclient.Enum{}, []*genclient.Service{})

	expectedAttributes := map[string]string{
		"target":      ``,
		"map":         `#[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]`,
		"map_i64":     `#[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<_, serde_with::DisplayFromStr>")]`,
		"map_i64_key": `#[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<serde_with::DisplayFromStr, _>")]`,
		"map_bytes":   `#[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]` + "\n" + `#[serde_as(as = "std::collections::HashMap<_, serde_with::base64::Base64>")]`,
	}
	c := testCodec()
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

func TestFieldType(t *testing.T) {
	target := &genclient.Message{
		Name: "Target",
		ID:   "..Target",
	}
	message := &genclient.Message{
		Name: "Fake",
		ID:   "..Fake",
		Fields: []*genclient.Field{
			{
				Name:     "f_int32",
				Typez:    genclient.INT32_TYPE,
				Optional: false,
				Repeated: false,
			},
			{
				Name:     "f_int32_optional",
				Typez:    genclient.INT32_TYPE,
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_int32_repeated",
				Typez:    genclient.INT32_TYPE,
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_msg",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_msg_repeated",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Target",
				Optional: false,
				Repeated: true,
			},
			{
				Name:     "f_timestamp",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: true,
				Repeated: false,
			},
			{
				Name:     "f_timestamp_repeated",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.Timestamp",
				Optional: false,
				Repeated: true,
			},
		},
	}
	api := genclient.NewTestAPI([]*genclient.Message{target, message}, []*genclient.Enum{}, []*genclient.Service{})

	expectedTypes := map[string]string{
		"f_int32":              "i32",
		"f_int32_optional":     "Option<i32>",
		"f_int32_repeated":     "Vec<i32>",
		"f_msg":                "Option<crate::model::Target>",
		"f_msg_repeated":       "Vec<crate::model::Target>",
		"f_timestamp":          "Option<gax_wkt::Timestamp>",
		"f_timestamp_repeated": "Vec<gax_wkt::Timestamp>",
	}
	c := testCodec()
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

type CaseConvertTest struct {
	Input    string
	Expected string
}

func TestToSnake(t *testing.T) {
	c := &Codec{}
	var snakeConvertTests = []CaseConvertTest{
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

func TestToPascal(t *testing.T) {
	c := &Codec{}
	var pascalConvertTests = []CaseConvertTest{
		{"foo_bar", "FooBar"},
		{"FooBar", "FooBar"},
		{"True", "True"},
		{"Self", "r#Self"},
		{"self", "r#Self"},
		{"yield", "Yield"},
	}
	for _, test := range pascalConvertTests {
		if output := c.ToPascal(test.Input); output != test.Expected {
			t.Errorf("Output %q not equal to expected %q", output, test.Expected)
		}
	}
}

func TestFormatDocComments(t *testing.T) {
	input := `Some comments describing the thing.

The next line has some extra trailing whitespace:
    
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
	c := &Codec{}
	got := c.FormatDocComments(input)
	if diff := cmp.Diff(want, got); len(diff) > 0 {
		t.Errorf("mismatch in FormatDocComments (-want, +got)\n:%s", diff)
	}
}

func TestMessageNames(t *testing.T) {
	message := &genclient.Message{
		Name: "Replication",
		ID:   "..Replication",
		Fields: []*genclient.Field{
			{
				Name:     "automatic",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &genclient.Message{
		Name:   "Automatic",
		ID:     "..Replication.Automatic",
		Parent: message,
	}

	api := genclient.NewTestAPI([]*genclient.Message{message, nested}, []*genclient.Enum{}, []*genclient.Service{})

	c := &Codec{}
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

func TestEnumNames(t *testing.T) {
	message := &genclient.Message{
		Name: "SecretVersion",
		ID:   "..SecretVersion",
		Fields: []*genclient.Field{
			{
				Name:     "automatic",
				Typez:    genclient.MESSAGE_TYPE,
				TypezID:  "..Automatic",
				Optional: true,
				Repeated: false,
			},
		},
	}
	nested := &genclient.Enum{
		Name:   "State",
		ID:     "..SecretVersion.State",
		Parent: message,
	}

	api := genclient.NewTestAPI([]*genclient.Message{message}, []*genclient.Enum{nested}, []*genclient.Service{})

	c := &Codec{}
	if got := c.EnumName(nested, api.State); got != "State" {
		t.Errorf("mismatched message name, got=%s, want=Automatic", got)
	}
	if got := c.FQEnumName(nested, api.State); got != "crate::model::secret_version::State" {
		t.Errorf("mismatched message name, got=%s, want=crate::model::secret_version::State", got)
	}
}
