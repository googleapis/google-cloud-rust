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

package language

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestPackageNames(t *testing.T) {
	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{},
		[]*api.Service{{Name: "Workflows", Package: "gcp-sdk-workflows-v1"}})
	// Override the default name for test APIs ("Test").
	model.Name = "workflows-v1"
	codec, err := newRustCodec(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := newRustTemplateData(model, codec, "")
	if err != nil {
		t.Fatal(err)
	}
	want := "gcp_sdk_workflows_v1"
	if got.PackageNamespace != want {
		t.Errorf("mismatched package namespace, want=%s, got=%s", want, got.PackageNamespace)
	}
}

func Test_RustEnumAnnotations(t *testing.T) {
	// Verify we can handle values that are not in SCREAMING_SNAKE_CASE style.
	v0 := &api.EnumValue{
		Name:          "week5",
		ID:            ".test.v1.TestEnum.week5",
		Documentation: "week5 is also documented.",
	}
	v1 := &api.EnumValue{
		Name:          "MULTI_WORD_VALUE",
		ID:            ".test.v1.TestEnum.MULTI_WORD_VALUES",
		Documentation: "MULTI_WORD_VALUE is also documented.",
	}
	v2 := &api.EnumValue{
		Name:          "VALUE",
		ID:            ".test.v1.TestEnum.VALUE",
		Documentation: "VALUE is also documented.",
	}
	enum := &api.Enum{
		Name:          "TestEnum",
		ID:            ".test.v1.TestEnum",
		Documentation: "The enum is documented.",
		Values:        []*api.EnumValue{v0, v1, v2},
	}

	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{enum}, []*api.Service{})
	codec, err := newRustCodec(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = newRustTemplateData(model, codec, "")
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(&rustEnumAnnotation{
		Name:       "TestEnum",
		ModuleName: "test_enum",
		DocLines:   []string{"/// The enum is documented."},
	}, enum.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&rustEnumValueAnnotation{
		Name:     "WEEK_5",
		EnumType: "TestEnum",
		DocLines: []string{"/// week5 is also documented."},
	}, v0.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&rustEnumValueAnnotation{
		Name:     "MULTI_WORD_VALUE",
		EnumType: "TestEnum",
		DocLines: []string{"/// MULTI_WORD_VALUE is also documented."},
	}, v1.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&rustEnumValueAnnotation{
		Name:     "VALUE",
		EnumType: "TestEnum",
		DocLines: []string{"/// VALUE is also documented."},
	}, v2.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}
}
