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

package rust_prost

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func TestModelAnnotations(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			SpecificationFormat: "protobuf",
			SpecificationSource: "../../testdata/googleapis/google/type",
		},
		Source: map[string]string{
			"include-list": "f1.proto,f2.proto",
		},
		Codec: map[string]string{
			"copyright-year": "2035",
		},
	}
	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{},
		[]*api.Service{{Name: "Workflows", Package: "google.cloud.workflows.v1"}})
	codec := newCodec(cfg)
	if err := codec.annotateModel(model, cfg); err != nil {
		t.Fatal(err)
	}
	want := &modelAnnotations{
		PackageName:   "google-cloud-workflows-v1",
		CopyrightYear: "2035",
		Files: []string{
			"../../testdata/googleapis/google/type/f1.proto",
			"../../testdata/googleapis/google/type/f2.proto",
		},
	}
	if diff := cmp.Diff(want, model.Codec, cmpopts.IgnoreFields(modelAnnotations{}, "BoilerPlate")); diff != "" {
		t.Errorf("mismatch in model annotations (-want, +got)\n:%s", diff)
	}
}

func TestServiceAnnotations(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			SpecificationFormat: "protobuf",
			SpecificationSource: "../../testdata/googleapis/google/type",
		},
		Source: map[string]string{
			"include-list": "unused.proto",
		},
		Codec: map[string]string{
			"copyright-year": "2035",
		},
	}
	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{},
		[]*api.Service{
			{
				Name:    "Workflows",
				Package: "google.cloud.workflows.v1",
				ID:      ".google.cloud.workflows.v1.Workflows",
			},
		})
	codec := newCodec(cfg)
	if err := codec.annotateModel(model, cfg); err != nil {
		t.Fatal(err)
	}
	want := &serviceAnnotations{
		ID: "google.cloud.workflows.v1.Workflows",
	}
	got, ok := model.State.ServiceByID[".google.cloud.workflows.v1.Workflows"]
	if !ok {
		t.Fatalf("cannot find service %s", ".google.cloud.workflows.v1.Workflows")
	}
	if diff := cmp.Diff(want, got.Codec); diff != "" {
		t.Errorf("mismatch in service annotations (-want, +got)\n:%s", diff)
	}
}

func TestMethodAnnotations(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			SpecificationFormat: "protobuf",
			SpecificationSource: "../../testdata/googleapis/google/type",
		},
		Source: map[string]string{
			"include-list": "unused.proto",
		},
		Codec: map[string]string{
			"copyright-year": "2035",
		},
	}
	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{},
		[]*api.Service{
			{
				Name:    "Workflows",
				Package: "google.cloud.workflows.v1",
				ID:      ".google.cloud.workflows.v1.Workflows",
				Methods: []*api.Method{
					{
						Name: "GetWorkflow",
						ID:   ".google.cloud.workflows.v1.Workflows.GetWorkflow",
					},
				},
			},
		})
	codec := newCodec(cfg)
	if err := codec.annotateModel(model, cfg); err != nil {
		t.Fatal(err)
	}
	want := &methodAnnotations{
		ID: "google.cloud.workflows.v1.Workflows.GetWorkflow",
	}
	got, ok := model.State.MethodByID[".google.cloud.workflows.v1.Workflows.GetWorkflow"]
	if !ok {
		t.Fatalf("cannot find service %s", ".google.cloud.workflows.v1.Workflows.GetWorkflow")
	}
	if diff := cmp.Diff(want, got.Codec); diff != "" {
		t.Errorf("mismatch in method annotations (-want, +got)\n:%s", diff)
	}
}
