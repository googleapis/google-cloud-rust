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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

func TestQueryParams(t *testing.T) {
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
			Bindings: []*api.PathBinding{
				{
					Verb: "GET",
					QueryParameters: map[string]bool{
						"options_field": true,
						"another_field": true,
					},
				},
			},
		},
	}
	test := api.NewTestAPI(
		[]*api.Message{options, request},
		[]*api.Enum{},
		[]*api.Service{
			{
				Name:    "TestService",
				ID:      "..TestService",
				Methods: []*api.Method{method},
			},
		})

	got := QueryParams(method, test.State)
	want := []*api.Field{optionsField, anotherField}
	less := func(a, b *api.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestPathParams(t *testing.T) {
	test := api.NewTestAPI(
		[]*api.Message{sample.Secret(), sample.UpdateRequest(), sample.CreateRequest()},
		[]*api.Enum{},
		[]*api.Service{sample.Service()},
	)

	less := func(a, b *api.Field) bool { return a.Name < b.Name }

	got := PathParams(sample.MethodCreate(), test.State)
	want := sample.CreateRequest().Fields
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}

	got = PathParams(sample.MethodUpdate(), test.State)
	want = []*api.Field{sample.UpdateRequest().Fields[0]}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}
