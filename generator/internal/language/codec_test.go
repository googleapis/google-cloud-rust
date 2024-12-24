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

	got := QueryParams(method, test.State)
	want := []*api.Field{optionsField, anotherField}
	less := func(a, b *api.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}

func TestPathParams(t *testing.T) {
	secret := &api.Message{
		Name: "Secret",
		ID:   "..Secret",
		Fields: []*api.Field{
			{
				Name:     "name",
				JSONName: "name",
				Typez:    api.STRING_TYPE,
			},
		},
	}
	updateRequest := &api.Message{
		Name: "UpdateRequest",
		ID:   "..UpdateRequest",
		Fields: []*api.Field{
			{
				Name:     "secret",
				JSONName: "secret",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  "..Secret",
			},
			{
				Name:     "field_mask",
				JSONName: "fieldMask",
				Typez:    api.MESSAGE_TYPE,
				TypezID:  ".google.protobuf.FieldMask",
				Optional: true,
			},
		},
	}
	updateMethod := &api.Method{
		Name:         "UpdateSecret",
		ID:           "..TestService.Test",
		InputTypeID:  updateRequest.ID,
		OutputTypeID: ".google.protobuf.Empty",
		PathInfo: &api.PathInfo{
			Verb: "PATCH",
			PathTemplate: []api.PathSegment{
				api.NewLiteralPathSegment("v1"),
				api.NewFieldPathPathSegment("secret.name"),
			},
			QueryParameters: map[string]bool{
				"field_mask": true,
			},
		},
	}
	createRequest := &api.Message{
		Name: "CreateRequest",
		ID:   "..CreateRequest",
		Fields: []*api.Field{
			{
				Name:     "parent",
				JSONName: "parent",
				Typez:    api.STRING_TYPE,
			},
			{
				Name:     "secret_id",
				JSONName: "secret_id",
				Typez:    api.STRING_TYPE,
			},
		},
	}
	createMethod := &api.Method{
		Name:         "CreateSecret",
		ID:           "..TestService.CreateSecret",
		InputTypeID:  createRequest.ID,
		OutputTypeID: ".google.protobuf.Empty",
		PathInfo: &api.PathInfo{
			Verb: "POST",
			PathTemplate: []api.PathSegment{
				api.NewLiteralPathSegment("v1"),
				api.NewFieldPathPathSegment("parent"),
				api.NewLiteralPathSegment("secrets"),
				api.NewFieldPathPathSegment("secret_id"),
			},
			QueryParameters: map[string]bool{},
		},
	}
	test := newTestAPI(
		[]*api.Message{secret, updateRequest, createRequest},
		[]*api.Enum{},
		[]*api.Service{
			{
				Name:    "TestService",
				ID:      "..TestService",
				Methods: []*api.Method{updateMethod, createMethod},
			},
		})

	less := func(a, b *api.Field) bool { return a.Name < b.Name }

	got := PathParams(createMethod, test.State)
	want := createRequest.Fields
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}

	got = PathParams(updateMethod, test.State)
	want = []*api.Field{updateRequest.Fields[0]}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("mismatched query parameters (-want, +got):\n%s", diff)
	}
}
