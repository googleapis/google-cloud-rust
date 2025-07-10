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

package api

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestFindServiceDependencies(t *testing.T) {
	enums := []*Enum{
		{
			Name: "SomeEnum",
			ID:   ".test.SomeEnum",
		},
	}
	messages := []*Message{
		{
			Name: "Message",
			ID:   ".test.Message",
			Fields: []*Field{
				{
					Name:  "a",
					Typez: STRING_TYPE,
				},
				{
					Name:    "b",
					Typez:   ENUM_TYPE,
					TypezID: ".test.SomeEnum",
				},
				{
					Name:     "c",
					Typez:    MESSAGE_TYPE,
					TypezID:  ".test.Message",
					Optional: true,
				},
			},
		},
		{
			Name:   "Unused",
			ID:     ".test.Unused",
			Fields: []*Field{},
		},
		{
			Name: "Request",
			ID:   ".test.Request",
			Fields: []*Field{
				{
					Name:    "body",
					Typez:   MESSAGE_TYPE,
					TypezID: ".test.Message",
				},
			},
		},
		{
			Name:   "Response",
			ID:     ".test.Response",
			Fields: []*Field{},
		},
		{
			Name:   "Empty",
			ID:     ".test.Empty",
			Fields: []*Field{},
		},
		{
			Name:   "OpMetadata",
			ID:     ".test.OpMetadata",
			Fields: []*Field{},
		},
		{
			Name:   "OpResponse",
			ID:     ".test.OpResponse",
			Fields: []*Field{},
		},
	}
	services := []*Service{
		{
			Name: "Service1",
			ID:   ".test.Service1",
			Methods: []*Method{
				{
					Name:         "Method0",
					ID:           ".test.Service1.Method0",
					InputTypeID:  ".test.Request",
					OutputTypeID: ".test.Response",
				},
			},
		},
		{
			Name: "Service2",
			ID:   ".test.Service2",
			Methods: []*Method{
				{
					Name:         "Method0",
					ID:           ".test.Service2.Method0",
					InputTypeID:  ".test.Empty",
					OutputTypeID: ".test.Empty",
					OperationInfo: &OperationInfo{
						MetadataTypeID: ".test.OpMetadata",
						ResponseTypeID: ".test.OpResponse",
					},
				},
			},
		},
	}
	less := func(a, b string) bool { return a < b }
	model := NewTestAPI(messages, enums, services)
        CrossReference(model)
	got := FindServiceDependencies(model, ".test.NotFound")
	want := &ServiceDependencies{}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	got = FindServiceDependencies(model, ".test.Service1")
	want = &ServiceDependencies{
		Messages: []string{".test.Request", ".test.Response", ".test.Message"},
		Enums:    []string{".test.SomeEnum"},
	}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	got = FindServiceDependencies(model, ".test.Service2")
	want = &ServiceDependencies{
		Messages: []string{".test.Empty", ".test.OpMetadata", ".test.OpResponse"},
	}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}
