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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestFindDependenciesUnknownIdErrors(t *testing.T) {
	enums := []*Enum{}
	messages := []*Message{}
	services := []*Service{}
	model := NewTestAPI(messages, enums, services)

	_, err := FindDependencies(model, []string{".test.UnknownId"})
	if err == nil {
		t.Errorf("FindDependencies should error on unknown IDs")
	}

	msg := err.Error()
	if !strings.Contains(msg, ".test.UnknownId") {
		t.Errorf("FindDependencies should report unknown IDs in its error message. message=`%s`", msg)
	}
}

func TestFindDependenciesEnumFields(t *testing.T) {
	enums := []*Enum{
		{
			Name: "OrphanEnum",
			ID:   ".test.OrphanEnum",
		},
	}
	messages := []*Message{
		{
			Name: "MessageWithEnumField",
			ID:   ".test.MessageWithEnumField",
			Fields: []*Field{
				{
					Name:    "enum",
					Typez:   ENUM_TYPE,
					TypezID: ".test.OrphanEnum",
				},
			},
		},
	}
	services := []*Service{}
	model := NewTestAPI(messages, enums, services)
	less := func(a, b string) bool { return a < b }

	// Verify that a standalone enum does not have dependencies.
	got, err := FindDependencies(model, []string{".test.OrphanEnum"})
	if err != nil {
		t.Fatal(err)
	}
	// Note that `MessageWithEnumField` is not included.
	want := []string{".test.OrphanEnum"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	// Verify that a message with an enum field depends on the enum.
	got, err = FindDependencies(model, []string{".test.MessageWithEnumField"})
	if err != nil {
		t.Fatal(err)
	}
	want = []string{".test.OrphanEnum", ".test.MessageWithEnumField"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

func TestFindDependenciesNestedEnum(t *testing.T) {
	enums := []*Enum{
		{
			Name: "ChildEnum",
			ID:   ".test.ParentMessage.ChildEnum",
		},
		{
			Name: "SiblingEnum",
			ID:   ".test.ParentMessage.SiblingEnum",
		},
	}
	messages := []*Message{
		{
			Name: "ParentMessage",
			ID:   ".test.ParentMessage",
		},
	}
	services := []*Service{}
	model := NewTestAPI(messages, enums, services)
	less := func(a, b string) bool { return a < b }

	// Verify parent message fans out to its child enums.
	parent := ".test.ParentMessage"
	child := ".test.ParentMessage.ChildEnum"
	sibling := ".test.ParentMessage.SiblingEnum"
	got, err := FindDependencies(model, []string{parent})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{parent, child, sibling}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	// Verify nested enums depend on their parent messages
	got, err = FindDependencies(model, []string{child})
	if err != nil {
		t.Fatal(err)
	}
	want = []string{parent, child} // No `sibling`
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

func TestFindDependenciesNestedMessage(t *testing.T) {
	enums := []*Enum{}
	messages := []*Message{
		{
			Name: "Grandparent",
			ID:   ".test.Grandparent",
		},
		{
			Name: "Grandparent.Parent",
			ID:   ".test.Grandparent.Parent",
		},
		{
			Name: "Grandparent.Parent.Child",
			ID:   ".test.Grandparent.Parent.Child",
		},
		{
			Name: "Grandparent.Parent.Sibling",
			ID:   ".test.Grandparent.Parent.Sibling",
		},
		{
			Name: "Ignored",
			ID:   ".test.Ignored",
		},
	}
	services := []*Service{}
	model := NewTestAPI(messages, enums, services)
	less := func(a, b string) bool { return a < b }

	// Verify that parent and nested messages are included. The following
	// test cases are all equivalent.
	grandparent := ".test.Grandparent"
	parent := ".test.Grandparent.Parent"
	child := ".test.Grandparent.Parent.Child"
	sibling := ".test.Grandparent.Parent.Sibling"
	for _, ids := range [][]string{
		{grandparent},
		{parent},
		{child, parent},
		{child, grandparent},
		{grandparent, child},
		{grandparent, parent, child},
	} {
		got, err := FindDependencies(model, ids)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{grandparent, parent, child, sibling}
		if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
			t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
		}
	}

	// Verify that unnecessary siblings are not included
	got, err := FindDependencies(model, []string{child})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{grandparent, parent, child} // No `sibling`
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

func TestFindDependenciesMessage(t *testing.T) {
	enums := []*Enum{}
	messages := []*Message{
		{
			Name: "MessageWithMessageField",
			ID:   ".test.MessageWithMessageField",
			Fields: []*Field{
				{
					Name:    "message",
					Typez:   MESSAGE_TYPE,
					TypezID: ".test.Orphan",
				},
			},
		},
		{
			Name: "Orphan",
			ID:   ".test.Orphan",
		},
	}
	services := []*Service{}
	model := NewTestAPI(messages, enums, services)
	less := func(a, b string) bool { return a < b }

	// Verify that we fan out over the field types
	got, err := FindDependencies(model, []string{".test.MessageWithMessageField"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".test.MessageWithMessageField", ".test.Orphan"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	got, err = FindDependencies(model, []string{".test.Orphan"})
	if err != nil {
		t.Fatal(err)
	}
	// Note that `MessageWithMessageField` is not included.
	want = []string{".test.Orphan"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

func TestFindDependenciesMethod(t *testing.T) {
	enums := []*Enum{}
	messages := []*Message{
		{
			Name: "Request",
			ID:   ".test.Request",
		},
		{
			Name: "Response",
			ID:   ".test.Response",
		},
	}
	services := []*Service{
		{
			Name: "Service",
			ID:   ".test.Service",
			Methods: []*Method{
				{
					Name:         "Method",
					ID:           ".test.Service.Method",
					InputTypeID:  ".test.Request",
					OutputTypeID: ".test.Response",
				},
				{
					Name:         "Sibling",
					ID:           ".test.Service.Sibling",
					InputTypeID:  ".test.Request",
					OutputTypeID: ".test.Response",
				},
			},
		},
	}
	model := NewTestAPI(messages, enums, services)
	CrossReference(model)
	less := func(a, b string) bool { return a < b }

	got, err := FindDependencies(model, []string{".test.Service.Method"})
	if err != nil {
		t.Fatal(err)
	}
	// Note that `Sibling` is not included
	want := []string{".test.Service", ".test.Service.Method", ".test.Request", ".test.Response"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	// Verify that messages don't imply methods
	got, err = FindDependencies(model, []string{".test.Request"})
	if err != nil {
		t.Fatal(err)
	}
	want = []string{".test.Request"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

func TestFindDependenciesLroMethod(t *testing.T) {
	enums := []*Enum{}
	messages := []*Message{
		{
			Name: "Empty",
			ID:   ".test.Empty",
		},
		{
			Name: "OpMetadata",
			ID:   ".test.OpMetadata",
		},
		{
			Name: "OpResponse",
			ID:   ".test.OpResponse",
		},
	}
	services := []*Service{
		{
			Name: "Service",
			ID:   ".test.Service",
			Methods: []*Method{
				{
					Name:         "Lro",
					ID:           ".test.Service.Lro",
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
	model := NewTestAPI(messages, enums, services)
	CrossReference(model)
	less := func(a, b string) bool { return a < b }

	got, err := FindDependencies(model, []string{".test.Service.Lro"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".test.Service", ".test.Service.Lro", ".test.Empty", ".test.OpMetadata", ".test.OpResponse"}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

func TestFindDependenciesService(t *testing.T) {
	enums := []*Enum{
		{
			Name: "Enum",
			ID:   ".test.Enum",
		},
	}
	messages := []*Message{
		{
			Name: "Request",
			ID:   ".test.Request",
			Fields: []*Field{
				{
					Name:    "message",
					Typez:   MESSAGE_TYPE,
					TypezID: ".test.ParentMessage.ChildMessage",
				},
			},
		},
		{
			Name: "ParentMessage",
			ID:   ".test.ParentMessage",
			Fields: []*Field{
				{
					Name:    "enum",
					Typez:   ENUM_TYPE,
					TypezID: ".test.Enum",
				},
			},
		},
		{
			Name: "ParentMessage.ChildMessage",
			ID:   ".test.ParentMessage.ChildMessage",
		},
		{
			Name: "Response",
			ID:   ".test.Response",
		},
		{
			Name: "OtherRequest",
			ID:   ".test.OtherRequest",
		},
		{
			Name: "OtherResponse",
			ID:   ".test.OtherResponse",
		},
		{
			Name: "Ignored",
			ID:   ".test.Ignored",
		},
	}
	services := []*Service{
		{
			Name: "Service",
			ID:   ".test.Service",
			Methods: []*Method{
				{
					Name:         "Method",
					ID:           ".test.Service.Method",
					InputTypeID:  ".test.Request",
					OutputTypeID: ".test.Response",
				},
				{
					Name:         "Sibling",
					ID:           ".test.Service.Sibling",
					InputTypeID:  ".test.Request",
					OutputTypeID: ".test.Response",
				},
			},
		}, {
			Name: "OtherService",
			ID:   ".test.OtherService",
			Methods: []*Method{
				{
					Name:         "OtherMethod",
					ID:           ".test.OtherService.OtherMethod",
					InputTypeID:  ".test.OtherRequest",
					OutputTypeID: ".test.OtherResponse",
				},
			},
		},
	}
	model := NewTestAPI(messages, enums, services)
	CrossReference(model)
	less := func(a, b string) bool { return a < b }

	got, err := FindDependencies(model, []string{".test.Service"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		".test.Service",
		".test.Service.Method",
		".test.Service.Sibling",
		".test.Request",
		".test.Response",
		".test.ParentMessage",
		".test.ParentMessage.ChildMessage",
		".test.Enum",
	}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}

	got, err = FindDependencies(model, []string{".test.Service", ".test.OtherService"})
	if err != nil {
		t.Fatal(err)
	}
	want = []string{
		".test.Service",
		".test.Service.Method",
		".test.Service.Sibling",
		".test.Request",
		".test.Response",
		".test.ParentMessage",
		".test.ParentMessage.ChildMessage",
		".test.Enum",
		".test.OtherService",
		".test.OtherService.OtherMethod",
		".test.OtherRequest",
		".test.OtherResponse",
	}
	if diff := cmp.Diff(want, flatten(got), cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("dependencies mismatch (-want, +got):\n%s", diff)
	}
}

// Simplify the test expectations
func flatten(m map[string]bool) []string {
	var arr []string
	for k, v := range m {
		if v {
			arr = append(arr, k)
		}
	}
	return arr
}
