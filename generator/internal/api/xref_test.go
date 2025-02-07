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
	"fmt"
	"testing"
)

func TestCrossReferenceOneOfs(t *testing.T) {
	var fields []*Field
	for i := range 4 {
		name := fmt.Sprintf("field%d", i)
		fields = append(fields, &Field{
			Name:    name,
			ID:      ".test.Message." + name,
			Typez:   STRING_TYPE,
			IsOneOf: true,
		})
	}
	fields = append(fields, &Field{
		Name:    "basic_field",
		ID:      ".test.Message.basic_field",
		Typez:   STRING_TYPE,
		IsOneOf: true,
	})
	group0 := &OneOf{
		Name:   "group0",
		Fields: []*Field{fields[0], fields[1]},
	}
	group1 := &OneOf{
		Name:   "group1",
		Fields: []*Field{fields[2], fields[3]},
	}
	message := &Message{
		Name:   "Message",
		ID:     ".test.Message",
		Fields: fields,
		OneOfs: []*OneOf{group0, group1},
	}
	model := NewTestAPI([]*Message{message}, []*Enum{}, []*Service{})
	if err := CrossReference(model); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		field *Field
		oneof *OneOf
	}{
		{fields[0], group0},
		{fields[1], group0},
		{fields[2], group1},
		{fields[3], group1},
		{fields[4], nil},
	} {
		if test.field.Group != test.oneof {
			t.Errorf("mismatched group for %s, got=%v, want=%v", test.field.Name, test.field.Group, test.oneof)
		}

	}
}

func TestCrossReferenceMethod(t *testing.T) {
	request := &Message{
		Name: "Request",
		ID:   ".test.Request",
	}
	response := &Message{
		Name: "Response",
		ID:   ".test.Response",
	}
	method := &Method{
		Name:         "GetResource",
		ID:           ".test.Service.GetResource",
		InputTypeID:  ".test.Request",
		OutputTypeID: ".test.Response",
	}
	service := &Service{
		Name:    "Service",
		ID:      ".test.Service",
		Methods: []*Method{method},
	}

	model := NewTestAPI([]*Message{request, response}, []*Enum{}, []*Service{service})
	if err := CrossReference(model); err != nil {
		t.Fatal(err)
	}
	if method.InputType != request {
		t.Errorf("mismatched input type, got=%v, want=%v", method.InputType, request)
	}
	if method.OutputType != response {
		t.Errorf("mismatched output type, got=%v, want=%v", method.OutputType, response)
	}
}

func TestCrossReferenceService(t *testing.T) {
	service := &Service{
		Name: "Service",
		ID:   ".test.Service",
	}
	mixin := &Service{
		Name: "Mixin",
		ID:   ".external.Mixin",
	}

	model := NewTestAPI([]*Message{}, []*Enum{}, []*Service{service})
	model.State.ServiceByID[mixin.ID] = mixin
	if err := CrossReference(model); err != nil {
		t.Fatal(err)
	}
	if service.Model != model {
		t.Errorf("mismatched model, got=%v, want=%v", service.Model, model)
	}
	if mixin.Model != model {
		t.Errorf("mismatched model, got=%v, want=%v", mixin.Model, model)
	}
}
