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

func TestSkipMessages(t *testing.T) {
	m0 := &Message{
		Name:    "Message0",
		Package: "test",
		ID:      ".test.Message0",
	}
	m1 := &Message{
		Name:    "Message1",
		Package: "test",
		ID:      ".test.Message1",
	}
	m2 := &Message{
		Name:    "Message2",
		Package: "test",
		ID:      ".test.Message2",
	}
	model := NewTestAPI([]*Message{m0, m1, m2}, []*Enum{}, []*Service{})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"skipped-ids": ".test.Message1",
	})
	want := []*Message{m0, m2}

	if diff := cmp.Diff(want, model.Messages); diff != "" {
		t.Errorf("mismatch in messages (-want, +got)\n:%s", diff)
	}
}

func TestSkipEnums(t *testing.T) {
	e0 := &Enum{
		Name:    "Enum0",
		Package: "test",
		ID:      ".test.Enum0",
	}
	e1 := &Enum{
		Name:    "Enum1",
		Package: "test",
		ID:      ".test.Enum1",
	}
	e2 := &Enum{
		Name:    "Enum2",
		Package: "test",
		ID:      ".test.Enum2",
	}
	model := NewTestAPI([]*Message{}, []*Enum{e0, e1, e2}, []*Service{})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"skipped-ids": ".test.Enum1",
	})

	want := []*Enum{e0, e2}

	if diff := cmp.Diff(want, model.Enums); diff != "" {
		t.Errorf("mismatch in enums (-want, +got)\n:%s", diff)
	}
}

func TestSkipNestedMessages(t *testing.T) {
	m0 := &Message{
		Name:    "Message0",
		Package: "test",
		ID:      ".test.Message2.Message0",
	}
	m1 := &Message{
		Name:    "Message1",
		Package: "test",
		ID:      ".test.Message2.Message1",
	}
	m2 := &Message{
		Name:     "Message2",
		Package:  "test",
		ID:       ".test.Message2",
		Messages: []*Message{m0, m1},
	}
	model := NewTestAPI([]*Message{m2}, []*Enum{}, []*Service{})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"skipped-ids": ".test.Message2.Message1",
	})
	want := []*Message{m0}
	if diff := cmp.Diff(want, m2.Messages); diff != "" {
		t.Errorf("mismatch in messages (-want, +got)\n:%s", diff)
	}
}

func TestSkipNestedEnums(t *testing.T) {
	e0 := &Enum{
		Name:    "Enum0",
		Package: "test",
		ID:      ".test.Message.Enum0",
	}
	e1 := &Enum{
		Name:    "Enum1",
		Package: "test",
		ID:      ".test.Message.Enum1",
	}
	e2 := &Enum{
		Name:    "Enum2",
		Package: "test",
		ID:      ".test.Message.Enum2",
	}
	m := &Message{
		Name:    "Message",
		Package: "test",
		ID:      ".test.Message",
		Enums:   []*Enum{e0, e1, e2},
	}
	model := NewTestAPI([]*Message{m}, []*Enum{}, []*Service{})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"skipped-ids": ".test.Message.Enum1",
	})

	want := []*Enum{e0, e2}
	if diff := cmp.Diff(want, m.Enums); diff != "" {
		t.Errorf("mismatch in enums (-want, +got)\n:%s", diff)
	}
}

func TestSkipServices(t *testing.T) {
	s0 := &Service{
		Name:    "Service0",
		Package: "test",
		ID:      ".test.Service0",
	}
	s1 := &Service{
		Name:    "Service1",
		Package: "test",
		ID:      ".test.Service1",
	}
	s2 := &Service{
		Name:    "Service2",
		Package: "test",
		ID:      ".test.Service2",
	}
	model := NewTestAPI([]*Message{}, []*Enum{}, []*Service{s0, s1, s2})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"skipped-ids": ".test.Service1",
	})

	want := []*Service{s0, s2}

	if diff := cmp.Diff(want, model.Services, cmpopts.IgnoreFields(Service{}, "Model")); diff != "" {
		t.Errorf("mismatch in services (-want, +got)\n:%s", diff)
	}
}

func TestSkipMethods(t *testing.T) {
	s0 := &Service{
		Name:    "Service0",
		Package: "test",
		ID:      ".test.Service0",
	}
	s1 := &Service{
		Name:    "Service1",
		Package: "test",
		ID:      ".test.Service1",
		Methods: []*Method{
			{
				Name: "Method0",
				ID:   ".test.Service1.Method0",
			},
			{
				Name: "Method1",
				ID:   ".test.Service1.Method1",
			},
			{
				Name: "Method2",
				ID:   ".test.Service1.Method2",
			},
		},
	}
	s2 := &Service{
		Name:    "Service2",
		Package: "test",
		ID:      ".test.Service2",
	}
	model := NewTestAPI([]*Message{}, []*Enum{}, []*Service{s0, s1, s2})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"skipped-ids": ".test.Service1.Method1",
	})

	wantServices := []*Service{s0, s1, s2}
	if diff := cmp.Diff(wantServices, model.Services, cmpopts.IgnoreFields(Service{}, "Model")); diff != "" {
		t.Errorf("mismatch in services (-want, +got)\n:%s", diff)
	}

	wantMethods := []*Method{
		{
			Name: "Method0",
			ID:   ".test.Service1.Method0",
		},
		{
			Name: "Method2",
			ID:   ".test.Service1.Method2",
		},
	}
	if diff := cmp.Diff(wantMethods, s1.Methods); diff != "" {
		t.Errorf("mismatch in methods (-want, +got)\n:%s", diff)
	}
}

func TestIncludeNestedEnums(t *testing.T) {
	e0 := &Enum{
		Name:    "Enum0",
		Package: "test",
		ID:      ".test.Message.Enum0",
	}
	e1 := &Enum{
		Name:    "Enum1",
		Package: "test",
		ID:      ".test.Message.Enum1",
	}
	e2 := &Enum{
		Name:    "Enum2",
		Package: "test",
		ID:      ".test.Message.Enum2",
	}
	m := &Message{
		Name:    "Message",
		Package: "test",
		ID:      ".test.Message",
	}
	model := NewTestAPI([]*Message{m}, []*Enum{e0, e1, e2}, []*Service{})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"included-ids": ".test.Message.Enum0",
	})

	want := []*Enum{e0}
	if diff := cmp.Diff(want, m.Enums, cmpopts.IgnoreFields(Message{}, "Enums")); diff != "" {
		t.Errorf("mismatch in enums (-want, +got)\n:%s", diff)
	}
}

func TestIncludeNestedMessages(t *testing.T) {
	m0 := &Message{
		Name:    "Message0",
		Package: "test",
		ID:      ".test.Message2.Message0",
	}
	m1 := &Message{
		Name:    "Message1",
		Package: "test",
		ID:      ".test.Message2.Message1",
	}
	m2 := &Message{
		Name:    "Message2",
		Package: "test",
		ID:      ".test.Message2",
	}
	model := NewTestAPI([]*Message{m0, m1, m2}, []*Enum{}, []*Service{})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"included-ids": ".test.Message2.Message0",
	})
	want := []*Message{m0}
	if diff := cmp.Diff(want, m2.Messages, cmpopts.IgnoreFields(Message{}, "Messages")); diff != "" {
		t.Errorf("mismatch in messages (-want, +got)\n:%s", diff)
	}
}

func TestIncludeMethods(t *testing.T) {
	m := &Message{
		Name: "Empty",
		ID:   ".test.Empty",
	}
	s0 := &Service{
		Name:    "Service0",
		Package: "test",
		ID:      ".test.Service0",
	}
	s1 := &Service{
		Name:    "Service1",
		Package: "test",
		ID:      ".test.Service1",
		Methods: []*Method{
			{
				Name:         "Method0",
				ID:           ".test.Service1.Method0",
				InputTypeID:  ".test.Empty",
				OutputTypeID: ".test.Empty",
			},
			{
				Name:         "Method1",
				ID:           ".test.Service1.Method1",
				InputTypeID:  ".test.Empty",
				OutputTypeID: ".test.Empty",
			},
			{
				Name:         "Method2",
				ID:           ".test.Service1.Method2",
				InputTypeID:  ".test.Empty",
				OutputTypeID: ".test.Empty",
			},
		},
	}
	s2 := &Service{
		Name:    "Service2",
		Package: "test",
		ID:      ".test.Service2",
	}
	model := NewTestAPI([]*Message{m}, []*Enum{}, []*Service{s0, s1, s2})
	CrossReference(model)
	SkipModelElements(model, map[string]string{
		"included-ids": ".test.Service1.Method1,.test.Service1.Method2",
	})

	wantServices := []*Service{s1}
	if diff := cmp.Diff(wantServices, model.Services, cmpopts.IgnoreFields(Method{}, "Model"), cmpopts.IgnoreFields(Service{}, "Model")); diff != "" {
		t.Errorf("mismatch in services (-want, +got)\n:%s", diff)
	}

	wantMethods := []*Method{
		{
			Name: "Method1",
			ID:   ".test.Service1.Method1",
		},
		{
			Name: "Method2",
			ID:   ".test.Service1.Method2",
		},
	}
	if diff := cmp.Diff(wantMethods, s1.Methods, cmpopts.IgnoreFields(Method{}, "Model", "Service", "InputType", "OutputType", "InputTypeID", "OutputTypeID")); diff != "" {
		t.Errorf("mismatch in methods (-want, +got)\n:%s", diff)
	}
}
