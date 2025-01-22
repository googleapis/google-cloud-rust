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
)

func TestSimple(t *testing.T) {
	field0 := &Field{
		Name:  "a",
		Typez: STRING_TYPE,
	}
	field1 := &Field{
		Name:     "b",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.Message",
		Optional: true,
	}
	messages := []*Message{
		{
			Name: "Message",
			ID:   ".test.Message",
			Fields: []*Field{
				field0, field1,
			},
		},
	}
	model := NewTestAPI(messages, []*Enum{}, []*Service{})
	LabelRecursiveFields(model)
	if field0.Recursive {
		t.Errorf("mismatched IsRecursive field for %v", field0)
	}
	if !field1.Recursive {
		t.Errorf("mismatched IsRecursive field for %v", field1)
	}
}

func TestSimpleMap(t *testing.T) {
	field0 := &Field{
		Repeated: false,
		Optional: false,
		Name:     "children",
		ID:       ".test.ParentMessage.children",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.ParentMessage.SingularMapEntry",
	}
	parent := &Message{
		Name:   "ParentMessage",
		ID:     ".test.ParentMessage",
		Fields: []*Field{field0},
	}

	key := &Field{
		Name:     "key",
		JSONName: "key",
		ID:       ".test.ParentMessage.SingularMapEntry.key",
		Typez:    STRING_TYPE,
	}
	value := &Field{
		Name:     "value",
		JSONName: "value",
		ID:       ".test.ParentMessage.SingularMapEntry.value",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.ParentMessage",
	}
	map_message := &Message{
		Name:    "SingularMapEntry",
		Package: "test",
		ID:      ".test.ParentMessage.SingularMapEntry",
		IsMap:   true,
		Fields:  []*Field{key, value},
	}

	model := NewTestAPI([]*Message{parent, map_message}, []*Enum{}, []*Service{})
	LabelRecursiveFields(model)
	for _, field := range []*Field{value, field0} {
		if !field.Recursive {
			t.Errorf("expected IsRecursive to be true for field %s", field.ID)
		}
	}
	if key.Recursive {
		t.Errorf("expected IsRecursive to be false for field %s", key.ID)
	}
}

func TestIndirect(t *testing.T) {
	field0 := &Field{
		Name:     "child",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.ChildMessage",
		Optional: true,
	}
	field1 := &Field{
		Name:     "grand_child",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.GrandChildMessage",
		Optional: true,
	}
	field2 := &Field{
		Name:     "back_to_grand_parent",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.Message",
		Optional: true,
	}
	messages := []*Message{
		{
			Name:   "Message",
			ID:     ".test.Message",
			Fields: []*Field{field0},
		},
		{
			Name:   "ChildMessage",
			ID:     ".test.ChildMessage",
			Fields: []*Field{field1},
		},
		{
			Name:   "GrandChildMessage",
			ID:     ".test.GrandChildMessage",
			Fields: []*Field{field2},
		},
	}
	model := NewTestAPI(messages, []*Enum{}, []*Service{})
	LabelRecursiveFields(model)
	for _, field := range []*Field{field0, field1, field2} {
		if !field.Recursive {
			t.Errorf("IsRecursive should be true for field %s", field.Name)
		}
	}
}

func TestViaMap(t *testing.T) {
	field0 := &Field{
		Name:    "parent",
		ID:      ".test.ChildMessage.parent",
		Typez:   MESSAGE_TYPE,
		TypezID: ".test.ParentMessage",
	}
	child := &Message{
		Name:   "ChildMessage",
		ID:     ".test.ChildMessage",
		Fields: []*Field{field0},
	}

	field1 := &Field{
		Repeated: false,
		Optional: false,
		Name:     "children",
		ID:       ".test.ParentMessage.children",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.ParentMessage.SingularMapEntry",
	}
	parent := &Message{
		Name:   "ParentMessage",
		ID:     ".test.ParentMessage",
		Fields: []*Field{field1},
	}

	key := &Field{
		Repeated: false,
		Optional: false,
		Name:     "key",
		JSONName: "key",
		ID:       ".test.ParentMessage.SingularMapEntry.key",
		Typez:    STRING_TYPE,
	}
	value := &Field{
		Repeated: false,
		Optional: false,
		Name:     "value",
		JSONName: "value",
		ID:       ".test.ParentMessage.SingularMapEntry.value",
		Typez:    MESSAGE_TYPE,
		TypezID:  ".test.ChildMessage",
	}
	map_message := &Message{
		Name:    "SingularMapEntry",
		Package: "test",
		ID:      ".test.ParentMessage.SingularMapEntry",
		IsMap:   true,
		Fields:  []*Field{key, value},
	}

	model := NewTestAPI([]*Message{parent, child, map_message}, []*Enum{}, []*Service{})
	LabelRecursiveFields(model)
	for _, field := range []*Field{value, field0, field1} {
		if !field.Recursive {
			t.Errorf("expected IsRecursive to be true for field %s", field.ID)
		}
	}
	if key.Recursive {
		t.Errorf("expected IsRecursive to be false for field %s", key.ID)
	}
}

func TestReferencedCycle(t *testing.T) {
	field0 := &Field{
		Name:    "parent",
		ID:      ".test.ChildMessage.parent",
		Typez:   MESSAGE_TYPE,
		TypezID: ".test.ParentMessage",
	}
	child := &Message{
		Name:   "ChildMessage",
		ID:     ".test.ChildMessage",
		Fields: []*Field{field0},
	}
	field1 := &Field{
		Name:    "child",
		ID:      ".test.ParentMessage.child",
		Typez:   MESSAGE_TYPE,
		TypezID: ".test.ChildMessage",
	}
	parent := &Message{
		Name:   "ParentdMessage",
		ID:     ".test.ParentMessage",
		Fields: []*Field{field1},
	}

	field2 := &Field{
		Name:    "ref",
		ID:      ".test.Holder.ref",
		Typez:   MESSAGE_TYPE,
		TypezID: ".test.ParentMessage",
	}
	holder := &Message{
		Name:   "Holder",
		ID:     ".test.Holder",
		Fields: []*Field{field2},
	}

	model := NewTestAPI([]*Message{holder, parent, child}, []*Enum{}, []*Service{})
	LabelRecursiveFields(model)
	for _, field := range []*Field{field0, field1} {
		if !field.Recursive {
			t.Errorf("expected IsRecursive to be true for field %s", field.ID)
		}
	}
	for _, field := range []*Field{field2} {
		if field.Recursive {
			t.Errorf("expected IsRecursive to be false for field %s", field.ID)
		}
	}
}
