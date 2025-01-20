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

package parser

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func checkMessage(t *testing.T, got api.Message, want api.Message) {
	t.Helper()
	// Checking Parent, Messages, Fields, and OneOfs requires special handling.
	if diff := cmp.Diff(want, got, messageIgnores(), fieldIgnores()); diff != "" {
		t.Errorf("message attributes mismatch (-want +got):\n%s", diff)
	}
	less := func(a, b *api.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Fields, got.Fields, cmpopts.SortSlices(less), fieldIgnores()); diff != "" {
		t.Errorf("field mismatch (-want, +got):\n%s", diff)
	}

	lessOneOf := func(a, b *api.OneOf) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.OneOfs, got.OneOfs, cmpopts.SortSlices(lessOneOf), oneOfIgnores(), fieldIgnores()); diff != "" {
		t.Errorf("oneofs mismatch (-want, +got):\n%s", diff)
	}
}

func checkEnum(t *testing.T, got api.Enum, want api.Enum) {
	t.Helper()
	if diff := cmp.Diff(want, got, enumIgnores()); diff != "" {
		t.Errorf("mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *api.EnumValue) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Values, got.Values, cmpopts.SortSlices(less), cmpopts.IgnoreFields(api.EnumValue{}, "Parent")); diff != "" {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}

func checkService(t *testing.T, got *api.Service, want *api.Service) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(api.Service{}, "Methods", "API")); diff != "" {
		t.Errorf("mismatched service attributes (-want, +got):\n%s", diff)
	}
	for _, m := range got.Methods {
		if m.Parent != got {
			t.Errorf("mismatched method parent want=%v, got=%v", got, m.Parent)
		}
	}
	compareMethods(t, want.Methods, got.Methods)
}

func checkMethod(t *testing.T, service *api.Service, name string, want *api.Method) {
	t.Helper()
	findMethod := func(name string) (*api.Method, bool) {
		for _, method := range service.Methods {
			if method.Name == name {
				return method, true
			}
		}
		return nil, false
	}
	got, ok := findMethod(name)
	if !ok {
		t.Errorf("missing method %s", name)
	}
	compareMethods(t, []*api.Method{want}, []*api.Method{got})
}

func compareMethods(t *testing.T, want []*api.Method, got []*api.Method) {
	t.Helper()
	less := func(a, b *api.Method) bool { return a.Name < b.Name }
	diff := cmp.Diff(want, got,
		cmpopts.SortSlices(less),
		methodIgnores(),
		pathInfoIgnores(),
		pathSegmentIgnores(),
		messageIgnores(),
		fieldIgnores(),
		enumIgnores(),
		oneOfIgnores(),
	)

	if diff != "" {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}

func messageIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.Message{}, "Fields", "OneOfs", "Enums", "Messages", "Parent", "API", "ElementsByName")
}

func methodIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.Method{}, "Parent", "InputType", "OutputType")
}

func pathInfoIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.PathInfo{}, "Method")
}

func pathSegmentIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.PathSegment{}, "Parent")
}

func fieldIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.Field{}, "Parent")
}

func enumIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.Enum{}, "Values", "Parent", "API")
}

func oneOfIgnores() cmp.Option {
	return cmpopts.IgnoreFields(api.OneOf{}, "Parent")
}
