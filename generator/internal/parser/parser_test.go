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

func checkMessage(t *testing.T, got *api.Message, want *api.Message) {
	t.Helper()
	// Checking Parent, Messages, Fields, and OneOfs requires special handling.
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(api.Message{}, "Fields", "OneOfs", "Parent", "Messages")); diff != "" {
		t.Errorf("message attributes mismatch (-want +got):\n%s", diff)
	}
	less := func(a, b *api.Field) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Fields, got.Fields, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("field mismatch (-want, +got):\n%s", diff)
	}
	// Ignore parent because types are cyclic
	if diff := cmp.Diff(want.OneOfs, got.OneOfs, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("oneofs mismatch (-want, +got):\n%s", diff)
	}
}

func checkEnum(t *testing.T, got api.Enum, want api.Enum) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(api.Enum{}, "Values", "UniqueNumberValues", "Parent")); diff != "" {
		t.Errorf("mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *api.EnumValue) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Values, got.Values, cmpopts.SortSlices(less), cmpopts.IgnoreFields(api.EnumValue{}, "Parent")); diff != "" {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
}

func checkService(t *testing.T, got *api.Service, want *api.Service) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(api.Service{}, "Methods")); diff != "" {
		t.Errorf("mismatched service attributes (-want, +got):\n%s", diff)
	}
	less := func(a, b *api.Method) bool { return a.Name < b.Name }
	if diff := cmp.Diff(want.Methods, got.Methods, cmpopts.SortSlices(less)); diff != "" {
		t.Errorf("method mismatch (-want, +got):\n%s", diff)
	}
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
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched data for method %s (-want, +got):\n%s", name, diff)
	}
}
