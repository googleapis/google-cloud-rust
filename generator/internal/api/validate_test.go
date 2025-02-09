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

import "testing"

func TestValidate(t *testing.T) {
	model := NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}},
		[]*Enum{{Name: "e1", Package: "p1"}},
		[]*Service{{Name: "s1", Package: "p1"}})
	model.PackageName = "p1"
	if err := Validate(model); err != nil {
		t.Errorf("unexpected error in API validation %q", err)
	}
}

func TestValidateMessageMismatch(t *testing.T) {
	test := NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*Enum{{Name: "e1", Package: "p1"}},
		[]*Service{{Name: "s1", Package: "p1"}})
	test.PackageName = "p1"
	if err := Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", test.PackageName)
	}

	test = NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}},
		[]*Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*Service{{Name: "s1", Package: "p1"}})
	test.PackageName = "p1"
	if err := Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", test.PackageName)
	}

	test = NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}},
		[]*Enum{{Name: "e1", Package: "p1"}},
		[]*Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	test.PackageName = "p1"
	if err := Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", test.PackageName)
	}
}

func TestValidateMessageMismatchNoPackage(t *testing.T) {
	test := NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}, {Name: "m2", Package: "p2"}},
		[]*Enum{{Name: "e1", Package: "p1"}},
		[]*Service{{Name: "s1", Package: "p1"}})
	if err := Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", test.PackageName)
	}

	test = NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}},
		[]*Enum{{Name: "e1", Package: "p1"}, {Name: "e2", Package: "p2"}},
		[]*Service{{Name: "s1", Package: "p1"}})
	if err := Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", test.PackageName)
	}

	test = NewTestAPI(
		[]*Message{{Name: "m1", Package: "p1"}},
		[]*Enum{{Name: "e1", Package: "p1"}},
		[]*Service{{Name: "s1", Package: "p1"}, {Name: "s2", Package: "p2"}})
	if err := Validate(test); err == nil {
		t.Errorf("expected an error in API validation got=%s", test.PackageName)
	}
}
