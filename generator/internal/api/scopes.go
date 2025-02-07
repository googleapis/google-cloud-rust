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

import "strings"

// Each element of the model (services, messages, enums) has a series of
// "scopes" associated with it. These are the relative names for symbols
// in the context of the element.
//
// This is intended for discovery of relative and absolute cross-reference links
// in the documentation.
//
// For example, with a proto specification like:
//
// ```proto
// package .test.v1;
//
// message M {
//   message Child {
//     string f1 = 1;
//   }
//   string f1 = 1;
//   Child f2 = 2;
// }
// ```
//
// In the context of `Child` we may say `[f1][]` and that is a cross-reference
// link to `.test.v1.M.Child.f1`.  We may also refer to the same field as
// `[Child.f1][]` or `[M.Child.f1][]` or even `[.test.v1.M.Child.f1]][]`.
//
// Meanwhile, in the context of `M` when we say `[f1][]` that refers to
// `.test.v1.M.f1`.

func (x *Service) Scopes() []string {
	return []string{strings.TrimPrefix(x.ID, "."), x.Package}
}

func (x *Message) Scopes() []string {
	localScope := strings.TrimPrefix(x.ID, ".")
	if x.Parent == nil {
		return []string{localScope, x.Package}
	}
	return append([]string{localScope}, x.Parent.Scopes()...)
}

func (x *Enum) Scopes() []string {
	localScope := strings.TrimPrefix(x.ID, ".")
	if x.Parent == nil {
		return []string{localScope, x.Package}
	}
	return append([]string{localScope}, x.Parent.Scopes()...)
}

func (x *EnumValue) Scopes() []string {
	localScope := strings.TrimPrefix(x.ID, ".")
	if x.Parent == nil {
		return []string{localScope}
	}
	return append([]string{localScope}, x.Parent.Scopes()...)
}
