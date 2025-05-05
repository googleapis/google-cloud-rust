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

func (model *API) HasDeprecatedEntities() bool {
	for _, e := range model.Enums {
		if e.hasDeprecatedEntities() {
			return true
		}
	}
	for _, m := range model.Messages {
		if m.hasDeprecatedEntities() {
			return true
		}
	}
	for _, s := range model.Services {
		if s.hasDeprecatedEntities() {
			return true
		}
	}
	return false
}

func (message *Message) hasDeprecatedEntities() bool {
	if message.Deprecated {
		return true
	}
	for _, m := range message.Messages {
		if m.hasDeprecatedEntities() {
			return true
		}
	}
	for _, e := range message.Enums {
		if e.hasDeprecatedEntities() {
			return true
		}
	}
	for _, f := range message.Fields {
		if f.Deprecated {
			return true
		}
	}
	return false
}

func (enum *Enum) hasDeprecatedEntities() bool {
	if enum.Deprecated {
		return true
	}
	for _, v := range enum.Values {
		if v.Deprecated {
			return true
		}
	}
	return false
}

func (service *Service) hasDeprecatedEntities() bool {
	if service.Deprecated {
		return true
	}
	for _, m := range service.Methods {
		if m.Deprecated {
			return true
		}
	}
	return false
}
