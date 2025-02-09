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

package api

import "strings"

func NewTestAPI(messages []*Message, enums []*Enum, services []*Service) *API {
	packageName := ""
	state := &APIState{
		MessageByID: make(map[string]*Message),
		MethodByID:  make(map[string]*Method),
		EnumByID:    make(map[string]*Enum),
		ServiceByID: make(map[string]*Service),
	}
	for _, m := range messages {
		packageName = m.Package
		state.MessageByID[m.ID] = m
	}
	for _, e := range enums {
		packageName = e.Package
		state.EnumByID[e.ID] = e
	}
	for _, s := range services {
		packageName = s.Package
		state.ServiceByID[s.ID] = s
		for _, m := range s.Methods {
			state.MethodByID[m.ID] = m
		}
	}
	for _, m := range messages {
		parentID := parentName(m.ID)
		parent := state.MessageByID[parentID]
		if parent != nil {
			m.Parent = parent
			parent.Messages = append(parent.Messages, m)
		}
	}
	for _, e := range enums {
		parent := state.MessageByID[parentName(e.ID)]
		if parent != nil {
			e.Parent = parent
			parent.Enums = append(parent.Enums, e)
		}
		for _, ev := range e.Values {
			ev.Parent = e
		}
	}

	return &API{
		Name:        "Test",
		PackageName: packageName,
		Messages:    messages,
		Enums:       enums,
		Services:    services,
		State:       state,
	}
}

// Creates a populated API state from lists of messages, enums, and services.
func parentName(id string) string {
	if lastIndex := strings.LastIndex(id, "."); lastIndex != -1 {
		return id[:lastIndex]
	}
	return "."
}
