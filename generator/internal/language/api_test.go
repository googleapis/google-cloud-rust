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

package language

import (
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
)

func newTestAPI(messages []*genclient.Message, enums []*genclient.Enum, services []*genclient.Service) *genclient.API {
	state := &genclient.APIState{
		MessageByID: make(map[string]*genclient.Message),
		EnumByID:    make(map[string]*genclient.Enum),
		ServiceByID: make(map[string]*genclient.Service),
	}
	for _, m := range messages {
		state.MessageByID[m.ID] = m
	}
	for _, e := range enums {
		state.EnumByID[e.ID] = e
	}
	for _, s := range services {
		state.ServiceByID[s.ID] = s
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
	}

	return &genclient.API{
		Name:     "Test",
		Messages: messages,
		Enums:    enums,
		Services: services,
		State:    state,
	}
}

// Creates a populated API state from lists of messages, enums, and services.
func parentName(id string) string {
	if lastIndex := strings.LastIndex(id, "."); lastIndex != -1 {
		return id[:lastIndex]
	}
	return "."
}
