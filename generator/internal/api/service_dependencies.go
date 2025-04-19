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
	"maps"
	"slices"
)

type Dependencies struct {
	Messages []string
	Enums    []string
}

// ServiceDependencies returns the message and enum IDs that are required by
// a given service.
//
// The function traverses `model` starting from the definition of `service`.
//   - Any message used by a method of service is included in the results.
//   - Any message used by LROs of the service is included in the results.
//   - The results are recursively scanned searching for any fields of the
//     messages included above.
//   - If a nested message is included in the results, then the parent message
//     is also included (recursively) in the results.
func ServiceDependencies(model *API, serviceID string) *Dependencies {
	service, ok := model.State.ServiceByID[serviceID]
	if !ok {
		return &Dependencies{}
	}
	state := newSearchState(serviceID, model)
	state.seed(service)
	state.search()
	return &Dependencies{
		Messages: slices.Sorted(maps.Keys(state.Messages)),
		Enums:    slices.Sorted(maps.Keys(state.Enums)),
	}
}

type searchState struct {
	// The message IDs that have already been visited
	Visited map[string]bool
	// The enums already included in these results
	Enums map[string]bool
	// The messages already included in these results
	Messages   map[string]bool
	model      *API
	candidates []*Message
	serviceID  string
}

func newSearchState(serviceID string, model *API) *searchState {
	return &searchState{
		Visited:   map[string]bool{},
		Enums:     map[string]bool{},
		Messages:  map[string]bool{},
		model:     model,
		serviceID: serviceID,
	}
}

func (state *searchState) seed(service *Service) {
	for _, method := range service.Methods {
		state.addCandidate(method.InputTypeID)
		state.addCandidate(method.OutputTypeID)
		if method.OperationInfo != nil {
			state.addCandidate(method.OperationInfo.MetadataTypeID)
			state.addCandidate(method.OperationInfo.ResponseTypeID)
		}
	}
}

func (state *searchState) search() {
	for len(state.candidates) > 0 {
		candidate := state.candidates[len(state.candidates)-1]
		state.candidates = state.candidates[0 : len(state.candidates)-1]

		if _, ok := state.Visited[candidate.ID]; ok {
			if state.serviceID == ".google.cloud.aiplatform.v1.GenAiTuningService" {
				fmt.Printf("search(%s) - skip %s\n", state.serviceID, candidate.ID)
			}
			continue
		}
		state.Messages[candidate.ID] = true
		state.recurse(candidate)
	}
	// Some APIs include messages that are not used by any of its services.
}

func (state *searchState) recurse(msg *Message) {
	if state.serviceID == ".google.cloud.aiplatform.v1.GenAiTuningService" {
		fmt.Printf("search(%s) - recurse %s\n", state.serviceID, msg.ID)
	}
	state.Visited[msg.ID] = true
	for _, field := range msg.Fields {
		if state.serviceID == ".google.cloud.aiplatform.v1.GenAiTuningService" && field.TypezID != "" {
			fmt.Printf("    search(%s) - recurse %s / %s\n", state.serviceID, msg.ID, field.TypezID)
		}
		switch field.Typez {
		case ENUM_TYPE:
			state.addEnum(field.TypezID)
		case MESSAGE_TYPE:
			state.addMessage(field.TypezID)
		default:
		}
	}
}

func (state *searchState) addEnum(id string) {
	if e, ok := state.model.State.EnumByID[id]; ok {
		if e.Parent != nil {
			state.addCandidate(e.Parent.ID)
		}
		state.Enums[id] = true
	}
}

func (state *searchState) addMessage(id string) {
	state.addCandidate(id)
	if m, ok := state.model.State.MessageByID[id]; ok {
		if m.Parent != nil {
			state.addCandidate(m.Parent.ID)
		}
		if !m.IsMap {
			state.Messages[id] = true
		}
	}
}

func (state *searchState) addCandidate(id string) {
	msg, ok := state.model.State.MessageByID[id]
	if !ok {
		return
	}
	if _, ok := state.Visited[msg.ID]; ok {
		return
	}
	state.candidates = append(state.candidates, msg)
}
