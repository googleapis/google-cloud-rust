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
	"maps"
	"slices"
)

type ServiceDependencies struct {
	Messages []string
	Enums    []string
}

// FindServiceDependencies returns the message and enum IDs that are required by
// a given service.
//
// The function traverses `model` starting from the definition of `serviceID`.
//   - Any message used by a method of the service is included in the results.
//   - Any message used by LROs of the service is included in the results.
//   - The results are recursively scanned searching for any fields of the
//     messages included above.
//   - If a nested message is included in the results, then the parent message
//     is also included (recursively) in the results.
func FindServiceDependencies(model *API, serviceID string) *ServiceDependencies {
	service, ok := model.State.ServiceByID[serviceID]
	if !ok {
		return &ServiceDependencies{}
	}
	state := newFindServicesState(serviceID, model)
	state.seed(service)
	state.search()
	return &ServiceDependencies{
		Messages: slices.Sorted(maps.Keys(state.Messages)),
		Enums:    slices.Sorted(maps.Keys(state.Enums)),
	}
}

type findServiceState struct {
	// The enums already included in these results
	Enums map[string]bool
	// The messages already included in these results
	Messages   map[string]bool
	model      *API
	candidates []*Message
	serviceID  string
}

func newFindServicesState(serviceID string, model *API) *findServiceState {
	return &findServiceState{
		Enums:     map[string]bool{},
		Messages:  map[string]bool{},
		model:     model,
		serviceID: serviceID,
	}
}

func (state *findServiceState) seed(service *Service) {
	for _, method := range service.Methods {
		state.addCandidate(method.InputTypeID)
		state.addCandidate(method.OutputTypeID)
		if method.OperationInfo != nil {
			state.addCandidate(method.OperationInfo.MetadataTypeID)
			state.addCandidate(method.OperationInfo.ResponseTypeID)
		}
	}
}

func (state *findServiceState) search() {
	for len(state.candidates) > 0 {
		candidate := state.candidates[len(state.candidates)-1]
		state.candidates = state.candidates[0 : len(state.candidates)-1]
		state.recurse(candidate)
	}
}

func (state *findServiceState) recurse(msg *Message) {
	if _, ok := state.Messages[msg.ID]; ok {
		return
	}
	state.Messages[msg.ID] = true
	for _, field := range msg.Fields {
		switch field.Typez {
		case ENUM_TYPE:
			state.addEnum(field.TypezID)
		case MESSAGE_TYPE:
			state.addMessage(field.TypezID)
		default:
		}
	}
}

func (state *findServiceState) addEnum(id string) {
	if e, ok := state.model.State.EnumByID[id]; ok {
		if e.Parent != nil {
			state.addCandidate(e.Parent.ID)
		}
		state.Enums[id] = true
	}
}

func (state *findServiceState) addMessage(id string) {
	state.addCandidate(id)
	if m, ok := state.model.State.MessageByID[id]; ok {
		if m.Parent != nil {
			state.addCandidate(m.Parent.ID)
		}
	}
}

func (state *findServiceState) addCandidate(id string) {
	msg, ok := state.model.State.MessageByID[id]
	if !ok {
		return
	}
	if _, ok := state.Messages[msg.ID]; ok {
		return
	}
	state.candidates = append(state.candidates, msg)
}
