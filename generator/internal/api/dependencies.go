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
)

// Returns the IDs of model elements required by the given set of `ids`
//
// We can think of the `model` as a directed graph, with nodes for each of the
// model elements. Finding required elements is a graph traversal problem.
//
// First observe that:
//   - if we are given a service, we want to include all of its methods.
//   - if we are given a method, we need to include its service, but not
//     necessarily any of its sibling methods.
//
// This implies that simply fanning out over the nodes is not sufficient.
//
// We resolve this by making two passes. In the first pass, we fan out over all
// child elements of the given elements. In the second pass, we add all required
// parents of any found elements.
//
// In the first pass, we compute the [reachable set] of the given `ids`, with
// edges from...
//
// - a service to each of its methods
// - a method to its request/response messages
// - an LRO method to its metadata/response type messages
// - a message to each of its fields' types (if they are a message or enum)
//
// In the second pass, we compute the reachable set of the nodes found from the
// first pass. This time, the graph has edges from...
//
// - a method to its service
// - a child message to its parent message
// - a child enum to its parent message
// - a message to each of its fields' types (if they are a message or enum)
//   - this edge is only necessary because we are too lazy to support pruning
//     the fields of a message.
//
// [reachable set]: https://en.wikipedia.org/wiki/Reachability
func FindDependencies(model *API, ids []string) (map[string]bool, error) {
	includedIDs := map[string]bool{}
	candidates := []string{}

	add := func(id string) {
		if _, ok := includedIDs[id]; !ok {
			candidates = append(candidates, id)
		}
		includedIDs[id] = true
	}

	// Seed with the given ids
	for _, id := range ids {
		add(id)
	}

	// Fan out over all child elements of the given nodes.
	for len(candidates) > 0 {
		id := candidates[len(candidates)-1]
		candidates = candidates[0 : len(candidates)-1]

		// Recurse one level, depending on the input type.
		service, ok := model.State.ServiceByID[id]
		if ok {
			for _, method := range service.Methods {
				add(method.ID)
			}
			continue
		}

		method, ok := model.State.MethodByID[id]
		if ok {
			add(method.InputTypeID)
			add(method.OutputTypeID)
			if method.OperationInfo != nil {
				add(method.OperationInfo.MetadataTypeID)
				add(method.OperationInfo.ResponseTypeID)
			}
			continue
		}

		message, ok := model.State.MessageByID[id]
		if ok {
			for _, field := range message.Fields {
				if field.Typez == ENUM_TYPE || field.Typez == MESSAGE_TYPE {
					add(field.TypezID)
				}
			}
			continue
		}

		_, ok = model.State.EnumByID[id]
		if ok {
			continue
		}

		return nil, fmt.Errorf("FindDependencies reached unknown ID=%q", id)
	}

	// Do a second pass, this time making sure everything has a parent. In
	// this pass, we do not fan out over child elements.
	for id := range includedIDs {
		candidates = append(candidates, id)
	}
	for len(candidates) > 0 {
		id := candidates[len(candidates)-1]
		candidates = candidates[0 : len(candidates)-1]

		// Recurse one level, depending on the input type.
		method, ok := model.State.MethodByID[id]
		if ok {
			add(method.Service.ID)
			continue
		}

		message, ok := model.State.MessageByID[id]
		if ok {
			if message.Parent != nil {
				add(message.Parent.ID)
			}
			// In the current definition of APIState, a message must
			// includes all of its fields.
			for _, field := range message.Fields {
				if field.Typez == ENUM_TYPE || field.Typez == MESSAGE_TYPE {
					add(field.TypezID)
				}
			}
			continue
		}

		enum, ok := model.State.EnumByID[id]
		if ok {
			if enum.Parent != nil {
				add(enum.Parent.ID)
			}
			continue
		}
	}

	return includedIDs, nil
}
