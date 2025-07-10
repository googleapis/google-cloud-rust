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
	deps := &ServiceDependencies{}

	includedIDs, _ := FindDependencies(model, []string{serviceID})
	for id := range includedIDs {
		_, ok := model.State.MessageByID[id]
		if ok {
			deps.Messages = append(deps.Messages, id)
		}
		_, ok = model.State.EnumByID[id]
		if ok {
			deps.Enums = append(deps.Enums, id)
		}
	}
	return deps
}
