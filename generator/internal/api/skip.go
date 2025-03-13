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
	"slices"
	"strings"
)

func SkipModelElements(model *API, options map[string]string) {
	ids, ok := options["skipped-ids"]
	if !ok {
		return
	}
	skippedIDs := map[string]bool{}
	for _, id := range strings.Split(ids, ",") {
		skippedIDs[id] = true
	}
	for _, m := range model.Messages {
		skipMessageElements(m, skippedIDs)
	}
	model.Enums = slices.DeleteFunc(model.Enums, func(x *Enum) bool { return skippedIDs[x.ID] })
	model.Messages = slices.DeleteFunc(model.Messages, func(x *Message) bool { return skippedIDs[x.ID] })
	model.Services = slices.DeleteFunc(model.Services, func(x *Service) bool { return skippedIDs[x.ID] })
	for _, service := range model.State.ServiceByID {
		service.Methods = slices.DeleteFunc(service.Methods, func(x *Method) bool { return skippedIDs[x.ID] })
	}
}

func skipMessageElements(message *Message, skippedIDs map[string]bool) {
	for _, m := range message.Messages {
		skipMessageElements(m, skippedIDs)
	}
	message.Messages = slices.DeleteFunc(message.Messages, func(x *Message) bool { return skippedIDs[x.ID] })
	message.Enums = slices.DeleteFunc(message.Enums, func(x *Enum) bool { return skippedIDs[x.ID] })
}
