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

func LabelRecursiveFields(model *API) {
	for _, message := range model.State.MessageByID {
		for _, field := range message.Fields {
			field.IsRecursive = field.recursivelyReferences(message.ID, model)
		}
	}
}

func (field *Field) recursivelyReferences(messageID string, model *API) bool {
	if field.Typez != MESSAGE_TYPE {
		return false
	}
	if field.TypezID == messageID || field.IsRecursive {
		return true
	}
	if fieldMessage, ok := model.State.MessageByID[field.TypezID]; ok {
		return fieldMessage.recursivelyReferences(messageID, model)
	}
	return false
}

func (message *Message) recursivelyReferences(messageID string, model *API) bool {
	for _, field := range message.Fields {
		if field.recursivelyReferences(messageID, model) {
			return true
		}
	}
	return false
}
