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

package parser

import (
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
)

// Adds auto-populated fields to methods. Fields that do not conform to
// [AIP-4235](https://google.aip.dev/client-libraries/4235) are skipped.
//
// The first phases of the parser have no knowledge of the service config
// settings and marks any fields that *might* be auto-populated (having the
// right type and annotations) as `AutoPopulated: true`. This phase applies the
// service configuration settings.
func updateAutoPopulatedFields(serviceConfig *serviceconfig.Service, model *api.API) {
	if serviceConfig == nil {
		return
	}
	for _, m := range serviceConfig.GetPublishing().GetMethodSettings() {
		selector := m.GetSelector()
		method, ok := model.State.MethodByID["."+selector]
		if !ok {
			continue
		}
		message, ok := model.State.MessageByID[method.InputTypeID]
		if !ok {
			continue
		}

		for _, name := range m.GetAutoPopulatedFields() {
			for _, field := range message.Fields {
				if field.Name != name {
					continue
				}
				if field.AutoPopulated {
					method.AutoPopulated = append(method.AutoPopulated, field)
				}
				break
			}
		}
	}
}
