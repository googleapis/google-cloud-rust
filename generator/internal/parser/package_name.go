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

import "github.com/googleapis/google-cloud-rust/generator/internal/api"

// updatePackageName() sets the PackageName field if it is not set. This happens
// often with protobuf libraries that lack a service config YAML file, typically
// type-only libraries.
func updatePackageName(model *api.API) {
	if model.PackageName != "" {
		return
	}
	if len(model.Services) > 0 {
		model.PackageName = model.Services[0].Package
	} else if len(model.Messages) > 0 {
		model.PackageName = model.Messages[0].Package
	} else if len(model.Enums) > 0 {
		model.PackageName = model.Enums[0].Package
	}
}
