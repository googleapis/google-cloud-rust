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

import "fmt"

// Validate verifies the model satisfies the requires to be used by Codecs.
func Validate(model *API) error {
	validatePkg := func(newPackage, elementName string) error {
		if model.PackageName == newPackage {
			return nil
		}
		// Special exceptions for mixin services
		if newPackage == "google.cloud.location" ||
			newPackage == "google.iam.v1" ||
			newPackage == "google.longrunning" {
			return nil
		}
		return fmt.Errorf("sidekick requires all top-level elements to be in the same package want=%q, got=%q for %q",
			model.PackageName, newPackage, elementName)
	}

	for _, s := range model.Services {
		if err := validatePkg(s.Package, s.ID); err != nil {
			return err
		}
	}
	for _, m := range model.Messages {
		if err := validatePkg(m.Package, m.ID); err != nil {
			return err
		}
	}
	for _, e := range model.Enums {
		if err := validatePkg(e.Package, e.ID); err != nil {
			return err
		}
	}
	return nil
}
