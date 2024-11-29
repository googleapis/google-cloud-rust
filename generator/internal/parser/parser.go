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

package parser

import "strings"

func splitApiName(name string) (string, string) {
	li := strings.LastIndex(name, ".")
	if li == -1 {
		return "", name
	}
	return name[:li], name[li+1:]
}

func wellKnownMixin(apiName string) bool {
	return strings.HasPrefix(apiName, "google.cloud.location.Location") ||
		strings.HasPrefix(apiName, "google.longrunning.Operations") ||
		strings.HasPrefix(apiName, "google.iam.v1.IAMPolicy")
}
