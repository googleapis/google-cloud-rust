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

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
	"google.golang.org/genproto/googleapis/api/annotations"
)

func TestProtobuf_ForceLongrunning(t *testing.T) {
	sc := sample.ServiceConfig()
	sc.Http = &annotations.Http{
		Rules: []*annotations.HttpRule{
			{
				Selector: "google.longrunning.Operations.CancelOperation",
				Pattern: &annotations.HttpRule_Post{
					Post: "/v2/{name=operations/**}:cancel",
				},
			},
			{
				Selector: "google.longrunning.Operations.GetOperation",
				Pattern: &annotations.HttpRule_Get{
					Get: "/v2/{name=operations/**}:cancel",
				},
			},
		},
	}

	wantMethods := mixinMethods{
		".google.longrunning.Operations.GetOperation":    true,
		".google.longrunning.Operations.CancelOperation": true,
	}
	gotMethods, gotDescriptors := loadMixins(sc, true)
	if diff := cmp.Diff(wantMethods, gotMethods); diff != "" {
		t.Errorf("mismatched operations (-want, +got):\n%s", diff)
	}

	names := map[string]bool{}
	for _, d := range gotDescriptors {
		names[d.GetName()] = true
	}
	if _, ok := names["google/cloud/location/locations.proto"]; !ok {
		t.Errorf("Missing longrunning descriptor in %v", gotDescriptors)
	}
}

func TestProtobuf_ForceLongrunningNoRules(t *testing.T) {
	sc := sample.ServiceConfig()
	sc.Http = &annotations.Http{}

	wantMethods := mixinMethods{
		".google.longrunning.Operations.GetOperation": true,
	}
	gotMethods, gotDescriptors := loadMixins(sc, true)
	if diff := cmp.Diff(wantMethods, gotMethods); diff != "" {
		t.Errorf("mismatched operations (-want, +got):\n%s", diff)
	}

	names := map[string]bool{}
	for _, d := range gotDescriptors {
		names[d.GetName()] = true
	}
	if _, ok := names["google/cloud/location/locations.proto"]; !ok {
		t.Errorf("Missing longrunning descriptor in %v", gotDescriptors)
	}
}
