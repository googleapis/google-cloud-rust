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

package language

import (
	"testing"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestPackageNames(t *testing.T) {
	model := newTestAPI(
		[]*api.Message{}, []*api.Enum{},
		[]*api.Service{{Name: "Workflows", Package: "gcp-sdk-workflows-v1"}})
	// Override the default name for test APIs ("Test").
	model.Name = "workflows-v1"
	codec, err := newRustCodec(t.TempDir(), map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	got := newRustTemplateData(model, codec)
	want := "gcp_sdk_workflows_v1"
	if got.PackageNamespace != want {
		t.Errorf("mismatched package namespace, want=%s, got=%s", want, got.PackageNamespace)
	}
}
