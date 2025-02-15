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

package dart

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestAnnotateModel(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	_, err := annotateModel(model, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	codec := model.Codec.(*modelAnnotations)

	if diff := cmp.Diff("google_cloud_test", codec.PackageName); diff != "" {
		t.Errorf("mismatch in Codec.PackageName (-want, +got)\n:%s", diff)
	}
	if diff := cmp.Diff("test", codec.MainFileName); diff != "" {
		t.Errorf("mismatch in Codec.MainFileName (-want, +got)\n:%s", diff)
	}
}
