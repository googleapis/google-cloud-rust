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

package rust_prost

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func TestParseOptions(t *testing.T) {
	cfg := &config.Config{
		General: config.GeneralConfig{
			SpecificationFormat: "protobuf",
			SpecificationSource: "../../testdata/googleapis/google/type",
		},
		Source: map[string]string{
			"include-list": "f1.proto,f2.proto",
		},
		Codec: map[string]string{
			"copyright-year":        "2038",
			"package-name-override": "google-cloud-bigtable",
			"root-name":             "test-root",
		},
	}
	got := newCodec(cfg)
	want := &codec{
		GenerationYear: "2038",
		PackageName:    "google-cloud-bigtable",
		RootName:       "test-root",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in codec (-want, +got)\n:%s", diff)
	}
}
