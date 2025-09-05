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

package main

import (
	"os"
	fspath "path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRenderToc(t *testing.T) {
	input := docfxTableOfContent{
		Name: "test-only-root-name",
		Uid:  "00010",
		Items: []*docfxTableOfContent{
			{Name: "module-b", Uid: "module.b"},
			{Name: "test-only-root-name", Uid: "crate.test-only-root"},
			{
				Name: "module-a",
				Uid:  "module.a",
				Items: []*docfxTableOfContent{
					{Name: "submodule-z", Uid: "module.z"},
					{Name: "submodule-y", Uid: "module.y"},
				},
			},
		},
	}
	outDir := t.TempDir()
	if err := renderTOC(&input, outDir); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(fspath.Join(outDir, "toc.yml"))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Split(string(contents), "\n")
	want := []string{
		"### YamlMime:TableOfContent",
		"- uid: 00010",
		"  name: test-only-root-name",
		"  items:",
		"  - uid: crate.test-only-root",
		"    name: test-only-root-name",
		"  - uid: module.a",
		"    name: module-a",
		"    items:",
		"    - uid: module.z",
		"      name: submodule-z",
		"    - uid: module.y",
		"      name: submodule-y",
		"  - uid: module.b",
		"    name: module-b",
		"",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched summary lines in generated YAML (-want +got):\n%s", diff)
	}

}

func TestRenderTocNestedModules(t *testing.T) {
	input, err := testDataPublicCA()
	if err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	toc, err := computeTOC(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := renderTOC(toc, outDir); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(fspath.Join(outDir, "toc.yml"))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Split(string(contents), "\n")
	want := []string{
		"### YamlMime:TableOfContent",
		"- uid: crate.google_cloud_security_publicca_v1",
		"  name: google_cloud_security_publicca_v1",
		"  items:",
		"  - uid: module.google_cloud_security_publicca_v1.builder",
		"    name: builder",
		"    items:",
		"    - uid: module.google_cloud_security_publicca_v1.builder.public_certificate_authority_service",
		"      name: public_certificate_authority_service",
		"  - uid: module.google_cloud_security_publicca_v1.client",
		"    name: client",
		"  - uid: module.google_cloud_security_publicca_v1.model",
		"    name: model",
		"  - uid: module.google_cloud_security_publicca_v1.stub",
		"    name: stub", "",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched summary lines in generated YAML (-want +got):\n%s", diff)
	}

}
