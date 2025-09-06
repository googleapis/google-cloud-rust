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
		Modules: []*docfxTableOfContent{
			{
				Name: "module-b",
				Uid:  "module.b",
				Modules: []*docfxTableOfContent{
					{Name: "module-c", Uid: "module-c"},
					{Name: "module-d", Uid: "module-d"},
				},
				Traits: []*docfxTableOfContent{
					{Name: "TraitA", Uid: "trait-a"},
					{Name: "TraitB", Uid: "trait-b"},
				},
				Structs: []*docfxTableOfContent{
					{Name: "StructA", Uid: "struct-a"},
					{Name: "StructB", Uid: "struct-b"},
				},
				Enums: []*docfxTableOfContent{
					{Name: "EnumA", Uid: "enum-a"},
					{Name: "EnumB", Uid: "enum-b"},
				},
				Aliases: []*docfxTableOfContent{
					{Name: "AliasA", Uid: "alias-a"},
					{Name: "AliasB", Uid: "alias-b"},
				},
			},
			{
				Name: "module-only-modules",
				Uid:  "module-only-modules",
				Modules: []*docfxTableOfContent{
					{Name: "module-c", Uid: "module-c"},
					{Name: "module-d", Uid: "module-d"},
				},
			},
			{
				Name: "module-only-traits",
				Uid:  "module-only-traits",
				Traits: []*docfxTableOfContent{
					{Name: "TraitA", Uid: "trait-a"},
					{Name: "TraitB", Uid: "trait-b"},
				},
			},
			{
				Name: "module-only-structs",
				Uid:  "module-only-structs",
				Structs: []*docfxTableOfContent{
					{Name: "StructA", Uid: "struct-a"},
					{Name: "StructB", Uid: "struct-b"},
				},
			},
			{
				Name: "module-only-enums",
				Uid:  "module-only-enums",
				Enums: []*docfxTableOfContent{
					{Name: "EnumA", Uid: "enum-a"},
					{Name: "EnumB", Uid: "enum-b"},
				},
			},
			{
				Name: "module-only-alias",
				Uid:  "module-only-alias",
				Aliases: []*docfxTableOfContent{
					{Name: "AliasA", Uid: "alias-a"},
					{Name: "AliasB", Uid: "alias-b"},
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
		"  - name: Modules",
		"    items:",
		"    - uid: module.b",
		"      name: module-b",
		"      items:",
		"      - name: Modules",
		"        items:",
		"        - uid: module-c",
		"          name: module-c",
		"        - uid: module-d",
		"          name: module-d",
		"      - name: Traits",
		"        items:",
		"        - uid: trait-a",
		"          name: TraitA",
		"        - uid: trait-b",
		"          name: TraitB",
		"      - name: Structs",
		"        items:",
		"        - uid: struct-a",
		"          name: StructA",
		"        - uid: struct-b",
		"          name: StructB",
		"      - name: Enums",
		"        items:",
		"        - uid: enum-a",
		"          name: EnumA",
		"        - uid: enum-b",
		"          name: EnumB",
		"      - name: Type Aliases",
		"        items:",
		"        - uid: alias-a",
		"          name: AliasA",
		"        - uid: alias-b",
		"          name: AliasB",
		"    - uid: module-only-alias",
		"      name: module-only-alias",
		"      items:",
		"      - name: Type Aliases",
		"        items:",
		"        - uid: alias-a",
		"          name: AliasA",
		"        - uid: alias-b",
		"          name: AliasB",
		"    - uid: module-only-enums",
		"      name: module-only-enums",
		"      items:",
		"      - name: Enums",
		"        items:",
		"        - uid: enum-a",
		"          name: EnumA",
		"        - uid: enum-b",
		"          name: EnumB",
		"    - uid: module-only-modules",
		"      name: module-only-modules",
		"      items:",
		"      - name: Modules",
		"        items:",
		"        - uid: module-c",
		"          name: module-c",
		"        - uid: module-d",
		"          name: module-d",
		"    - uid: module-only-structs",
		"      name: module-only-structs",
		"      items:",
		"      - name: Structs",
		"        items:",
		"        - uid: struct-a",
		"          name: StructA",
		"        - uid: struct-b",
		"          name: StructB",
		"    - uid: module-only-traits",
		"      name: module-only-traits",
		"      items:",
		"      - name: Traits",
		"        items:",
		"        - uid: trait-a",
		"          name: TraitA",
		"        - uid: trait-b",
		"          name: TraitB",
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
		"  - name: Modules",
		"    items:",
		"    - uid: module.google_cloud_security_publicca_v1.builder",
		"      name: builder",
		"      items:",
		"      - name: Modules",
		"        items:",
		"        - uid: module.google_cloud_security_publicca_v1.builder.public_certificate_authority_service",
		"          name: public_certificate_authority_service",
		"          items:",
		"          - name: Structs",
		"            items:",
		"            - uid: struct.google_cloud_security_publicca_v1.builder.public_certificate_authority_service.CreateExternalAccountKey",
		"              name: CreateExternalAccountKey",
		"          - name: Type Aliases",
		"            items:",
		"            - uid: typealias.google_cloud_security_publicca_v1.builder.public_certificate_authority_service.ClientBuilder",
		"              name: ClientBuilder",
		"    - uid: module.google_cloud_security_publicca_v1.client",
		"      name: client",
		"      items:",
		"      - name: Structs",
		"        items:",
		"        - uid: struct.google_cloud_security_publicca_v1.client.PublicCertificateAuthorityService",
		"          name: PublicCertificateAuthorityService",
		"    - uid: module.google_cloud_security_publicca_v1.model",
		"      name: model",
		"      items:",
		"      - name: Structs",
		"        items:",
		"        - uid: struct.google_cloud_security_publicca_v1.model.CreateExternalAccountKeyRequest",
		"          name: CreateExternalAccountKeyRequest",
		"        - uid: struct.google_cloud_security_publicca_v1.model.ExternalAccountKey",
		"          name: ExternalAccountKey",
		"    - uid: module.google_cloud_security_publicca_v1.stub",
		"      name: stub",
		"      items:",
		"      - name: Traits",
		"        items:",
		"        - uid: trait.google_cloud_security_publicca_v1.stub.PublicCertificateAuthorityService",
		"          name: PublicCertificateAuthorityService", "",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched summary lines in generated YAML (-want +got):\n%s", diff)
	}

}
