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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cbroglie/mustache"
)

func renderTOC(toc *docfxTableOfContent, outDir string) error {
	// Sort the toc before rendering.
	sort.SliceStable(toc.Items, func(i, j int) bool {
		// Always put the crate as the first item.
		if strings.HasPrefix(toc.Items[i].Uid, "crate.") {
			return true
		} else if strings.HasPrefix(toc.Items[j].Uid, "crate.") {
			return false
		}
		return toc.Items[i].Name < toc.Items[j].Name
	})
	contents, err := templatesProvider("toc.yml.mustache")
	if err != nil {
		return err
	}
	output, err := mustache.RenderPartials(contents, &mustacheProvider{}, toc)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, "toc.yml"), []byte(output), 0644)
}

func simplifiedParentUid(simplifiedId string) string {
	idx := strings.LastIndex(simplifiedId, ".")
	if idx <= 0 {
		return ""
	}
	return simplifiedId[:idx]
}

func simplifiedUid(uid string) string {
	elements := strings.Split(uid, ".")
	switch len(elements) {
	case 0:
		return ""
	case 1:
		return uid
	default:
		return strings.Join(elements[1:], ".")
	}
}

func computeTOC(crate *crate) (*docfxTableOfContent, error) {
	rootId := idToString(crate.Root)
	rootUid, err := crate.getDocfxUid(rootId)
	if err != nil {
		return nil, err
	}

	// The table contents indexed by the UID. We use this to insert each new
	// entry in the right place.
	rootName := crate.getRootName()
	toc := &docfxTableOfContent{
		Name: rootName,
		Uid:  rootUid,
	}
	items := map[string]*docfxTableOfContent{
		rootName: toc,
	}
	insertItem := func(uid, name string) {
		// Add or update the entry in the `items` index.
		var entry *docfxTableOfContent
		indexId := simplifiedUid(uid)
		if e, ok := items[indexId]; ok {
			e.Name = name
			e.Uid = uid
			entry = e
		} else {
			entry = &docfxTableOfContent{
				Name: name,
				Uid:  uid,
			}
			items[indexId] = entry
		}
		// Find the parent entry and insert the entry into the parent Items
		parentId := simplifiedParentUid(indexId)
		var parent *docfxTableOfContent
		if e, ok := items[parentId]; ok {
			parent = e
		} else {
			parent = &docfxTableOfContent{}
			items[parentId] = parent
		}
		parent.appendItem(entry)

	}
	for id := range crate.Index {
		kind := crate.getKind(id)
		switch kind {
		case moduleKind:
			uid, err := crate.getDocfxUid(id)
			if err != nil {
				return nil, err
			}
			name := crate.getName(id)
			insertItem(uid, name)
		case crateKind:
			// There is only one crate per package and it is handled outside
			// this loop.
			continue
		case traitKind, enumKind, structKind, typeAliasKind:
			// Someday we may want to put these into the TOC, skip for now.
			continue
		case functionKind, structFieldKind, variantKind, useKind, assocTypeKind, assocConstKind, strippedModuleKind, implKind:
			// We do not generate an toc item for these. They should be
			// documented as part of their containing type.
			continue
		case undefinedKind:
			fallthrough
		default:
			return nil, fmt.Errorf("unexpected item kind, %s, for id %s", kind, id)
		}
	}
	return toc, nil
}
