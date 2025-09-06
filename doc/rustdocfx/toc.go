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
	"strings"
)

// docfxTableOfContent is a context for mustache templates in `_toc.yaml` files.
//
// Based off https://dotnet.github.io/docfx/docs/table-of-contents.html#reference-tocs
type docfxTableOfContent struct {
	Name    string
	Uid     string
	Modules []*docfxTableOfContent
	Traits  []*docfxTableOfContent
	Structs []*docfxTableOfContent
	Enums   []*docfxTableOfContent
	Aliases []*docfxTableOfContent
}

func (toc *docfxTableOfContent) HasModules() bool {
	return len(toc.Modules) != 0
}

func (toc *docfxTableOfContent) HasTraits() bool {
	return len(toc.Traits) != 0
}

func (toc *docfxTableOfContent) HasStructs() bool {
	return len(toc.Structs) != 0
}

func (toc *docfxTableOfContent) HasEnums() bool {
	return len(toc.Enums) != 0
}

func (toc *docfxTableOfContent) HasAliases() bool {
	return len(toc.Aliases) != 0
}

func (toc *docfxTableOfContent) HasItems() bool {
	return toc.HasModules() || toc.HasTraits() || toc.HasStructs() || toc.HasEnums() || toc.HasAliases()
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
	insertItem := func(id string) (*docfxTableOfContent, *docfxTableOfContent, error) {
		uid, err := crate.getDocfxUid(id)
		if err != nil {
			return nil, nil, err
		}
		name := crate.getName(id)
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
		return parent, entry, nil
	}

	for id := range crate.Index {
		kind := crate.getKind(id)
		switch kind {
		case moduleKind:
			parent, entry, err := insertItem(id)
			if err != nil {
				return nil, err
			}
			parent.Modules = append(parent.Modules, entry)
		case crateKind:
			// There is only one crate per package and it is handled outside
			// this loop.
			continue
		case traitKind:
			parent, entry, err := insertItem(id)
			if err != nil {
				return nil, err
			}
			parent.Traits = append(parent.Traits, entry)
		case structKind:
			parent, entry, err := insertItem(id)
			if err != nil {
				return nil, err
			}
			parent.Structs = append(parent.Structs, entry)
		case enumKind:
			parent, entry, err := insertItem(id)
			if err != nil {
				return nil, err
			}
			parent.Enums = append(parent.Enums, entry)
		case typeAliasKind:
			parent, entry, err := insertItem(id)
			if err != nil {
				return nil, err
			}
			parent.Aliases = append(parent.Aliases, entry)
		case functionKind, structFieldKind, variantKind, useKind, assocTypeKind, assocConstKind, strippedModuleKind, implKind:
			// We do not generate an toc item for these. They should be
			// documented as part of their containing type or module.
			continue
		case undefinedKind:
			fallthrough
		default:
			return nil, fmt.Errorf("unexpected item kind, %s, for id %s", kind, id)
		}
	}
	return toc, nil
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
