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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type crate struct {
	Name     string
	Version  string
	Location string
	Root     uint32
	Index    map[string]item
	Paths    map[string]itemSummary
}

func (c *crate) getRootName() string {
	return c.Index[idToString(c.Root)].Name
}

func (c *crate) getDocfxUid(id string) (string, error) {
	if len(c.Paths[id].Path) > 0 {
		path := strings.Join(c.Paths[id].Path, ".")
		return fmt.Sprintf("%s.%s", c.getKind(id), path), nil
	} else {
		return "", fmt.Errorf("error getting docfx Uid, %s does not have a path", id)
	}
}

func (c *crate) getDocfxUidWithParentPrefix(parentUid, id string) string {
	return parentUid + "." + c.getName(id)
}

func (c *crate) getKind(id string) kind {
	if c.Index[id].Inner.Struct != nil {
		return structKind
	}
	if c.Index[id].Inner.Enum != nil {
		return enumKind
	}
	if c.Index[id].Inner.Trait != nil {
		return traitKind
	}
	if c.Index[id].Inner.TypeAlias != nil {
		return typeAliasKind
	}
	if c.Index[id].Inner.Module != nil {
		if idToString(c.Root) == id {
			return crateKind
		}
		return moduleKind
	}
	if c.Index[id].Inner.Function != nil {
		return functionKind
	}
	// NOWNOW, do get kind for implementation, traitimplementation, autotraitimplementation, blanketimplementation
	// implementation will probably need to nest the items.
	// NOWNOW: Need to add inner.implementations

	// What kind do we have:
	// for Traits, providedmethod
	// for structs, implementation, traitimplementation, autotraitimplementation, blanketimplementation
	return undefinedKind
}

func (c *crate) getName(id string) string {
	return c.Index[id].Name
}

func (c *crate) getDocString(id string) string {
	return c.Index[id].Docs
}

type kind int

const (
	undefinedKind kind = iota
	structKind
	enumKind
	traitKind
	typeAliasKind
	crateKind
	moduleKind
	functionKind
)

var kindName = map[kind]string{
	undefinedKind: "undefined",
	structKind:    "struct",
	enumKind:      "enum",
	traitKind:     "trait",
	typeAliasKind: "type_alias",
	crateKind:     "crate",
	moduleKind:    "module",
	functionKind:  "function",
}

func (k kind) String() string {
	return kindName[k]
}

type item struct {
	Id    uint32
	Name  string
	Docs  string
	Inner itemEnum
}

type itemSummary struct {
	CrateId uint32
	Kind    string
	Path    []string
}

type itemEnum struct {
	Module    *module
	Trait     *trait
	Function  *function
	Struct    *structInner
	Enum      *enum
	TypeAlias *typeAlias `json:"type_alias"`
}

type module struct {
	IsCrate bool
	Items   []uint32
}

type trait struct {
	Items []uint32
}

type function struct {
	Sig functionSignature
}

type structInner struct {
}

type enum struct {
}

type typeAlias struct {
}

type functionSignature struct {
	Inputs [][]interface{}
}

func getWorkspaceCrates(jsonBytes []byte) ([]crate, error) {
	var crates []crate
	err := json.Unmarshal(jsonBytes, &crates)
	if err != nil {
		return nil, fmt.Errorf("workspace crate unmarshal error: %v", err)
	}
	return crates, nil
}

func unmarshalRustdoc(crate *crate, jsonBytes []byte) {
	json.Unmarshal(jsonBytes, &crate)
}

func idToString(id uint32) string {
	return strconv.FormatUint(uint64(id), 10)
}
