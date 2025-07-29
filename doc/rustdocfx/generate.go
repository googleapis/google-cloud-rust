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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cbroglie/mustache"
)

type docfxMetadata struct {
	Name    string
	Version string
}

func newDocfxMetadata(c crate) (*docfxMetadata, error) {
	d := new(docfxMetadata)
	d.Name = c.getRootName()
	d.Version = c.Version
	return d, nil
}

type docfxManagedReference struct {
	HasItems      bool
	Items         []docfxItem
	HasReferences bool
	References    []docfxReference
}

func (mangedReference *docfxManagedReference) appendItem(item *docfxItem) error {
	mangedReference.HasItems = true
	mangedReference.Items = append(mangedReference.Items, *item)
	return nil
}

func (mangedReference *docfxManagedReference) prependItem(item *docfxItem) error {
	mangedReference.HasItems = true
	mangedReference.Items = append([]docfxItem{*item}, mangedReference.Items...)
	return nil
}

func (mangedReference *docfxManagedReference) appendReference(reference *docfxReference) error {
	mangedReference.HasReferences = true
	mangedReference.References = append(mangedReference.References, *reference)
	return nil
}

type docfxItem struct {
	Uid         string
	Name        string
	Summary     string
	Type        string
	HasChildren bool
	Children    []string
	Syntax      docfxSyntax
}

type docfxSyntax struct {
	Content       string
	HasParameters bool
	Parameters    []docfxParameter
	HasReturns    bool
	Returns       []docfxParameter
}

func (syntax *docfxSyntax) appendParameter(parameter *docfxParameter) error {
	syntax.HasParameters = true
	syntax.Parameters = append(syntax.Parameters, *parameter)
	return nil
}

func (syntax *docfxSyntax) appendReturn(returnValue *docfxParameter) error {
	syntax.HasReturns = true
	syntax.Returns = append(syntax.Returns, *returnValue)
	return nil
}

type docfxParameter struct {
	Id          string
	Description string
	VarType     string
}

func newDocfxItem(c crate, id string) (*docfxItem, error) {
	var errs []error

	r := new(docfxItem)
	r.Name = c.getName(id)
	uid, err := c.getDocfxUid(id)
	if err != nil {
		errs = append(errs, err)
	}
	r.Uid = uid
	// TODO: This may not map to a correct type in doc fx pipeline type.
	r.Type = c.getKind(id).String()
	r.Summary = c.getDocString(id)

	if len(errs) > 0 {
		return nil, fmt.Errorf("errors creating new DocfxItem docfx yml files for id %s: %w", id, errors.Join(errs...))
	}
	return r, nil
}

func newDocfxItemFromFunction(c crate, parent *docfxItem, id string) (*docfxItem, error) {
	r := new(docfxItem)
	r.Name = c.getName(id)
	r.Uid = c.getDocfxUidWithParentPrefix(parent.Uid, id)

	// Type is explicitly not set.
	r.Summary = c.getDocString(id)

	syntax := new(docfxSyntax)
	for i := 0; i < len(c.Index[id].Inner.Function.Sig.Inputs); i++ {
		parameter := new(docfxParameter)
		if s, ok := c.Index[id].Inner.Function.Sig.Inputs[i][0].(string); ok {
			parameter.Id = s
		}
		syntax.appendParameter(parameter)
	}
	// TODO: Use return value from c.
	docfxReturn := new(docfxParameter)
	docfxReturn.Id = "TODO:Return Id"
	docfxReturn.Description = "TODO:Return Description"
	syntax.appendReturn(docfxReturn)
	r.Syntax = *syntax
	return r, nil
}

func (item *docfxItem) appendChildren(uid string) error {
	item.HasChildren = true
	item.Children = append(item.Children, uid)
	return nil
}

type docfxReference struct {
	Uid        string
	Name       string
	IsExternal bool
	Parent     string
}

// Based off https://dotnet.github.io/docfx/docs/table-of-contents.html#reference-tocs
type docfxTableOfContent struct {
	Name     string
	Uid      string
	HasItems bool
	Items    []docfxTableOfContent
}

func (toc *docfxTableOfContent) appendItem(item docfxTableOfContent) error {
	toc.HasItems = true
	toc.Items = append(toc.Items, item)
	return nil
}

func newDocfxManagedReference(c crate, id string) (*docfxManagedReference, error) {
	var errs []error

	r := new(docfxManagedReference)

	parent, _ := newDocfxItem(c, id)

	reference := new(docfxReference)
	reference.Uid = parent.Uid
	reference.Name = parent.Name
	reference.IsExternal = false
	r.appendReference(reference)

	if c.Index[id].Inner.Module != nil {
		for i := 0; i < len(c.Index[id].Inner.Module.Items); i++ {
			referenceId := idToString(c.Index[id].Inner.Module.Items[i])
			kind := c.getKind(referenceId)
			if kind == undefinedKind {
				// TODO: Remove this check after we can generate gax/external crate references.
				break
			}
			reference := new(docfxReference)
			uid, err := c.getDocfxUid(referenceId)
			if err != nil {
				errs = append(errs, err)
			}
			reference.Uid = uid
			reference.Name = c.getName(referenceId)
			reference.IsExternal = false
			reference.Parent = parent.Uid

			parent.appendChildren(reference.Uid)
			r.appendReference(reference)
		}
	}

	if c.Index[id].Inner.Trait != nil {
		for i := 0; i < len(c.Index[id].Inner.Trait.Items); i++ {
			// This assumes the inner trait items are all functions. Validation and error checking is needed.
			referenceId := idToString(c.Index[id].Inner.Trait.Items[i])
			function, _ := newDocfxItemFromFunction(c, parent, referenceId)
			if c.getKind(referenceId) == functionKind {
				function.Type = "providedmethod"
			} else {
				errs = append(errs, fmt.Errorf("error expected trait item with id %s to be a function instead of %s", referenceId, c.getKind(referenceId)))
				break
			}
			r.appendItem(function)

			reference := new(docfxReference)
			reference.Uid = c.getDocfxUidWithParentPrefix(parent.Uid, referenceId)
			reference.Name = c.getName(referenceId)
			reference.IsExternal = false
			reference.Parent = parent.Uid

			parent.appendChildren(reference.Uid)
			r.appendReference(reference)
		}
	}

	// TODO(NOW): structs
	// "1560" struct/impls/ -> "1890"  implementation/for/resolved_path/id "1560"
	// items: "1886" (with_request) (function)
	// Got to do functions

	// TODOP(NOW): enums

	// The parent item needs to be the first element of items.
	r.prependItem(parent)

	if len(errs) > 0 {
		return nil, fmt.Errorf("errors constructing page for %s: %w", id, errors.Join(errs...))
	}
	return r, nil
}

func generate(c crate, outDir string) error {
	var errs []error

	m, _ := newDocfxMetadata(c)
	s, err := mustache.RenderFile("/usr/local/google/home/chuongph/Desktop/google-cloud-rust/doc/rustdocfx/templates/docs.metadata.json.mustache", m)
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "docs.metadata.json"), []byte(s), 0666); err != nil {
		errs = append(errs, err)
	}

	rootId := idToString(c.Root)
	rootUid, err := c.getDocfxUid(rootId)
	if err != nil {
		errs = append(errs, err)
	}
	toc := docfxTableOfContent{Name: c.getRootName(), Uid: rootUid}

	for id, _ := range c.Index {
		kind := c.getKind(id)
		switch kind {
		case crateKind:
			fallthrough
		case traitKind:
			fallthrough
		case enumKind:
			fallthrough
		case structKind:
			fallthrough
		case typeAliasKind:
			fallthrough
		case moduleKind:
			r, err := newDocfxManagedReference(c, id)
			if err != nil {
				errs = append(errs, err)
			}

			// TODO(NOW): Use a better file path.
			s, _ := mustache.RenderFile("/usr/local/google/home/chuongph/Desktop/google-cloud-rust/doc/rustdocfx/templates/universalReference.yml.mustache", r)
			uid, err := c.getDocfxUid(id)
			if err != nil {
				errs = append(errs, err)
			}
			if err := os.WriteFile(filepath.Join(outDir, fmt.Sprintf("%s.yml", uid)), []byte(s), 0666); err != nil {
				errs = append(errs, err)
			}
			// TODO(NOW): crate is added twice for this table.
			tocItem := docfxTableOfContent{Name: c.getName(id), Uid: uid}
			toc.appendItem(tocItem)
		default:
			// TODO(NOW): Do we need to log errors for unexpcted kinds?
			// errs = append(errs, fmt.Errorf("unexpected path kind, %s, for id %s", c.Paths[id].Kind, id))
		}
	}

	s, _ = mustache.RenderFile("/usr/local/google/home/chuongph/Desktop/google-cloud-rust/doc/rustdocfx/templates/toc.yml.mustache", toc)
	if err := os.WriteFile(filepath.Join(outDir, "toc.yml"), []byte(s), 0666); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors generating docfx yml files for %s: %w", c.Name, errors.Join(errs...))
	}
	return nil
}
