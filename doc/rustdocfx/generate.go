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
	"slices"
	"sort"
	"time"

	"github.com/cbroglie/mustache"
)

type docfxMetadata struct {
	Name              string
	Version           string
	UpdateTimeSeconds int64
	UpdateTimeNano    int
}

func newDocfxMetadata(c *crate) (*docfxMetadata, error) {
	d := new(docfxMetadata)
	d.Name = c.getRootName()
	d.Version = c.Version
	now := time.Now().UTC()
	d.UpdateTimeSeconds = now.Unix()
	d.UpdateTimeNano = now.Nanosecond()
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

func newDocfxItem(c *crate, id string) (*docfxItem, error) {
	var errs []error

	r := new(docfxItem)
	r.Name = c.getName(id)
	uid, err := c.getDocfxUid(id)
	if err != nil {
		errs = append(errs, err)
	}
	r.Uid = uid
	r.Type = c.getKind(id).String()
	r.Summary = c.getDocString(id)

	if len(errs) > 0 {
		return nil, fmt.Errorf("errors creating new DocfxItem docfx yml files for id %s: %w", id, errors.Join(errs...))
	}
	return r, nil
}

func processTrait(c *crate, id string, page *docfxManagedReference, parent *docfxItem) error {
	for i := 0; i < len(c.Index[id].Inner.Trait.Items); i++ {
		// This assumes the inner trait items are all functions. Validation and error checking is needed.
		referenceId := idToString(c.Index[id].Inner.Trait.Items[i])
		kind := c.getKind(referenceId)
		switch kind {
		case functionKind:
			function, err := newDocfxItemFromFunction(c, parent, referenceId)
			if err != nil {
				return fmt.Errorf("error processing trait item with id %s: %w", id, err)
			}
			function.Type = "providedmethod"
			page.appendItem(function)

			reference, err := newDocfxReferenceFromDocfxItem(function, parent)
			if err != nil {
				return fmt.Errorf("error processing trait item with id %s: %w", id, err)
			}
			parent.appendChildren(reference.Uid)
			page.appendReference(reference)
		case assocTypeKind:
			// TODO: Add associated types.
			continue
		default:
			return fmt.Errorf("error expected trait item with id %s to be a function instead of %s", referenceId, kind)
		}
	}
	return nil
}

func processModule(c *crate, id string, page *docfxManagedReference, parent *docfxItem) error {
	for i := 0; i < len(c.Index[id].Inner.Module.Items); i++ {
		referenceId := idToString(c.Index[id].Inner.Module.Items[i])
		kind := c.getKind(referenceId)
		if kind == useKind {
			// TODO: Remove this check after we can generate gax/external crate references.
			continue
		}
		reference := new(docfxReference)
		uid, err := c.getDocfxUid(referenceId)
		if err != nil {
			return err
		}
		reference.Uid = uid
		reference.Name = c.getName(referenceId)
		reference.IsExternal = false
		reference.Parent = parent.Uid

		parent.appendChildren(reference.Uid)
		page.appendReference(reference)
	}
	return nil
}

func processStruct(c *crate, id string, page *docfxManagedReference, parent *docfxItem) error {
	if c.Index[id].Inner.Struct != nil {
		isNonExhaustive := isNonExhaustive(c.Index[id].Attrs)
		for i := 0; i < len(c.Index[id].Inner.Struct.Kind.Plain.Fields); i++ {
			fieldId := idToString(c.Index[id].Inner.Struct.Kind.Plain.Fields[i])
			field, err := newDocfxItemFromField(c, parent, fieldId)
			if err != nil {
				return fmt.Errorf("error processing struct item with id %s: %w", id, err)
			}
			if isNonExhaustive {
				// TODO: Change to fieldnonexhaustive when https://github.com/googleapis/doc-pipeline/pull/698 is merged/pushed.
				// field.Type = "fieldnonexhaustive"
				field.Type = "enumvariantnonexhaustive"
			} else {
				field.Type = "field"
			}
			page.appendItem(field)

			reference, err := newDocfxReferenceFromDocfxItem(field, parent)
			if err != nil {
				return fmt.Errorf("error processing struct item with id %s: %w", id, err)
			}
			parent.appendChildren(reference.Uid)
			page.appendReference(reference)
		}

		for i := 0; i < len(c.Index[id].Inner.Struct.Impls); i++ {
			referenceId := idToString(c.Index[id].Inner.Struct.Impls[i])
			// TODO: This assumes the inner struct impls are all impls. Validation and error checking is needed.
			err := processImplementation(c, referenceId, page, parent)
			if err != nil {
				return fmt.Errorf("error processing struct item with id %s: %w", id, err)
			}
		}
	}
	return nil
}

func processTypeAlias(c *crate, id string, page *docfxManagedReference, parent *docfxItem) error {
	if c.Index[id].Inner.TypeAlias != nil {
		// Generates a type alias doc string in the following format:
		// pub type LhsIdentifier = RhsIdentifier<Args>
		LhsIdentifier := c.Index[id].Name
		typeAliasString := fmt.Sprintf("pub type %s = %s;", LhsIdentifier, c.Index[id].Inner.TypeAlias.Type.ResolvedPath.toString())
		// TODO: Create code block in the item Summary for the type alias string.
		parent.Summary = fmt.Sprintf("%#v", typeAliasString+"\n"+c.Index[id].Docs)
	}
	return nil
}

func isNonExhaustive(attrs []string) bool {
	return slices.IndexFunc(attrs, func(attr string) bool { return attr == "#[non_exhaustive]" }) >= 0
}

func processEnum(c *crate, id string, page *docfxManagedReference, parent *docfxItem) error {
	if c.Index[id].Inner.Enum.HasStrippedVariants {
		return fmt.Errorf("error processing enum, expecting %s to have no stripped variants", id)
	}

	isNonExhaustive := isNonExhaustive(c.Index[id].Attrs)

	// Adds the variants
	for i := 0; i < len(c.Index[id].Inner.Enum.Variants); i++ {
		variantId := idToString(c.Index[id].Inner.Enum.Variants[i])

		enumVariant, err := newDocfxItemFromEnumVariant(c, parent, variantId)
		if err != nil {
			return fmt.Errorf("error processing enum item with id %s: %w", id, err)
		}
		if isNonExhaustive {
			enumVariant.Type = "enumvariantnonexhaustive"
		} else {
			enumVariant.Type = "enumvariant"
		}
		page.appendItem(enumVariant)

		reference, err := newDocfxReferenceFromDocfxItem(enumVariant, parent)
		if err != nil {
			return fmt.Errorf("error processing enum item with id %s: %w", id, err)
		}
		parent.appendChildren(reference.Uid)
		page.appendReference(reference)
	}

	for i := 0; i < len(c.Index[id].Inner.Enum.Impls); i++ {
		// TODO: This assumes the inner enum impls are all impls. Validation and error checking is needed.
		referenceId := idToString(c.Index[id].Inner.Enum.Impls[i])
		err := processImplementation(c, referenceId, page, parent)
		if err != nil {
			return fmt.Errorf("error processing enum item with id %s: %w", id, err)
		}
	}
	return nil
}

func processImplementation(c *crate, id string, page *docfxManagedReference, parent *docfxItem) error {
	if c.Index[id].Inner.Impl.BlanketImpl != nil {
		// TODO: Add blanket implementations.
		return nil
	}

	if c.Index[id].Inner.Impl.IsSyntheic {
		// TODO: Add autotraitimplementation.
		return nil
	}

	if c.Index[id].Inner.Impl.Trait != nil {
		// TODO: Add traitimplementation.
		return nil
	}

	for j := 0; j < len(c.Index[id].Inner.Impl.Items); j++ {
		innerImplItemId := idToString(c.Index[id].Inner.Impl.Items[j])
		innerImplItemKind := c.getKind(innerImplItemId)
		switch innerImplItemKind {
		case functionKind:
			function, err := newDocfxItemFromFunction(c, parent, innerImplItemId)
			if err != nil {
				return fmt.Errorf("error processing item with id %s: %w", id, err)
			}
			function.Type = "implementation"
			page.appendItem(function)

			reference, err := newDocfxReferenceFromDocfxItem(function, parent)
			if err != nil {
				return fmt.Errorf("error processing item with id %s: %w", id, err)
			}
			parent.appendChildren(reference.Uid)
			page.appendReference(reference)
		case assocConstKind:
			// TODO: Add associated types.
			continue
		default:
			return fmt.Errorf("error expected implementation with id %s to be a function instead of %s", innerImplItemId, innerImplItemKind)
		}
	}
	return nil
}

func newDocfxItemFromAssocConst(c *crate, parent *docfxItem, id string) (*docfxItem, error) {
	r := new(docfxItem)
	r.Name = c.getName(id)
	r.Uid = c.getDocfxUidWithParentPrefix(parent.Uid, id)
	r.Type = "implementation"

	typeString := c.Index[id].Inner.AssocConst.Type.toString()
	constString := fmt.Sprintf("const %s: %s = %s%s", c.Index[id].Name, typeString, *c.Index[id].Inner.AssocConst.Value, typeString)
	// TODO: Need to handle special case where value is "_". Generated rustdoc json currently do not handle consts that reference another value.
	r.Summary = fmt.Sprintf("%#v%#v", constString, c.getDocString(id))
	return r, nil
}

func newDocfxItemFromFunction(c *crate, parent *docfxItem, id string) (*docfxItem, error) {
	r := new(docfxItem)
	r.Name = c.getName(id)
	r.Uid = c.getDocfxUidWithParentPrefix(parent.Uid, id)

	functionSignature, err := c.Index[id].Inner.Function.toString(c.getName(id))
	if err != nil {
		return r, fmt.Errorf("error generating function signature for id %s: %w", id, err)
	}

	// Type is explicitly not set as this function is used for multiple doc pipeline types.
	r.Summary = fmt.Sprintf("%#v%#v", functionSignature, c.getDocString(id))
	return r, nil
}

func newDocfxItemFromImpl(c *crate, parent *docfxItem, id string) (*docfxItem, error) {
	r := new(docfxItem)
	name := c.Index[id].Inner.Impl.Trait.Path

	r.Name = name
	r.Summary = fmt.Sprintf("impl %s for %s", name, parent.Name)
	r.Uid = parent.Uid + "." + name
	if c.Index[id].Inner.Impl.IsNegative {
		// TODO: Update the name when the implementation is negative as r.Name cannot start with '!'
		r.Summary = fmt.Sprintf("impl !%s for %s", name, parent.Name)
	}
	return r, nil
}

func newDocfxItemFromEnumVariant(c *crate, parent *docfxItem, id string) (*docfxItem, error) {
	r := new(docfxItem)
	r.Name = c.getName(id)
	r.Uid = c.getDocfxUidWithParentPrefix(parent.Uid, id)
	r.Summary = c.getDocString(id)
	return r, nil
}

func newDocfxItemFromField(c *crate, parent *docfxItem, id string) (*docfxItem, error) {
	r := new(docfxItem)
	// TODO: Add the field type to Name.
	r.Name = c.getName(id)
	r.Uid = c.getDocfxUidWithParentPrefix(parent.Uid, id)
	r.Summary = c.getDocString(id)
	return r, nil
}

func newDocfxReferenceFromDocfxItem(item, parent *docfxItem) (*docfxReference, error) {
	reference := new(docfxReference)
	if item == nil {
		return nil, fmt.Errorf("expecting item != nil")
	}
	reference.Uid = item.Uid
	reference.Name = item.Name
	reference.IsExternal = false
	if parent != nil {
		reference.Parent = parent.Uid
	}
	return reference, nil
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

func newDocfxManagedReference(c *crate, id string) (*docfxManagedReference, error) {
	r := new(docfxManagedReference)

	parent, err := newDocfxItem(c, id)
	if err != nil {
		return nil, fmt.Errorf("error constructing page for %s: %w", id, err)
	}

	reference, err := newDocfxReferenceFromDocfxItem(parent, nil)
	if err != nil {
		return nil, fmt.Errorf("error constructing page for %s: %w", id, err)
	}
	r.appendReference(reference)

	kind := c.getKind(id)
	switch kind {
	case traitKind:
		err = processTrait(c, id, r, parent)
	case crateKind:
		fallthrough
	case moduleKind:
		err = processModule(c, id, r, parent)
	case structKind:
		err = processStruct(c, id, r, parent)
	case typeAliasKind:
		err = processTypeAlias(c, id, r, parent)
	case enumKind:
		err = processEnum(c, id, r, parent)
	default:
		err = fmt.Errorf("unexpected kind for id %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("error constructing %s page for %s: %w", kind, id, err)
	}

	err = r.prependItem(parent)
	if err != nil {
		return nil, fmt.Errorf("error constructing page for %s: %w", id, err)
	}
	return r, nil
}

func generate(c *crate, projectRoot string, outDir string) error {
	var errs []error

	m, err := newDocfxMetadata(c)
	if err != nil {
		errs = append(errs, err)
	}
	s, err := mustache.RenderFile(filepath.Join(projectRoot, "doc/rustdocfx/templates/docs.metadata.mustache"), m)
	if err != nil {
		errs = append(errs, err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "docs.metadata"), []byte(s), 0666); err != nil {
		errs = append(errs, err)
	}

	rootId := idToString(c.Root)
	rootUid, err := c.getDocfxUid(rootId)
	if err != nil {
		errs = append(errs, err)
	}
	toc := docfxTableOfContent{Name: c.getRootName(), Uid: rootUid}

	for id := range c.Index {
		switch c.getKind(id) {
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

			s, err := mustache.RenderFile(filepath.Join(projectRoot, "doc/rustdocfx/templates/universalReference.yml.mustache"), r)
			if err != nil {
				errs = append(errs, err)
			}
			uid, err := c.getDocfxUid(id)
			if err != nil {
				errs = append(errs, err)
			}
			if err := os.WriteFile(filepath.Join(outDir, fmt.Sprintf("%s.yml", uid)), []byte(s), 0666); err != nil {
				errs = append(errs, err)
			}
			if c.getKind(id) == moduleKind {
				tocItem := docfxTableOfContent{Name: c.getName(id), Uid: uid}
				toc.appendItem(tocItem)
			}
		case functionKind:
			fallthrough
		case structFieldKind:
			fallthrough
		case variantKind:
			fallthrough
		case useKind:
			fallthrough
		case assocTypeKind:
			fallthrough
		case assocConstKind:
			fallthrough
		case strippedModuleKind:
			fallthrough
		case implKind:
			// We do not generate a page these kinds as they are stripped or are used as inner items in other pages.
			continue
		case undefinedKind:
			fallthrough
		default:
			errs = append(errs, fmt.Errorf("unexpected item kind, %s, for id %s", c.getKind(id), id))
		}
	}

	// Sort the toc before rendering.
	sort.SliceStable(toc.Items, func(i, j int) bool { return toc.Items[i].Name < toc.Items[j].Name })
	s, err = mustache.RenderFile(filepath.Join(projectRoot, "doc/rustdocfx/templates/toc.yml.mustache"), toc)
	if err != nil {
		errs = append(errs, err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "toc.yml"), []byte(s), 0666); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors generating docfx yml files: %w", errors.Join(errs...))
	}
	return nil
}
