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

type Id = uint32

type crate struct {
	Name     string
	Version  string
	Location string
	Root     Id
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
	// Heuristic to determine item kind.
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
		if c.Index[id].Inner.Module.IsStripped {
			return strippedModuleKind
		}
		return moduleKind
	}
	if c.Index[id].Inner.Function != nil {
		return functionKind
	}
	if c.Index[id].Inner.Impl != nil {
		return implKind
	}
	if c.Index[id].Inner.StructField != nil {
		return structFieldKind
	}
	if c.Index[id].Inner.Variant != nil {
		return variantKind
	}
	if c.Index[id].Inner.Use != nil {
		return useKind
	}
	if c.Index[id].Inner.AssocType != nil {
		return assocTypeKind
	}
	if c.Index[id].Inner.AssocConst != nil {
		return assocConstKind
	}
	return undefinedKind
}

func (c *crate) getName(id string) string {
	return c.Index[id].Name
}

func (c *crate) getDocString(id string) string {
	return fmt.Sprintf("%#v", c.Index[id].Docs)
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
	strippedModuleKind
	functionKind
	implKind
	structFieldKind
	variantKind
	useKind
	assocTypeKind
	assocConstKind
)

var kindName = map[kind]string{
	undefinedKind:      "undefined",
	structKind:         "struct",
	enumKind:           "enum",
	traitKind:          "trait",
	typeAliasKind:      "typealias",
	crateKind:          "crate",
	moduleKind:         "module",
	strippedModuleKind: "stripped_module",
	functionKind:       "function",
	implKind:           "implementation",
	structFieldKind:    "struct_field",
	variantKind:        "variant",
	useKind:            "use",
	assocTypeKind:      "assoc_type",
	assocConstKind:     "assoc_const",
}

func (k kind) String() string {
	return kindName[k]
}

type item struct {
	Id    Id
	Name  string
	Docs  string
	Inner itemEnum
	Attrs []string
}

type itemSummary struct {
	CrateId Id
	Kind    string
	Path    []string
}

type itemEnum struct {
	Module      *module
	Trait       *trait
	Function    *function
	Struct      *structInner
	Enum        *enum
	TypeAlias   *typeAlias `json:"type_alias"`
	Impl        *impl
	StructField *typeEnum `json:"struct_field"`
	Variant     *variant
	Use         *use
	AssocType   *assocType  `json:"assoc_type"`
	AssocConst  *assocConst `json:"assoc_const"`
}

type module struct {
	IsCrate    bool
	Items      []Id
	IsStripped bool `json:"is_stripped"`
}

type trait struct {
	Items []Id
}

type function struct {
	Sig      functionSignature
	Generics generics
	Header   functionHeader
}

type variant struct {
	// Identionally left blank.
}

type use struct {
	// Identionally left blank.
}

type assocType struct {
	// Identionally left blank.
}

type assocConst struct {
	Type  typeEnum `json:"type"`
	Value *string
}

type functionHeader struct {
	IsConst  bool `json:"is_const"`
	IsUnsafe bool `json:"is_unsafe"`
	IsAsync  bool `json:"is_async"`
}

type generics struct {
	Params         []genericParamDef
	WherePredicate []wherePredicate `json:"where_predicates"`
}

type genericParamDef struct {
	Name string
	Kind genericParamDefKind
}

type wherePredicate struct {
	BoundPredicate *BoundPredicate `json:"bound_predicate"`
}

type BoundPredicate struct {
	Type   typeEnum `json:"type"`
	Bounds []genericBound
}

type genericParamDefKind struct {
	GenericParamDefType *genericParamDefKindType `json:"type"`
}

type genericParamDefKindType struct {
	Bounds     []genericBound
	IsSyntheic bool `json:"is_synthetic"`
}

type genericBound struct {
	TraitBound *traitBound `json:"trait_bound"`
	Outlives   *string
}

type traitBound struct {
	Trait path
}

type structInner struct {
	Kind  structInnerKind
	Impls []Id
}

type structInnerKind struct {
	Plain plain
}

type plain struct {
	Fields []Id
}

type enum struct {
	HasStrippedVariants bool `json:"has_stripped_variants"`
	Variants            []Id
	Impls               []Id
}

type typeAlias struct {
	Type *typeEnum
}

type impl struct {
	Items       []uint32
	IsSyntheic  bool `json:"is_synthetic"`
	IsNegative  bool `json:"is_negative"`
	Trait       *path
	BlanketImpl *typeEnum `json:"blanket_impl"`
}

type path struct {
	Path string
	Id   Id
	Args genericArgs
}

func (f *function) toString(name string) (string, error) {
	keywords := ""
	// We do not expect any functions to be const or unsafe.
	if f.Header.IsConst {
		return "", fmt.Errorf("error, IsConst == true")
	}
	if f.Header.IsUnsafe {
		return "", fmt.Errorf("error, IsUnsafe == true")
	}
	if f.Header.IsAsync {
		keywords = "async"
	}
	if f.Sig.IsCVariadic {
		// We do not yet handle c variadic functions.
		return "", fmt.Errorf("error, IsCVariadic == true")
	}

	genericsString := ""
	genericsParams := []string{}
	for i := 0; i < len(f.Generics.Params); i++ {
		if f.Generics.Params[i].Kind.GenericParamDefType != nil {
			param := f.Generics.Params[i].Name
			// Skip as syntheic generics are handled in the parameters.
			// See, https://docs.rs/rustdoc-types/latest/rustdoc_types/enum.GenericParamDefKind.html#variant.Type.field.is_synthetic
			if f.Generics.Params[i].Kind.GenericParamDefType.IsSyntheic {
				continue
			}
			for j := 0; j < len(f.Generics.Params[i].Kind.GenericParamDefType.Bounds); j++ {
				if f.Generics.Params[i].Kind.GenericParamDefType.Bounds[j].TraitBound != nil {
					traitBoundString, err := f.Generics.Params[i].Kind.GenericParamDefType.Bounds[j].TraitBound.Trait.toString()
					if err != nil {
						return "", fmt.Errorf("error generics generation: %w", err)
					}
					param = fmt.Sprintf("%s: %s", f.Generics.Params[i].Name, traitBoundString)
				} else {
					return "", fmt.Errorf("unexpected generics generation")
				}
			}
			genericsParams = append(genericsParams, param)
		}
	}
	if len(genericsParams) > 0 {
		genericsString = fmt.Sprintf("<%s>", strings.Join(genericsParams, ", "))
	}

	args := []string{}
	for i := 0; i < len(f.Sig.Inputs); i++ {
		if s, ok := f.Sig.Inputs[i][0].(string); ok {
			arg := s
			if g, ok := f.Sig.Inputs[i][1].(map[string]interface{}); ok {
				// TODO: Refactor with typeEnum toString().
				if _, ok := g["generic"]; ok {
					// generic "Self" are not listed.
					if g["generic"] != "Self" {
						arg = fmt.Sprintf("%s: %s", arg, g["generic"])
					}
				}
				if g["primitive"] != nil {
					arg = fmt.Sprintf("%s: %s", arg, g["primitive"])
				}
				if g["resolved_path"] != nil {
					b, err := json.Marshal(g["resolved_path"])
					if err != nil {
						return "", fmt.Errorf("error marshaling resolved_path")
					}
					var p path
					err = json.Unmarshal(b, &p)
					if err != nil {
						return "", fmt.Errorf("error Unmarshal resolved_path")
					}
					argString, err := p.toString()
					if err != nil {
						return "", fmt.Errorf("error arg generation: %w", err)
					}
					arg = fmt.Sprintf("%s: %s", arg, argString)
				}
				if g["impl_trait"] != nil {
					b, err := json.Marshal(g["impl_trait"])
					if err != nil {
						return "", fmt.Errorf("error marshaling impl_trait")
					}
					var n []genericBound
					err = json.Unmarshal(b, &n)
					if err != nil {
						return "", fmt.Errorf("error Unmarshal impl_trait")
					}
					if len(n) == 0 {
						return "", fmt.Errorf("error, where param impl trait bounds == 0")
					}
					bounds := []string{}
					for j := 0; j < len(n); j++ {
						if n[j].TraitBound != nil {
							bound, err := n[j].TraitBound.Trait.toString()
							if err != nil {
								return "", fmt.Errorf("error arg generation: %w", err)
							}
							bounds = append(bounds, bound)
						} else if n[j].Outlives != nil {
							bounds = append(bounds, *n[j].Outlives)
						} else {
							return "", fmt.Errorf("unexpected impl trait bound")
						}
					}
					arg = fmt.Sprintf("%s: impl %s", arg, strings.Join(bounds, " + "))
				}
				if g["borrowed_ref"] != nil {
					b, err := json.Marshal(g["borrowed_ref"])
					if err != nil {
						return "", fmt.Errorf("error marshaling borrowed_ref")
					}
					var n borrowedRef
					err = json.Unmarshal(b, &n)
					if err != nil {
						return "", fmt.Errorf("error Unmarshal borrowed_ref")
					}
					argString, err := n.toString()
					if err != nil {
						return "", fmt.Errorf("error arg generation: %w", err)
					}
					arg = fmt.Sprintf("%s: %s", arg, argString)
				}
			}
			args = append(args, arg)
		}
	}
	argString := fmt.Sprintf("(%s)", strings.Join(args, ", "))

	whereString := ""
	wherePredicates := []string{}
	for i := 0; i < len(f.Generics.WherePredicate); i++ {
		if f.Generics.WherePredicate[i].BoundPredicate != nil {
			typeString, err := f.Generics.WherePredicate[i].BoundPredicate.Type.toString()
			if err != nil {
				return "", fmt.Errorf("error where predicate generation: %w", err)
			}
			if len(f.Generics.WherePredicate[i].BoundPredicate.Bounds) == 0 {
				return "", fmt.Errorf("error, where predicate bound == 0")
			}
			bounds := []string{}
			for j := 0; j < len(f.Generics.WherePredicate[i].BoundPredicate.Bounds); j++ {
				if f.Generics.WherePredicate[i].BoundPredicate.Bounds[j].TraitBound != nil {
					// TODO(NOW): Refactor to TraitBound toString().
					bound, err := f.Generics.WherePredicate[i].BoundPredicate.Bounds[j].TraitBound.Trait.toString()
					if err != nil {
						return "", fmt.Errorf("error where predicate generation: %w", err)
					}
					bounds = append(bounds, bound)
				} else if f.Generics.WherePredicate[i].BoundPredicate.Bounds[j].Outlives != nil {
					bounds = append(bounds, *f.Generics.WherePredicate[i].BoundPredicate.Bounds[j].Outlives)
				} else {
					return "", fmt.Errorf("unexpected predicate bound")
				}
			}
			// 4 spaces are used to ident.
			predicate := fmt.Sprintf("    %s: %s,", typeString, strings.Join(bounds, " + "))
			wherePredicates = append(wherePredicates, predicate)
		}
	}
	if len(wherePredicates) > 0 {
		whereString = fmt.Sprintf("\nwhere\n%s", strings.Join(wherePredicates, "\n"))
	}

	returnString := ""
	if f.Sig.Output != nil {
		output, err := f.Sig.Output.toString()
		if err != nil {
			return "", fmt.Errorf("error return generation: %w", err)
		}
		returnString = fmt.Sprintf(" -> %s", output)
	}
	signature := fmt.Sprintf("%s fn %s%s%s%s%s", keywords, name, genericsString, argString, returnString, whereString)
	return signature, nil
}

func (path *path) toString() (string, error) {
	argString := ""
	args := []string{}
	for i := 0; i < len(path.Args.AngleBracketed.Args); i++ {
		arg, err := path.Args.AngleBracketed.Args[i].Type.toString()
		if err != nil {
			return "", fmt.Errorf("path.toString error: %w", err)
		}
		args = append(args, arg)
	}
	if len(args) > 0 {
		argString = fmt.Sprintf("<%s>", strings.Join(args, ", "))
	}
	return fmt.Sprintf("%s%s", path.Path, argString), nil
}

type typeEnum struct {
	ResolvedPath path `json:"resolved_path"`
	Generic      string
	Primitive    string
	Tuple        []typeEnum
	BorrowedRef  *borrowedRef   `json:"borrowed_ref"`
	ImplTrait    []genericBound `json:"impl_trait"`
}

func (t *typeEnum) toString() (string, error) {
	if t.Generic != "" {
		return t.Generic, nil
	}
	if t.Primitive != "" {
		return t.Primitive, nil
	}
	if len(t.Tuple) > 0 {
		elements := []string{}
		for i := 0; i < len(t.Tuple); i++ {
			element, err := t.Tuple[i].toString()
			if err != nil {
				return "", fmt.Errorf("error typeEnum.toString: %w", err)
			}
			elements = append(elements, element)
		}
		return fmt.Sprintf("(%s)", strings.Join(elements, ", ")), nil
	}
	if t.BorrowedRef != nil {
		borrowedRefString, err := t.BorrowedRef.toString()
		if err != nil {
			return "", fmt.Errorf("error typeEnum.toString: %w", err)
		}
		return borrowedRefString, nil
	}
	if len(t.ImplTrait) > 0 {
		bounds := []string{}
		for i := 0; i < len(t.ImplTrait); i++ {
			if t.ImplTrait[i].TraitBound != nil {
				bound, err := t.ImplTrait[i].TraitBound.Trait.toString()
				if err != nil {
					return "", fmt.Errorf("error typeEnum.toString: %w", err)
				}
				bounds = append(bounds, bound)
			} else if t.ImplTrait[i].Outlives != nil {
				bounds = append(bounds, *t.ImplTrait[i].Outlives)
			}
		}
		return fmt.Sprintf("impl %s", strings.Join(bounds, " + ")), nil
	}
	resolvedPathString, err := t.ResolvedPath.toString()
	if err != nil {
		return "", fmt.Errorf("error typeEnum.toString: %w", err)
	}
	return resolvedPathString, nil
}

type borrowedRef struct {
	Lifetime  *string
	IsMutable bool `json:"is_mutable"`
	Type      typeEnum
}

func (t *borrowedRef) toString() (string, error) {
	if t == nil {
		return "", fmt.Errorf("error borrowRef.toString: unexpected nil")
	}
	typeString, err := t.Type.toString()
	if err != nil {
		return "", fmt.Errorf("error borrowRef.toString: %w", err)
	}
	lifetime := ""
	if t.Lifetime != nil {
		lifetime = *t.Lifetime + " "
	}
	if t.IsMutable {
		return fmt.Sprintf("&mut %s%s", lifetime, typeString), nil
	} else {
		return fmt.Sprintf("&%s%s", lifetime, typeString), nil
	}
}

type functionSignature struct {
	Inputs      [][]interface{}
	Output      *typeEnum
	IsCVariadic bool `json:"is_c_variadic"`
}

type genericArgs struct {
	AngleBracketed angleBracketed `json:"angle_bracketed"`
}

type angleBracketed struct {
	Args []genericArg
}

type genericArg struct {
	Type typeEnum
}

func getWorkspaceCrates(jsonBytes []byte) ([]crate, error) {
	var crates []crate
	err := json.Unmarshal(jsonBytes, &crates)
	if err != nil {
		return nil, fmt.Errorf("workspace crate unmarshal error: %w", err)
	}
	return crates, nil
}

func unmarshalRustdoc(crate *crate, jsonBytes []byte) {
	json.Unmarshal(jsonBytes, &crate)
}

func idToString(id Id) string {
	return strconv.FormatUint(uint64(id), 10)
}
