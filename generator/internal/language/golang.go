// Copyright 2024 Google LLC
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

package language

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/iancoleman/strcase"
)

func newGoCodec(copts *genclient.CodecOptions) (*goCodec, error) {
	year, _, _ := time.Now().Date()
	codec := &goCodec{
		GenerationYear: fmt.Sprintf("%04d", year),
		ImportMap:      map[string]*Import{},
	}
	for key, definition := range copts.Options {
		switch {
		case key == "package-name-override":
			codec.PackageNameOverride = definition
		case key == "go-package-name":
			codec.GoPackageName = definition
		case strings.HasPrefix(key, "import-mapping"):
			keys := strings.Split(key, ":")
			if len(keys) != 2 {
				return nil, fmt.Errorf("key should be in the format import-mapping:proto.path, got=%q", key)
			}
			defs := strings.Split(definition, ";")
			if len(defs) != 2 {
				return nil, fmt.Errorf("%s should be in the format path;name, got=%q", definition, keys[1])
			}
			codec.ImportMap[keys[1]] = &Import{
				Path: defs[0],
				Name: defs[1],
			}
		}
	}
	return codec, nil
}

type goCodec struct {
	// The source package name (e.g. google.iam.v1 in Protobuf). The codec can
	// generate code for one source package at a time.
	SourceSpecificationPackageName string
	// The year when the files were first generated.
	GenerationYear string
	// Package name override. If not empty, overrides the default package name.
	PackageNameOverride string
	// The package name to generate code into
	GoPackageName string
	// A map containing package id to import path information
	ImportMap map[string]*Import
}

type Import struct {
	Path string
	Name string
}

func (c *goCodec) LoadWellKnownTypes(s *genclient.APIState) {
	timestamp := &genclient.Message{
		ID:      ".google.protobuf.Timestamp",
		Name:    "Time",
		Package: "time",
	}
	duration := &genclient.Message{
		ID:      ".google.protobuf.Duration",
		Name:    "Duration",
		Package: "time",
	}
	s.MessageByID[timestamp.ID] = timestamp
	s.MessageByID[duration.ID] = duration
}

func (*goCodec) FieldAttributes(*genclient.Field, *genclient.APIState) []string {
	return []string{}
}

func (c *goCodec) FieldType(f *genclient.Field, state *genclient.APIState) string {
	var out string
	switch f.Typez {
	case genclient.STRING_TYPE:
		out = "string"
	case genclient.INT64_TYPE:
		out = "int64"
	case genclient.INT32_TYPE:
		out = "int32"
	case genclient.BOOL_TYPE:
		out = "bool"
	case genclient.BYTES_TYPE:
		out = "[]byte"
	case genclient.MESSAGE_TYPE:
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			key := c.FieldType(m.Fields[0], state)
			val := c.FieldType(m.Fields[1], state)
			out = "map[" + key + "]" + val
			break
		}
		out = "*" + c.MessageName(m, state)
	case genclient.ENUM_TYPE:
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		out = c.EnumName(e, state)
	default:
		slog.Error("unhandled fieldType", "type", f.Typez, "id", f.TypezID)
	}
	return out
}

func (c *goCodec) AsQueryParameter(f *genclient.Field, state *genclient.APIState) string {
	return fmt.Sprintf("req.%s.to_str()", c.ToCamel(f.Name))
}

func (c *goCodec) TemplateDir() string {
	return "go"
}

func (c *goCodec) MethodInOutTypeName(id string, s *genclient.APIState) string {
	if id == "" {
		return ""
	}
	m, ok := s.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return strcase.ToCamel(m.Name)
}

func (*goCodec) MessageAttributes(*genclient.Message, *genclient.APIState) []string {
	return []string{}
}

func (c *goCodec) MessageName(m *genclient.Message, state *genclient.APIState) string {
	if m.Parent != nil {
		return c.MessageName(m.Parent, state) + "_" + strcase.ToCamel(m.Name)
	}
	if imp, ok := c.ImportMap[m.Package]; ok {
		return imp.Name + "." + c.ToPascal(m.Name)
	}
	return c.ToPascal(m.Name)
}

func (c *goCodec) FQMessageName(m *genclient.Message, state *genclient.APIState) string {
	return c.MessageName(m, state)
}

func (c *goCodec) EnumName(e *genclient.Enum, state *genclient.APIState) string {
	if e.Parent != nil {
		return c.MessageName(e.Parent, state) + "_" + strcase.ToCamel(e.Name)
	}
	return strcase.ToCamel(e.Name)
}

func (c *goCodec) FQEnumName(e *genclient.Enum, state *genclient.APIState) string {
	return c.EnumName(e, state)
}

func (c *goCodec) EnumValueName(e *genclient.EnumValue, state *genclient.APIState) string {
	if e.Parent.Parent != nil {
		return c.MessageName(e.Parent.Parent, state) + "_" + strings.ToUpper(e.Name)
	}
	return strings.ToUpper(e.Name)
}

func (c *goCodec) FQEnumValueName(v *genclient.EnumValue, state *genclient.APIState) string {
	return c.EnumValueName(v, state)
}

func (c *goCodec) OneOfType(o *genclient.OneOf, _ *genclient.APIState) string {
	panic("not needed for Go")
}

func (c *goCodec) BodyAccessor(m *genclient.Method, state *genclient.APIState) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + strcase.ToCamel(m.PathInfo.BodyFieldPath)
}

func (c *goCodec) HTTPPathFmt(m *genclient.PathInfo, state *genclient.APIState) string {
	fmt := ""
	for _, segment := range m.PathTemplate {
		if segment.Literal != nil {
			fmt = fmt + "/" + *segment.Literal
		} else if segment.FieldPath != nil {
			fmt = fmt + "/%s"
		} else if segment.Verb != nil {
			fmt = fmt + ":" + *segment.Verb
		}
	}
	return fmt
}

func (c *goCodec) HTTPPathArgs(h *genclient.PathInfo, state *genclient.APIState) []string {
	var args []string
	// TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/34
	for _, segment := range h.PathTemplate {
		if segment.FieldPath != nil {
			// TODO(#34) - handle nested path params
			args = append(args, fmt.Sprintf(", req.%s", strcase.ToCamel(*segment.FieldPath)))
		}
	}
	return args
}

func (c *goCodec) QueryParams(m *genclient.Method, state *genclient.APIState) []*genclient.Field {
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup type", "id", m.InputTypeID)
		return nil
	}

	var queryParams []*genclient.Field
	for _, field := range msg.Fields {
		if !m.PathInfo.QueryParameters[field.JSONName] {
			continue
		}
		queryParams = append(queryParams, field)
	}
	return queryParams
}

func (c *goCodec) ToSnake(symbol string) string {
	return EscapeKeyword(c.ToSnakeNoMangling(symbol))
}

func (*goCodec) ToSnakeNoMangling(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return EscapeKeyword(symbol)
	}
	return EscapeKeyword(strcase.ToSnake(symbol))
}

func (*goCodec) ToPascal(symbol string) string {
	return EscapeKeyword(strcase.ToCamel(symbol))
}

func (*goCodec) ToCamel(symbol string) string {
	return strcase.ToLowerCamel(symbol)
}

func (*goCodec) FormatDocComments(documentation string) []string {
	ss := strings.Split(documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimRightFunc(ss[i], unicode.IsSpace)
	}
	return ss
}

func (*goCodec) RequiredPackages() []string {
	return []string{}
}

func (c *goCodec) CopyrightYear() string {
	return c.GenerationYear
}

func (c *goCodec) PackageName(api *genclient.API) string {
	if len(c.PackageNameOverride) > 0 {
		return c.PackageNameOverride
	}
	return api.Name
}

func (c *goCodec) validatePackageName(newPackage, elementName string) error {
	if c.SourceSpecificationPackageName == newPackage {
		return nil
	}
	// Special exceptions for mixin services
	if newPackage == "google.cloud.location" ||
		newPackage == "google.iam.v1" ||
		newPackage == "google.longrunning" {
		return nil
	}
	if c.SourceSpecificationPackageName == newPackage {
		return nil
	}
	return fmt.Errorf("rust codec requires all top-level elements to be in the same package want=%s, got=%s for %s",
		c.SourceSpecificationPackageName, newPackage, elementName)
}

func (c *goCodec) Validate(api *genclient.API) error {
	// Set the source package. We should always take the first service registered
	// as the source package. Services with mixes will register those after the
	// source package.
	if len(api.Services) > 0 {
		c.SourceSpecificationPackageName = api.Services[0].Package
	} else if len(api.Messages) > 0 {
		c.SourceSpecificationPackageName = api.Messages[0].Package
	}
	for _, s := range api.Services {
		if err := c.validatePackageName(s.Package, s.ID); err != nil {
			return err
		}
	}
	for _, s := range api.Messages {
		if err := c.validatePackageName(s.Package, s.ID); err != nil {
			return err
		}
	}
	for _, s := range api.Enums {
		if err := c.validatePackageName(s.Package, s.ID); err != nil {
			return err
		}
	}
	return nil
}

type GoContext struct {
	GoPackage string
}

func (c *goCodec) AdditionalContext() any {
	return GoContext{
		GoPackage: c.GoPackageName,
	}
}

func (c *goCodec) Imports() []string {
	var imports []string
	for _, imp := range c.ImportMap {
		imports = append(imports, fmt.Sprintf("%q", imp.Path))
	}
	return imports
}

// The list of Golang keywords and reserved words can be found at:
//
// https://go.dev/ref/spec#Keywords
func EscapeKeyword(symbol string) string {
	keywords := map[string]bool{
		"break":       true,
		"default":     true,
		"func":        true,
		"interface":   true,
		"select":      true,
		"case":        true,
		"defer":       true,
		"go":          true,
		"map":         true,
		"struct":      true,
		"chan":        true,
		"else":        true,
		"goto":        true,
		"package":     true,
		"switch":      true,
		"const":       true,
		"fallthrough": true,
		"if":          true,
		"range":       true,
		"type":        true,
		"continue":    true,
		"for":         true,
		"import":      true,
		"return":      true,
		"var":         true,
	}
	_, ok := keywords[symbol]
	if !ok {
		return symbol
	}
	return symbol + "_"
}
