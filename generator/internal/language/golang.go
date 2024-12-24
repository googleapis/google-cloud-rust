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
	"embed"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/iancoleman/strcase"
)

//go:embed templates/go
var goTemplates embed.FS

func NewGoCodec(options map[string]string) (*GoCodec, error) {
	year, _, _ := time.Now().Date()
	codec := &GoCodec{
		GenerationYear: fmt.Sprintf("%04d", year),
		ImportMap:      map[string]*GoImport{},
	}
	for key, definition := range options {
		switch {
		case key == "package-name-override":
			codec.PackageNameOverride = definition
		case key == "go-package-name":
			codec.GoPackageName = definition
		case key == "copyright-year":
			codec.GenerationYear = definition
		case key == "not-for-publication":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `not-for-publication` value %q to boolean: %w", definition, err)
			}
			codec.DoNotPublish = value
		case strings.HasPrefix(key, "import-mapping"):
			keys := strings.Split(key, ":")
			if len(keys) != 2 {
				return nil, fmt.Errorf("key should be in the format import-mapping:proto.path, got=%q", key)
			}
			defs := strings.Split(definition, ";")
			if len(defs) != 2 {
				return nil, fmt.Errorf("%s should be in the format path;name, got=%q", definition, keys[1])
			}
			codec.ImportMap[keys[1]] = &GoImport{
				Path: defs[0],
				Name: defs[1],
			}
		}
	}
	return codec, nil
}

type GoCodec struct {
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
	ImportMap map[string]*GoImport
	// Some packages are not intended for publication. For example, they may be
	// intended only for testing the generator or the SDK, or the service may
	// not be GA.
	DoNotPublish bool
}

type GoImport struct {
	Path string
	Name string
}

func (c *GoCodec) LoadWellKnownTypes(s *api.APIState) {
	timestamp := &api.Message{
		ID:      ".google.protobuf.Timestamp",
		Name:    "Time",
		Package: "time",
	}
	duration := &api.Message{
		ID:      ".google.protobuf.Duration",
		Name:    "Duration",
		Package: "time",
	}
	s.MessageByID[timestamp.ID] = timestamp
	s.MessageByID[duration.ID] = duration
}

func (*GoCodec) FieldAttributes(*api.Field, *api.APIState) []string {
	return []string{}
}

func (c *GoCodec) FieldType(f *api.Field, state *api.APIState) string {
	var out string
	switch f.Typez {
	case api.STRING_TYPE:
		out = "string"
	case api.INT64_TYPE:
		out = "int64"
	case api.INT32_TYPE:
		out = "int32"
	case api.BOOL_TYPE:
		out = "bool"
	case api.BYTES_TYPE:
		out = "[]byte"
	case api.MESSAGE_TYPE:
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
	case api.ENUM_TYPE:
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

func (c *GoCodec) PrimitiveFieldType(f *api.Field, state *api.APIState) string {
	return c.FieldType(f, state)
}

func (c *GoCodec) AsQueryParameter(f *api.Field, state *api.APIState) string {
	return fmt.Sprintf("req.%s.to_str()", c.ToCamel(f.Name))
}

func (c *GoCodec) TemplatesProvider() TemplateProvider {
	return func(name string) (string, error) {
		contents, err := goTemplates.ReadFile(name)
		if err != nil {
			return "", err
		}
		return string(contents), nil
	}
}

func (c *GoCodec) GeneratedFiles() []GeneratedFile {
	return walkTemplatesDir(goTemplates, "templates/go")
}

func (c *GoCodec) MethodInOutTypeName(id string, s *api.APIState) string {
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

func (*GoCodec) MessageAttributes(*api.Message, *api.APIState) []string {
	return []string{}
}

func (c *GoCodec) MessageName(m *api.Message, state *api.APIState) string {
	if m.Parent != nil {
		return c.MessageName(m.Parent, state) + "_" + strcase.ToCamel(m.Name)
	}
	if imp, ok := c.ImportMap[m.Package]; ok {
		return imp.Name + "." + c.ToPascal(m.Name)
	}
	return c.ToPascal(m.Name)
}

func (c *GoCodec) FQMessageName(m *api.Message, state *api.APIState) string {
	return c.MessageName(m, state)
}

func (c *GoCodec) EnumName(e *api.Enum, state *api.APIState) string {
	if e.Parent != nil {
		return c.MessageName(e.Parent, state) + "_" + strcase.ToCamel(e.Name)
	}
	return strcase.ToCamel(e.Name)
}

func (c *GoCodec) FQEnumName(e *api.Enum, state *api.APIState) string {
	return c.EnumName(e, state)
}

func (c *GoCodec) EnumValueName(e *api.EnumValue, state *api.APIState) string {
	if e.Parent.Parent != nil {
		return c.MessageName(e.Parent.Parent, state) + "_" + strings.ToUpper(e.Name)
	}
	return strings.ToUpper(e.Name)
}

func (c *GoCodec) FQEnumValueName(v *api.EnumValue, state *api.APIState) string {
	return c.EnumValueName(v, state)
}

func (c *GoCodec) OneOfType(o *api.OneOf, _ *api.APIState) string {
	panic("not needed for Go")
}

func (c *GoCodec) BodyAccessor(m *api.Method, state *api.APIState) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + strcase.ToCamel(m.PathInfo.BodyFieldPath)
}

func (c *GoCodec) HTTPPathFmt(m *api.PathInfo, state *api.APIState) string {
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

func (c *GoCodec) HTTPPathArgs(h *api.PathInfo, state *api.APIState) []string {
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

func (c *GoCodec) ToSnake(symbol string) string {
	return goEscapeKeyword(c.ToSnakeNoMangling(symbol))
}

func (*GoCodec) ToSnakeNoMangling(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return goEscapeKeyword(symbol)
	}
	return goEscapeKeyword(strcase.ToSnake(symbol))
}

func (*GoCodec) ToPascal(symbol string) string {
	return goEscapeKeyword(strcase.ToCamel(symbol))
}

func (*GoCodec) ToCamel(symbol string) string {
	return strcase.ToLowerCamel(symbol)
}

func (*GoCodec) FormatDocComments(documentation string, _ *api.APIState) []string {
	ss := strings.Split(documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimRightFunc(ss[i], unicode.IsSpace)
	}
	return ss
}

func (*GoCodec) RequiredPackages() []string {
	return []string{}
}

func (c *GoCodec) CopyrightYear() string {
	return c.GenerationYear
}

func (c *GoCodec) PackageName(api *api.API) string {
	if len(c.PackageNameOverride) > 0 {
		return c.PackageNameOverride
	}
	return api.Name
}

func (c *GoCodec) PackageVersion() string {
	// Go does not need package versions in any generated file.
	return ""
}

func (c *GoCodec) validatePackageName(newPackage, elementName string) error {
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

func (c *GoCodec) Validate(api *api.API) error {
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

func (c *GoCodec) AdditionalContext(*api.API) any {
	return GoContext{
		GoPackage: c.GoPackageName,
	}
}

func (c *GoCodec) Imports() []string {
	var imports []string
	for _, imp := range c.ImportMap {
		imports = append(imports, fmt.Sprintf("%q", imp.Path))
	}
	return imports
}

func (c *GoCodec) NotForPublication() bool {
	return c.DoNotPublish
}

func (c *GoCodec) GenerateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations, we cannot generate working
	// RPCs for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	return !m.ClientSideStreaming && !m.ServerSideStreaming && m.PathInfo != nil && len(m.PathInfo.PathTemplate) != 0
}

// The list of Golang keywords and reserved words can be found at:
//
// https://go.dev/ref/spec#Keywords
func goEscapeKeyword(symbol string) string {
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
