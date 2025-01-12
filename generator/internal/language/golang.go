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

func newGoCodec(options map[string]string) (*goCodec, error) {
	year, _, _ := time.Now().Date()
	codec := &goCodec{
		generationYear: fmt.Sprintf("%04d", year),
		importMap:      map[string]*goImport{},
	}
	for key, definition := range options {
		switch {
		case key == "package-name-override":
			codec.packageNameOverride = definition
		case key == "go-package-name":
			codec.goPackageName = definition
		case key == "copyright-year":
			codec.generationYear = definition
		case key == "not-for-publication":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `not-for-publication` value %q to boolean: %w", definition, err)
			}
			codec.doNotPublish = value
		case strings.HasPrefix(key, "import-mapping"):
			keys := strings.Split(key, ":")
			if len(keys) != 2 {
				return nil, fmt.Errorf("key should be in the format import-mapping:proto.path, got=%q", key)
			}
			defs := strings.Split(definition, ";")
			if len(defs) != 2 {
				return nil, fmt.Errorf("%s should be in the format path;name, got=%q", definition, keys[1])
			}
			codec.importMap[keys[1]] = &goImport{
				path: defs[0],
				name: defs[1],
			}
		}
	}
	return codec, nil
}

type goCodec struct {
	// The source package name (e.g. google.iam.v1 in Protobuf). The codec can
	// generate code for one source package at a time.
	sourceSpecificationPackageName string
	// The year when the files were first generated.
	generationYear string
	// Package name override. If not empty, overrides the default package name.
	packageNameOverride string
	// The package name to generate code into
	goPackageName string
	// A map containing package id to import path information
	importMap map[string]*goImport
	// Some packages are not intended for publication. For example, they may be
	// intended only for testing the generator or the SDK, or the service may
	// not be GA.
	doNotPublish bool
}

type goImport struct {
	path string
	name string
}

func (c *goCodec) loadWellKnownTypes(s *api.APIState) {
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

func (*goCodec) fieldAttributes(*api.Field, *api.APIState) []string {
	return []string{}
}

func (c *goCodec) fieldType(f *api.Field, state *api.APIState) string {
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
			key := c.fieldType(m.Fields[0], state)
			val := c.fieldType(m.Fields[1], state)
			out = "map[" + key + "]" + val
			break
		}
		out = "*" + c.messageName(m)
	case api.ENUM_TYPE:
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		out = c.enumName(e)
	default:
		slog.Error("unhandled fieldType", "type", f.Typez, "id", f.TypezID)
	}
	return out
}

func (c *goCodec) primitiveFieldType(f *api.Field, state *api.APIState) string {
	return c.fieldType(f, state)
}

func (c *goCodec) asQueryParameter(f *api.Field, _ *api.APIState) string {
	return fmt.Sprintf("req.%s.to_str()", c.toCamel(f.Name))
}

func (c *goCodec) templatesProvider() templateProvider {
	return func(name string) (string, error) {
		contents, err := goTemplates.ReadFile(name)
		if err != nil {
			return "", err
		}
		return string(contents), nil
	}
}

func (c *goCodec) generatedFiles() []GeneratedFile {
	return walkTemplatesDir(goTemplates, "templates/go")
}

func (c *goCodec) methodInOutTypeName(id string, s *api.APIState) string {
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

func (*goCodec) messageAttributes(*api.Message, *api.APIState) []string {
	return []string{}
}

func (c *goCodec) messageName(m *api.Message) string {
	if m.Parent != nil {
		return c.messageName(m.Parent) + "_" + strcase.ToCamel(m.Name)
	}
	if imp, ok := c.importMap[m.Package]; ok {
		return imp.name + "." + c.toPascal(m.Name)
	}
	return c.toPascal(m.Name)
}

func (c *goCodec) fqMessageName(m *api.Message) string {
	return c.messageName(m)
}

func (c *goCodec) enumName(e *api.Enum) string {
	if e.Parent != nil {
		return c.messageName(e.Parent) + "_" + strcase.ToCamel(e.Name)
	}
	return strcase.ToCamel(e.Name)
}

func (c *goCodec) fqEnumName(e *api.Enum, state *api.APIState) string {
	return c.enumName(e)
}

func (c *goCodec) enumValueName(e *api.EnumValue) string {
	if e.Parent.Parent != nil {
		return c.messageName(e.Parent.Parent) + "_" + strings.ToUpper(e.Name)
	}
	return strings.ToUpper(e.Name)
}

func (c *goCodec) bodyAccessor(m *api.Method) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + strcase.ToCamel(m.PathInfo.BodyFieldPath)
}

func (c *goCodec) httpPathFmt(m *api.PathInfo) string {
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

func (c *goCodec) httpPathArgs(h *api.PathInfo) []string {
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

func (c *goCodec) toSnake(symbol string) string {
	return goEscapeKeyword(c.toSnakeNoMangling(symbol))
}

func (*goCodec) toSnakeNoMangling(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return goEscapeKeyword(symbol)
	}
	return goEscapeKeyword(strcase.ToSnake(symbol))
}

func (*goCodec) toPascal(symbol string) string {
	return goEscapeKeyword(strcase.ToCamel(symbol))
}

func (*goCodec) toCamel(symbol string) string {
	return strcase.ToLowerCamel(symbol)
}

func (*goCodec) formatDocComments(documentation string, _ *api.APIState) []string {
	ss := strings.Split(documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimRightFunc(ss[i], unicode.IsSpace)
	}
	return ss
}

func (*goCodec) requiredPackages() []string {
	return []string{}
}

func (c *goCodec) copyrightYear() string {
	return c.generationYear
}

func (c *goCodec) sourcePackageName() string {
	return c.sourceSpecificationPackageName
}

func (c *goCodec) packageName(api *api.API) string {
	if len(c.packageNameOverride) > 0 {
		return c.packageNameOverride
	}
	return api.Name
}

func (c *goCodec) packageVersion() string {
	// Go does not need package versions in any generated file.
	return ""
}

func (c *goCodec) validatePackageName(newPackage, elementName string) error {
	if c.sourceSpecificationPackageName == newPackage {
		return nil
	}
	// Special exceptions for mixin services
	if newPackage == "google.cloud.location" ||
		newPackage == "google.iam.v1" ||
		newPackage == "google.longrunning" {
		return nil
	}
	if c.sourceSpecificationPackageName == newPackage {
		return nil
	}
	return fmt.Errorf("rust codec requires all top-level elements to be in the same package want=%s, got=%s for %s",
		c.sourceSpecificationPackageName, newPackage, elementName)
}

func (c *goCodec) validate(api *api.API) error {
	// Set the source package. We should always take the first service registered
	// as the source package. Services with mixes will register those after the
	// source package.
	if len(api.Services) > 0 {
		c.sourceSpecificationPackageName = api.Services[0].Package
	} else if len(api.Messages) > 0 {
		c.sourceSpecificationPackageName = api.Messages[0].Package
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

// GoContext provides additional context for the Go template.
type GoContext struct {
	GoPackage string
}

func (c *goCodec) additionalContext(*api.API) any {
	return GoContext{
		GoPackage: c.goPackageName,
	}
}

func (c *goCodec) imports() []string {
	var imports []string
	for _, imp := range c.importMap {
		imports = append(imports, fmt.Sprintf("%q", imp.path))
	}
	return imports
}

func (c *goCodec) notForPublication() bool {
	return c.doNotPublish
}

func (c *goCodec) generateMethod(m *api.Method) bool {
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
