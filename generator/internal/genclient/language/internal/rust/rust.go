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

package rust

import (
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/iancoleman/strcase"
)

func NewCodec(copts *genclient.CodecOptions) (*Codec, error) {
	year, _, _ := time.Now().Date()
	codec := &Codec{
		GenerationYear:  fmt.Sprintf("%04d", year),
		OutputDirectory: copts.OutDir,
		ExtraPackages:   []*RustPackage{},
		PackageMapping:  map[string]*RustPackage{},
	}
	for key, definition := range copts.Options {
		switch key {
		case "package-name-override":
			codec.PackageNameOverride = definition
			continue
		case "generate-module":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, err
			}
			codec.GenerateModule = value
			continue
		case "copyright-year":
			codec.GenerationYear = definition
			continue
		}
		if !strings.HasPrefix(key, "package:") {
			continue
		}
		var specificationPackages []string
		pkg := &RustPackage{
			Name: strings.TrimPrefix(key, "package:"),
		}
		for _, element := range strings.Split(definition, ",") {
			s := strings.SplitN(element, "=", 2)
			if len(s) != 2 {
				return nil, fmt.Errorf("the definition for package %s should be a comma-separated list of key=value pairs, got=%q", key, definition)
			}
			switch s[0] {
			case "package":
				pkg.Package = s[1]
			case "path":
				pkg.Path = s[1]
			case "version":
				pkg.Version = s[1]
			case "source":
				specificationPackages = append(specificationPackages, s[1])
			case "feature":
				pkg.Features = append(pkg.Features, strings.Split(s[1], ",")...)
			default:
				return nil, fmt.Errorf("unknown field (%s) in definition of rust package %s, got=%s", s[0], key, definition)
			}
		}
		if pkg.Package == "" {
			return nil, fmt.Errorf("missing rust package name for package %s, got=%s", key, definition)
		}
		codec.ExtraPackages = append(codec.ExtraPackages, pkg)
		for _, source := range specificationPackages {
			codec.PackageMapping[source] = pkg
		}
	}
	return codec, nil
}

type Codec struct {
	// The output directory relative to the project root.
	OutputDirectory string
	// Package name override. If not empty, overrides the default package name.
	PackageNameOverride string
	// The year when the files were first generated.
	GenerationYear string
	// Generate a module of a larger crate, as opposed to a full crate.
	GenerateModule bool
	// Additional Rust packages imported by this module. The Mustache template
	// hardcodes a number of packages, but some are configured via the
	// command-line.
	ExtraPackages []*RustPackage
	// A mapping between the specification package names (typically Protobuf),
	// and the Rust package name that contains these types.
	PackageMapping map[string]*RustPackage
	// The source package name (e.g. google.iam.v1 in Protobuf). The codec can
	// generate code for one source package at a time.
	SourceSpecificationPackageName string
}

type RustPackage struct {
	// The name we import this package under.
	Name string
	// What the Rust package calls itself.
	Package string
	// The path to file the package locally, unused if empty.
	Path string
	// The version of the package, unused if empty.
	Version string
	// Optional features enabled for the package.
	Features []string
}

func (c *Codec) LoadWellKnownTypes(s *genclient.APIState) {
	// TODO(#77) - replace these placeholders with real types
	wellKnown := []*genclient.Message{
		{
			ID:      ".google.protobuf.Any",
			Name:    "Any",
			Package: "google.protobuf",
		},
		{
			ID:      ".google.protobuf.Empty",
			Name:    "Empty",
			Package: "google.protobuf",
		},
		{
			ID:      ".google.protobuf.FieldMask",
			Name:    "FieldMask",
			Package: "google.protobuf",
		},
		{
			ID:      ".google.protobuf.Duration",
			Name:    "Duration",
			Package: "google.protobuf",
		},
		{
			ID:      ".google.protobuf.Timestamp",
			Name:    "Timestamp",
			Package: "google.protobuf",
		},
	}
	for _, message := range wellKnown {
		s.MessageByID[message.ID] = message
	}
}

func ScalarFieldType(f *genclient.Field) string {
	var out string
	switch f.Typez {
	case genclient.DOUBLE_TYPE:
		out = "f64"
	case genclient.FLOAT_TYPE:
		out = "f32"
	case genclient.INT64_TYPE:
		out = "i64"
	case genclient.UINT64_TYPE:
		out = "u64"
	case genclient.INT32_TYPE:
		out = "i32"
	case genclient.FIXED64_TYPE:
		out = "u64"
	case genclient.FIXED32_TYPE:
		out = "u32"
	case genclient.BOOL_TYPE:
		out = "bool"
	case genclient.STRING_TYPE:
		out = "String"
	case genclient.BYTES_TYPE:
		out = "bytes::Bytes"
	case genclient.UINT32_TYPE:
		out = "u32"
	case genclient.SFIXED32_TYPE:
		out = "i32"
	case genclient.SFIXED64_TYPE:
		out = "i64"
	case genclient.SINT32_TYPE:
		out = "i32"
	case genclient.SINT64_TYPE:
		out = "i64"

	default:
		slog.Error("Unexpected field type", "field", *f)
		return ""
	}
	return out
}

func (c *Codec) fieldFormatter(f *genclient.Field) string {
	switch f.Typez {
	case genclient.INT64_TYPE,
		genclient.UINT64_TYPE,
		genclient.FIXED64_TYPE,
		genclient.SFIXED64_TYPE,
		genclient.SINT64_TYPE:
		return "serde_with::DisplayFromStr"
	case genclient.BYTES_TYPE:
		return "serde_with::base64::Base64"
	default:
		return "_"
	}
}

func (c *Codec) FieldAttributes(f *genclient.Field, state *genclient.APIState) []string {
	switch f.Typez {
	case genclient.DOUBLE_TYPE,
		genclient.FLOAT_TYPE,
		genclient.INT32_TYPE,
		genclient.FIXED32_TYPE,
		genclient.BOOL_TYPE,
		genclient.STRING_TYPE,
		genclient.UINT32_TYPE,
		genclient.SFIXED32_TYPE,
		genclient.SINT32_TYPE,
		genclient.ENUM_TYPE,
		genclient.GROUP_TYPE:
		return []string{}

	case genclient.INT64_TYPE,
		genclient.UINT64_TYPE,
		genclient.FIXED64_TYPE,
		genclient.SFIXED64_TYPE,
		genclient.SINT64_TYPE,
		genclient.BYTES_TYPE:
		formatter := c.fieldFormatter(f)
		if f.Optional {
			return []string{fmt.Sprintf(`#[serde_as(as = "Option<%s>")]`, formatter)}
		}
		if f.Repeated {
			return []string{fmt.Sprintf(`#[serde_as(as = "Vec<%s>")]`, formatter)}
		}
		return []string{fmt.Sprintf(`#[serde_as(as = "%s")]`, formatter)}

	case genclient.MESSAGE_TYPE:
		if message, ok := state.MessageByID[f.TypezID]; ok && message.IsMap {
			attr := []string{`#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`}
			var key, value *genclient.Field
			for _, f := range message.Fields {
				switch f.Name {
				case "key":
					key = f
				case "value":
					value = f
				default:
				}
			}
			if key == nil || value == nil {
				slog.Error("missing key or value in map field")
				return attr
			}
			keyFormat := c.fieldFormatter(key)
			valFormat := c.fieldFormatter(value)
			if keyFormat == "_" && valFormat == "_" {
				return attr
			}
			return append(attr, fmt.Sprintf(`#[serde_as(as = "std::collections::HashMap<%s, %s>")]`, keyFormat, valFormat))
		}
		return []string{}

	default:
		slog.Error("unexpected field type", "field", *f)
		return []string{}
	}
}

func (c *Codec) FieldType(f *genclient.Field, state *genclient.APIState) string {
	if f.IsOneOf {
		return c.wrapOneOfField(f, c.baseFieldType(f, state))
	}
	if f.Repeated {
		return fmt.Sprintf("Vec<%s>", c.baseFieldType(f, state))
	}
	if f.Optional {
		return fmt.Sprintf("Option<%s>", c.baseFieldType(f, state))
	}
	return c.baseFieldType(f, state)
}

// Returns the field type, ignoring any repeated or optional attributes.
func (c *Codec) baseFieldType(f *genclient.Field, state *genclient.APIState) string {
	if f.Typez == genclient.MESSAGE_TYPE {
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			key := c.FieldType(m.Fields[0], state)
			val := c.FieldType(m.Fields[1], state)
			return "std::collections::HashMap<" + key + "," + val + ">"
		}
		return c.FQMessageName(m, state)
	} else if f.Typez == genclient.ENUM_TYPE {
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		return c.FQEnumName(e, state)
	} else if f.Typez == genclient.GROUP_TYPE {
		slog.Error("TODO(#39) - better handling of `oneof` fields")
		return ""
	}
	return ScalarFieldType(f)

}

func (c *Codec) wrapOneOfField(f *genclient.Field, value string) string {
	if f.Typez == genclient.MESSAGE_TYPE {
		return fmt.Sprintf("(%s)", value)
	}
	return fmt.Sprintf("{ %s: %s }", c.ToSnake(f.Name), value)
}

func (c *Codec) AsQueryParameter(f *genclient.Field, state *genclient.APIState) string {
	if f.Typez == genclient.MESSAGE_TYPE {
		// Query parameters in nested messages are first converted to a
		// `serde_json::Value`` and then recursively merged into the request
		// query. The conversion to `serde_json::Value` is expensive, but very
		// few requests use nested objects as query parameters. Furthermore,
		// the conversion is skipped if the object field is `None`.`
		return fmt.Sprintf("&serde_json::to_value(&req.%s).map_err(Error::serde)?", c.ToSnake(f.Name))
	}
	return fmt.Sprintf("&req.%s", c.ToSnake(f.Name))
}

func (c *Codec) TemplateDir() string {
	if c.GenerateModule {
		return "rust/mod"
	}
	return "rust/crate"
}

func (c *Codec) MethodInOutTypeName(id string, state *genclient.APIState) string {
	if id == "" {
		return ""
	}
	m, ok := state.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return c.FQMessageName(m, state)
}

func (c *Codec) rustPackage(packageName string) string {
	if packageName == c.SourceSpecificationPackageName {
		return "crate::model"
	}
	mapped, ok := c.PackageMapping[packageName]
	if !ok {
		slog.Error("unknown source package name", "name", packageName)
		return packageName
	}
	// TODO(#158) - maybe google.protobuf should not be this special?
	if packageName == "google.protobuf" {
		return mapped.Name
	}
	return mapped.Name + "::model"
}

func (c *Codec) MessageName(m *genclient.Message, state *genclient.APIState) string {
	return c.ToPascal(m.Name)
}

func (c *Codec) messageScopeName(m *genclient.Message, childPackageName string) string {
	if m == nil {
		return c.rustPackage(childPackageName)
	}
	if m.Parent == nil {
		return c.rustPackage(m.Package) + "::" + c.ToSnake(m.Name)
	}
	return c.messageScopeName(m.Parent, m.Package) + "::" + c.ToSnake(m.Name)
}

func (c *Codec) enumScopeName(e *genclient.Enum) string {
	return c.messageScopeName(e.Parent, "")
}

func (c *Codec) FQMessageName(m *genclient.Message, _ *genclient.APIState) string {
	return c.messageScopeName(m.Parent, m.Package) + "::" + c.ToPascal(m.Name)
}

func (c *Codec) EnumName(e *genclient.Enum, state *genclient.APIState) string {
	return c.ToPascal(e.Name)
}

func (c *Codec) FQEnumName(e *genclient.Enum, _ *genclient.APIState) string {
	return c.messageScopeName(e.Parent, "") + "::" + c.ToPascal(e.Name)
}

func (c *Codec) EnumValueName(e *genclient.EnumValue, _ *genclient.APIState) string {
	// The Protobuf naming convention is to use SCREAMING_SNAKE_CASE, we do not
	// need to change anything for Rust
	return EscapeKeyword(e.Name)
}

func (c *Codec) FQEnumValueName(v *genclient.EnumValue, state *genclient.APIState) string {
	return fmt.Sprintf("%s::%s::%s", c.enumScopeName(v.Parent), c.ToSnake(v.Parent.Name), c.EnumValueName(v, state))
}

func (c *Codec) OneOfType(o *genclient.OneOf, _ *genclient.APIState) string {
	return c.messageScopeName(o.Parent, "") + "::" + c.ToPascal(o.Name)
}

func (c *Codec) BodyAccessor(m *genclient.Method, state *genclient.APIState) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + c.ToSnake(m.PathInfo.BodyFieldPath)
}

func (c *Codec) HTTPPathFmt(m *genclient.PathInfo, state *genclient.APIState) string {
	fmt := ""
	for _, segment := range m.PathTemplate {
		if segment.Literal != nil {
			fmt = fmt + "/" + *segment.Literal
		} else if segment.FieldPath != nil {
			fmt = fmt + "/{}"
		} else if segment.Verb != nil {
			fmt = fmt + ":" + *segment.Verb
		}
	}
	return fmt
}

// Returns a Rust expression to access (and if needed validatre) each path parameter.
//
// In most cases the parameter is a simple string in the form `name`. In those
// cases the field *must* be a thing that can be formatted to a string, and
// a simple "req.name" expression will work file.
//
// In some cases the parameter is a sequence of `.` separated fields, in the
// form: `field0.field1 ... .fieldN`. In that case each field from `field0` to
// `fieldN-1` must be optional (they are all messages), and each must be
// validated.
//
// We use the `gax::path_parameter::PathParameter::required()` helper to perform
// this validation. This function recursively creates an expression, the
// recursion starts with
//
// ```rust
// use gax::path_parameter::PathParameter as PP;
// PP::required(&req.field0)?.field1
// ```
//
// And then builds up:
//
// ```rust
// use gax::path_parameter::PathParameter as PP;
// PP::required(PP::required(&req.field0)?.field1)?.field2
// ```
//
// and so on.
func (c *Codec) unwrapFieldPath(components []string, requestAccess string) (string, string) {
	if len(components) == 1 {
		return requestAccess + "." + c.ToSnake(components[0]), components[0]
	}
	unwrap, name := c.unwrapFieldPath(components[0:len(components)-1], "&req")
	last := components[len(components)-1]
	return fmt.Sprintf("gax::path_parameter::PathParameter::required(%s, \"%s\").map_err(Error::other)?.%s", unwrap, name, last), ""
}

func (c *Codec) derefFieldPath(fieldPath string) string {
	components := strings.Split(fieldPath, ".")
	unwrap, _ := c.unwrapFieldPath(components, "req")
	return unwrap
}

func (c *Codec) HTTPPathArgs(h *genclient.PathInfo, state *genclient.APIState) []string {
	var args []string
	for _, arg := range h.PathTemplate {
		if arg.FieldPath != nil {
			args = append(args, c.derefFieldPath(*arg.FieldPath))
		}
	}
	return args
}

func (c *Codec) QueryParams(m *genclient.Method, state *genclient.APIState) []*genclient.Field {
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup request type", "id", m.InputTypeID)
		return nil
	}

	var queryParams []*genclient.Field
	for _, field := range msg.Fields {
		if !m.PathInfo.QueryParameters[field.Name] {
			continue
		}
		queryParams = append(queryParams, field)
	}
	return queryParams
}

// Convert a name to `snake_case`. The Rust naming conventions use this style
// for modules, fields, and functions.
//
// This type of conversion can easily introduce keywords. Consider
//
//	`ToSnake("True") -> "true"`
func (*Codec) ToSnake(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return EscapeKeyword(symbol)
	}
	return EscapeKeyword(strcase.ToSnake(symbol))
}

// Convert a name to `PascalCase`.  Strangley, the `strcase` package calls this
// `ToCamel` while usually `camelCase` starts with a lowercase letter. The
// Rust naming convensions use this style for structs, enums and traits.
//
// This type of conversion rarely introduces keywords. The one example is
//
//	`ToPascal("self") -> "Self"`
func (*Codec) ToPascal(symbol string) string {
	return EscapeKeyword(strcase.ToCamel(symbol))
}

func (*Codec) ToCamel(symbol string) string {
	return EscapeKeyword(strcase.ToLowerCamel(symbol))
}

func (*Codec) FormatDocComments(documentation string) []string {
	inBlockQuote := false
	ss := strings.Split(documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimRightFunc(ss[i], unicode.IsSpace)
		if strings.HasSuffix(ss[i], "```") {
			if !inBlockQuote {
				ss[i] = ss[i] + "norust"
			}
			inBlockQuote = !inBlockQuote
		}
		// Add the comments here. Otherwise it is harder to ensure empty
		// comments do not have a trailing whitespace.
		if len(ss[i]) > 0 {
			ss[i] = fmt.Sprintf("/// %s", ss[i])
		} else {
			ss[i] = "///"
		}
	}
	return ss
}

func (c *Codec) projectRoot() string {
	if c.OutputDirectory == "" {
		return ""
	}
	rel := ".."
	for range strings.Count(c.OutputDirectory, "/") {
		rel = path.Join(rel, "..")
	}
	return rel
}

func (c *Codec) RequiredPackages() []string {
	lines := []string{}
	for _, pkg := range c.ExtraPackages {
		components := []string{}
		if pkg.Version != "" {
			components = append(components, fmt.Sprintf("version = %q", pkg.Version))
		}
		if pkg.Path != "" {
			components = append(components, fmt.Sprintf("path = %q", path.Join(c.projectRoot(), pkg.Path)))
		}
		if pkg.Package != "" {
			components = append(components, fmt.Sprintf("package = %q", pkg.Package))
		}
		if len(pkg.Features) > 0 {
			feats := strings.Join(mapSlice(pkg.Features, func(s string) string { return fmt.Sprintf("%q", s) }), ", ")
			components = append(components, fmt.Sprintf("features = [%s]", feats))
		}
		lines = append(lines, fmt.Sprintf("%s = { %s }", pkg.Name, strings.Join(components, ", ")))
	}
	sort.Strings(lines)
	return lines
}

func (c *Codec) CopyrightYear() string {
	return c.GenerationYear
}

func (c *Codec) PackageName(api *genclient.API) string {
	if len(c.PackageNameOverride) > 0 {
		return c.PackageNameOverride
	}
	return api.Name
}

func (c *Codec) validatePackageName(newPackage, elementName string) error {
	if c.SourceSpecificationPackageName == "" {
		c.SourceSpecificationPackageName = newPackage
		return nil
	}
	if c.SourceSpecificationPackageName == newPackage {
		return nil
	}
	return fmt.Errorf("rust codec requires all top-level elements to be in the same package want=%s, got=%s for %s",
		c.SourceSpecificationPackageName, newPackage, elementName)
}

func (c *Codec) Validate(api *genclient.API) error {
	// The Rust codec can only generate clients and models for a single protobuf
	// package at a time.
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

// The list of Rust keywords and reserved words can be found at:
//
//	https://doc.rust-lang.org/reference/keywords.html
func EscapeKeyword(symbol string) string {
	keywords := map[string]bool{
		"as":       true,
		"break":    true,
		"const":    true,
		"continue": true,
		"crate":    true,
		"else":     true,
		"enum":     true,
		"extern":   true,
		"false":    true,
		"fn":       true,
		"for":      true,
		"if":       true,
		"impl":     true,
		"in":       true,
		"let":      true,
		"loop":     true,
		"match":    true,
		"mod":      true,
		"move":     true,
		"mut":      true,
		"pub":      true,
		"ref":      true,
		"return":   true,
		"self":     true,
		"Self":     true,
		"static":   true,
		"struct":   true,
		"super":    true,
		"trait":    true,
		"true":     true,
		"type":     true,
		"unsafe":   true,
		"use":      true,
		"where":    true,
		"while":    true,

		// Keywords in Rust 2018+.
		"async": true,
		"await": true,
		"dyn":   true,

		// Reserved
		"abstract": true,
		"become":   true,
		"box":      true,
		"do":       true,
		"final":    true,
		"macro":    true,
		"override": true,
		"priv":     true,
		"typeof":   true,
		"unsized":  true,
		"virtual":  true,
		"yield":    true,

		// Reserved in Rust 2018+
		"try": true,
	}
	_, ok := keywords[symbol]
	if !ok {
		return symbol
	}
	return "r#" + symbol
}

func mapSlice[T, R any](s []T, f func(T) R) []R {
	r := make([]R, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}
