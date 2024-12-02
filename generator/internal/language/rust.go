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
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/iancoleman/strcase"
)

func NewRustCodec(outdir string, options map[string]string) (*RustCodec, error) {
	year, _, _ := time.Now().Date()
	codec := &RustCodec{
		GenerationYear:           fmt.Sprintf("%04d", year),
		OutputDirectory:          outdir,
		ModulePath:               "model",
		DeserializeWithdDefaults: true,
		ExtraPackages:            []*RustPackage{},
		PackageMapping:           map[string]*RustPackage{},
	}
	for key, definition := range options {
		switch key {
		case "package-name-override":
			codec.PackageNameOverride = definition
			continue
		case "generate-module":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `generate-module` value %q to boolean: %w", definition, err)
			}
			codec.GenerateModule = value
			continue
		case "module-path":
			codec.ModulePath = definition
		case "deserialize-with-defaults":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `deserialize-with-defaults` value %q to boolean: %w", definition, err)
			}
			codec.DeserializeWithdDefaults = value
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
				return nil, fmt.Errorf("the definition for package %q should be a comma-separated list of key=value pairs, got=%q", key, definition)
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
			case "ignore":
				value, err := strconv.ParseBool(s[1])
				if err != nil {
					return nil, fmt.Errorf("cannot convert `ignore` value %q (part of %q) to boolean: %w", definition, s[1], err)
				}
				pkg.Ignore = value
			default:
				return nil, fmt.Errorf("unknown field %q in definition of rust package %q, got=%q", s[0], key, definition)
			}
		}
		if !pkg.Ignore && pkg.Package == "" {
			return nil, fmt.Errorf("missing rust package name for package %s, got=%s", key, definition)
		}
		codec.ExtraPackages = append(codec.ExtraPackages, pkg)
		for _, source := range specificationPackages {
			codec.PackageMapping[source] = pkg
		}
	}
	return codec, nil
}

type RustCodec struct {
	// The output directory relative to the project root.
	OutputDirectory string
	// Package name override. If not empty, overrides the default package name.
	PackageNameOverride string
	// The year when the files were first generated.
	GenerationYear string
	// Generate a module of a larger crate, as opposed to a full crate.
	GenerateModule bool
	// The full path of the generated module within the crate. This defaults to
	// `model`. When generating only a module within a larger crate (see
	// `GenerateModule`), this overrides the path for elements within the crate.
	// Note that using `self` does not work, as the generated code may contain
	// nested modules for nested messages.
	ModulePath string
	// If true, the deserialization functions will accept default values in
	// messages. In almost all cases this should be `true`, but
	DeserializeWithdDefaults bool
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
	// If true, ignore the package. We anticipate that the top-level
	// `.sidekick.toml` file will have a number of pre-configured dependencies,
	// but these will be ignored by a handful of packages.
	Ignore bool
	// What the Rust package calls itself.
	Package string
	// The path to file the package locally, unused if empty.
	Path string
	// The version of the package, unused if empty.
	Version string
	// Optional features enabled for the package.
	Features []string
}

func (c *RustCodec) LoadWellKnownTypes(s *api.APIState) {
	// TODO(#77) - replace these placeholders with real types
	wellKnown := []*api.Message{
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

func ScalarFieldType(f *api.Field) string {
	var out string
	switch f.Typez {
	case api.DOUBLE_TYPE:
		out = "f64"
	case api.FLOAT_TYPE:
		out = "f32"
	case api.INT64_TYPE:
		out = "i64"
	case api.UINT64_TYPE:
		out = "u64"
	case api.INT32_TYPE:
		out = "i32"
	case api.FIXED64_TYPE:
		out = "u64"
	case api.FIXED32_TYPE:
		out = "u32"
	case api.BOOL_TYPE:
		out = "bool"
	case api.STRING_TYPE:
		out = "String"
	case api.BYTES_TYPE:
		out = "bytes::Bytes"
	case api.UINT32_TYPE:
		out = "u32"
	case api.SFIXED32_TYPE:
		out = "i32"
	case api.SFIXED64_TYPE:
		out = "i64"
	case api.SINT32_TYPE:
		out = "i32"
	case api.SINT64_TYPE:
		out = "i64"

	default:
		slog.Error("Unexpected field type", "field", *f)
		return ""
	}
	return out
}

func (c *RustCodec) fieldFormatter(typez api.Typez) string {
	switch typez {
	case api.INT64_TYPE,
		api.UINT64_TYPE,
		api.FIXED64_TYPE,
		api.SFIXED64_TYPE,
		api.SINT64_TYPE:
		return "serde_with::DisplayFromStr"
	case api.BYTES_TYPE:
		return "serde_with::base64::Base64"
	default:
		return "_"
	}
}

func (c *RustCodec) fieldBaseAttributes(f *api.Field) []string {
	if f.Synthetic {
		return []string{`#[serde(skip)]`}
	}
	if c.ToCamel(c.ToSnake(f.Name)) != f.JSONName {
		return []string{fmt.Sprintf(`#[serde(rename = "%s")]`, f.JSONName)}
	}
	return []string{}
}

func (c *RustCodec) wrapperFieldAttributes(f *api.Field, defaultAttributes []string) []string {
	var formatter string
	switch f.TypezID {
	case ".google.protobuf.BytesValue":
		formatter = c.fieldFormatter(api.BYTES_TYPE)
	case ".google.protobuf.UInt64Value":
		formatter = c.fieldFormatter(api.UINT64_TYPE)
	case ".google.protobuf.Int64Value":
		formatter = c.fieldFormatter(api.INT64_TYPE)
	default:
		return defaultAttributes
	}
	return []string{fmt.Sprintf(`#[serde_as(as = "Option<%s>")]`, formatter)}
}

func (c *RustCodec) FieldAttributes(f *api.Field, state *api.APIState) []string {
	attributes := c.fieldBaseAttributes(f)
	switch f.Typez {
	case api.DOUBLE_TYPE,
		api.FLOAT_TYPE,
		api.INT32_TYPE,
		api.FIXED32_TYPE,
		api.BOOL_TYPE,
		api.STRING_TYPE,
		api.UINT32_TYPE,
		api.SFIXED32_TYPE,
		api.SINT32_TYPE,
		api.ENUM_TYPE,
		api.GROUP_TYPE:
		return attributes

	case api.INT64_TYPE,
		api.UINT64_TYPE,
		api.FIXED64_TYPE,
		api.SFIXED64_TYPE,
		api.SINT64_TYPE,
		api.BYTES_TYPE:
		formatter := c.fieldFormatter(f.Typez)
		if f.Optional {
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "Option<%s>")]`, formatter))
		}
		if f.Repeated {
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "Vec<%s>")]`, formatter))
		}
		return append(attributes, fmt.Sprintf(`#[serde_as(as = "%s")]`, formatter))

	case api.MESSAGE_TYPE:
		if message, ok := state.MessageByID[f.TypezID]; ok && message.IsMap {
			attributes = append(attributes, `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`)
			var key, value *api.Field
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
				return attributes
			}
			keyFormat := c.fieldFormatter(key.Typez)
			valFormat := c.fieldFormatter(value.Typez)
			if keyFormat == "_" && valFormat == "_" {
				return attributes
			}
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "std::collections::HashMap<%s, %s>")]`, keyFormat, valFormat))
		}
		return c.wrapperFieldAttributes(f, attributes)

	default:
		slog.Error("unexpected field type", "field", *f)
		return attributes
	}
}

func (c *RustCodec) FieldType(f *api.Field, state *api.APIState) string {
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
func (c *RustCodec) baseFieldType(f *api.Field, state *api.APIState) string {
	if f.Typez == api.MESSAGE_TYPE {
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
	} else if f.Typez == api.ENUM_TYPE {
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		return c.FQEnumName(e, state)
	} else if f.Typez == api.GROUP_TYPE {
		slog.Error("TODO(#39) - better handling of `oneof` fields")
		return ""
	}
	return ScalarFieldType(f)

}

func (c *RustCodec) wrapOneOfField(f *api.Field, value string) string {
	if f.Typez == api.MESSAGE_TYPE {
		return fmt.Sprintf("(%s)", value)
	}
	return fmt.Sprintf("{ %s: %s }", c.ToSnake(f.Name), value)
}

func (c *RustCodec) AsQueryParameter(f *api.Field, state *api.APIState) string {
	if f.Typez == api.MESSAGE_TYPE {
		// Query parameters in nested messages are first converted to a
		// `serde_json::Value`` and then recursively merged into the request
		// query. The conversion to `serde_json::Value` is expensive, but very
		// few requests use nested objects as query parameters. Furthermore,
		// the conversion is skipped if the object field is `None`.`
		return fmt.Sprintf("&serde_json::to_value(&req.%s).map_err(Error::serde)?", c.ToSnake(f.Name))
	}
	return fmt.Sprintf("&req.%s", c.ToSnake(f.Name))
}

func (c *RustCodec) TemplateDir() string {
	if c.GenerateModule {
		return "rust/mod"
	}
	return "rust/crate"
}

func (c *RustCodec) MethodInOutTypeName(id string, state *api.APIState) string {
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

func (c *RustCodec) rustPackage(packageName string) string {
	if packageName == c.SourceSpecificationPackageName {
		return "crate::" + c.ModulePath
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

func (c *RustCodec) MessageAttributes(*api.Message, *api.APIState) []string {
	serde := `#[serde(default, rename_all = "camelCase")]`
	if !c.DeserializeWithdDefaults {
		serde = `#[serde(rename_all = "camelCase")]`
	}
	return []string{
		`#[serde_with::serde_as]`,
		`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
		serde,
		`#[non_exhaustive]`,
	}
}

func (c *RustCodec) MessageName(m *api.Message, state *api.APIState) string {
	return c.ToPascal(m.Name)
}

func (c *RustCodec) messageScopeName(m *api.Message, childPackageName string) string {
	if m == nil {
		return c.rustPackage(childPackageName)
	}
	if m.Parent == nil {
		return c.rustPackage(m.Package) + "::" + c.ToSnake(m.Name)
	}
	return c.messageScopeName(m.Parent, m.Package) + "::" + c.ToSnake(m.Name)
}

func (c *RustCodec) enumScopeName(e *api.Enum) string {
	return c.messageScopeName(e.Parent, "")
}

func (c *RustCodec) FQMessageName(m *api.Message, _ *api.APIState) string {
	return c.messageScopeName(m.Parent, m.Package) + "::" + c.ToPascal(m.Name)
}

func (c *RustCodec) EnumName(e *api.Enum, state *api.APIState) string {
	return c.ToPascal(e.Name)
}

func (c *RustCodec) FQEnumName(e *api.Enum, _ *api.APIState) string {
	return c.messageScopeName(e.Parent, "") + "::" + c.ToPascal(e.Name)
}

func (c *RustCodec) EnumValueName(e *api.EnumValue, _ *api.APIState) string {
	// The Protobuf naming convention is to use SCREAMING_SNAKE_CASE, we do not
	// need to change anything for Rust
	return rustEscapeKeyword(e.Name)
}

func (c *RustCodec) FQEnumValueName(v *api.EnumValue, state *api.APIState) string {
	return fmt.Sprintf("%s::%s::%s", c.enumScopeName(v.Parent), c.ToSnake(v.Parent.Name), c.EnumValueName(v, state))
}

func (c *RustCodec) OneOfType(o *api.OneOf, _ *api.APIState) string {
	return c.messageScopeName(o.Parent, "") + "::" + c.ToPascal(o.Name)
}

func (c *RustCodec) BodyAccessor(m *api.Method, state *api.APIState) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + c.ToSnake(m.PathInfo.BodyFieldPath)
}

func (c *RustCodec) HTTPPathFmt(m *api.PathInfo, state *api.APIState) string {
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
func (c *RustCodec) unwrapFieldPath(components []string, requestAccess string) (string, string) {
	if len(components) == 1 {
		return requestAccess + "." + c.ToSnake(components[0]), components[0]
	}
	unwrap, name := c.unwrapFieldPath(components[0:len(components)-1], "&req")
	last := components[len(components)-1]
	return fmt.Sprintf("gax::path_parameter::PathParameter::required(%s, \"%s\").map_err(Error::other)?.%s", unwrap, name, last), ""
}

func (c *RustCodec) derefFieldPath(fieldPath string) string {
	components := strings.Split(fieldPath, ".")
	unwrap, _ := c.unwrapFieldPath(components, "req")
	return unwrap
}

func (c *RustCodec) HTTPPathArgs(h *api.PathInfo, state *api.APIState) []string {
	var args []string
	for _, arg := range h.PathTemplate {
		if arg.FieldPath != nil {
			args = append(args, c.derefFieldPath(*arg.FieldPath))
		}
	}
	return args
}

func (c *RustCodec) QueryParams(m *api.Method, state *api.APIState) []*api.Field {
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup request type", "id", m.InputTypeID)
		return nil
	}

	var queryParams []*api.Field
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
func (c *RustCodec) ToSnake(symbol string) string {
	return rustEscapeKeyword(c.ToSnakeNoMangling(symbol))
}

func (*RustCodec) ToSnakeNoMangling(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return symbol
	}
	return strcase.ToSnake(symbol)
}

// Convert a name to `PascalCase`.  Strangley, the `strcase` package calls this
// `ToCamel` while usually `camelCase` starts with a lowercase letter. The
// Rust naming convensions use this style for structs, enums and traits.
//
// This type of conversion rarely introduces keywords. The one example is
//
//	`ToPascal("self") -> "Self"`
func (*RustCodec) ToPascal(symbol string) string {
	return rustEscapeKeyword(strcase.ToCamel(symbol))
}

func (*RustCodec) ToCamel(symbol string) string {
	return rustEscapeKeyword(strcase.ToLowerCamel(symbol))
}

// TODO(#92) - protect all quotes with `norust`
// TODO(#30) - convert protobuf links to Rusty links.
//
// Blockquotes require special treatment for Rust. By default, Rust assumes
// blockquotes contain compilable Rust code samples. To override the default
// the blockquote must be marked with "```norust". The proto comments have
// many blockquotes that do not follow this convention (nor should they).
//
// This function handles some easy cases of blockquotes, but a full treatment
// requires parsing of the comments. The CommonMark [spec] includes some
// difficult examples.
//
// [spec]: https://spec.commonmark.org/0.13/#block-quotes
func (*RustCodec) FormatDocComments(documentation string) []string {
	inBlockquote := false
	blockquotePrefix := ""

	var results []string
	for _, line := range strings.Split(documentation, "\n") {
		if inBlockquote {
			switch {
			case line == "```":
				inBlockquote = false
				results = append(results, "```")
			case strings.HasPrefix(line, blockquotePrefix):
				results = append(results, strings.TrimPrefix(line, blockquotePrefix))
			default:
				inBlockquote = false
				results = append(results, "```")
				results = append(results, line)
			}
		} else {
			switch {
			case line == "```":
				results = append(results, "```norust")
				inBlockquote = true
			case strings.HasPrefix(line, "    "):
				inBlockquote = true
				blockquotePrefix = "    "
				results = append(results, "```norust")
				results = append(results, strings.TrimPrefix(line, blockquotePrefix))
			case strings.HasPrefix(line, "   > "):
				inBlockquote = true
				blockquotePrefix = "   > "
				results = append(results, "```norust")
				results = append(results, strings.TrimPrefix(line, blockquotePrefix))
			case strings.HasPrefix(line, "> "):
				inBlockquote = true
				blockquotePrefix = "> "
				results = append(results, "```norust")
				results = append(results, strings.TrimPrefix(line, blockquotePrefix))
			default:
				results = append(results, line)
			}
		}
	}
	if inBlockquote {
		results = append(results, "```")
	}
	for i, line := range results {
		results[i] = strings.TrimRightFunc(fmt.Sprintf("/// %s", line), unicode.IsSpace)
	}
	return results
}

func (c *RustCodec) projectRoot() string {
	if c.OutputDirectory == "" {
		return ""
	}
	rel := ".."
	for range strings.Count(c.OutputDirectory, "/") {
		rel = path.Join(rel, "..")
	}
	return rel
}

func (c *RustCodec) RequiredPackages() []string {
	lines := []string{}
	for _, pkg := range c.ExtraPackages {
		if pkg.Ignore {
			continue
		}
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
		lines = append(lines, fmt.Sprintf("%-10s = { %s }", pkg.Name, strings.Join(components, ", ")))
	}
	sort.Strings(lines)
	return lines
}

func (c *RustCodec) CopyrightYear() string {
	return c.GenerationYear
}

func (c *RustCodec) PackageName(api *api.API) string {
	if len(c.PackageNameOverride) > 0 {
		return c.PackageNameOverride
	}
	name := strings.TrimPrefix(api.PackageName, "google.cloud.")
	name = strings.TrimPrefix(name, "google.")
	name = strings.ReplaceAll(name, ".", "-")
	if name == "" {
		name = api.Name
	}
	return "gcp-sdk-" + name
}

func (c *RustCodec) validatePackageName(newPackage, elementName string) error {
	if c.SourceSpecificationPackageName == newPackage {
		return nil
	}
	// Special exceptions for mixin services
	if newPackage == "google.cloud.location" ||
		newPackage == "google.iam.v1" ||
		newPackage == "google.longrunning" {
		return nil
	}
	return fmt.Errorf("rust codec requires all top-level elements to be in the same package want=%s, got=%s for %s",
		c.SourceSpecificationPackageName, newPackage, elementName)
}

func (c *RustCodec) Validate(api *api.API) error {
	// Set the source package. We should always take the first service registered
	// as the source package. Services with mixins will register those after the
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

func (c *RustCodec) AdditionalContext() any {
	return nil
}

func (c *RustCodec) Imports() []string {
	return nil
}

// The list of Rust keywords and reserved words can be found at:
//
//	https://doc.rust-lang.org/reference/keywords.html
func rustEscapeKeyword(symbol string) string {
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
