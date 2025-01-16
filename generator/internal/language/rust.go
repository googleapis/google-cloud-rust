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
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/iancoleman/strcase"
)

//go:embed templates/rust
var rustTemplates embed.FS

// A regular expression to find code links in comments.
//
// The Google API documentation (typically in protos) include links to code
// elements in the form `[Thing][google.package.blah.v1.Thing.SubThing]`.
// This regular expression captures the `][...]` part. There is a lot of scaping
// because the brackets are metacharacters in regex.
var commentLinkRegex = regexp.MustCompile(
	`` + // `go fmt` is annoying
		`\]` + // The closing bracket for the `[Thing]`
		`\[` + // The opening bracket for the code element.
		`[a-z_]+` + // Must start with an all lowercase name like `google` or `grafeas`.
		`\.` + // Separated by a dot
		`[a-zA-Z0-9_\.]+` + // We don't try to parse these with a regex, alphanum, underscores and dots are all accepted in any order
		`\]`) // The closing bracket

// A regular expression to find https links in comments.
//
// The Google API documentation (typically in protos) includes some raw HTTP[S]
// links. While many markdown implementations autolink, Rustdoc does not. It
// expects the writer to use these:
//
// https://www.markdownguide.org/basic-syntax#urls-and-email-addresses
//
// Furthermore, rustdoc warns if you have something that looks like an autolink.
// We convert raw links because raw links are too common in the documentation.
var commentUrlRegex = regexp.MustCompile(
	`` + // `go fmt` is annoying
		`https?://` + // Accept either https or http.
		`([A-Za-z0-9\.]+\.)+` + // Be generous in accepting most of the authority (hostname)
		`[a-zA-Z]{2,63}` + // The root domain is far more strict
		`([-a-zA-Z0-9@:%_\+.~#?&/=]+)?`) // Accept just about anything on the query and URL fragments

func newRustCodec(options map[string]string) (*rustCodec, error) {
	year, _, _ := time.Now().Date()
	codec := &rustCodec{
		generationYear:           fmt.Sprintf("%04d", year),
		modulePath:               "model",
		deserializeWithdDefaults: true,
		extraPackages:            []*rustPackage{},
		packageMapping:           map[string]*rustPackage{},
		version:                  "0.0.0",
	}

	for key, definition := range options {
		switch {
		case key == "package-name-override":
			codec.packageNameOverride = definition
		case key == "generate-module":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `generate-module` value %q to boolean: %w", definition, err)
			}
			codec.generateModule = value
		case key == "module-path":
			codec.modulePath = definition
		case key == "deserialize-with-defaults":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `deserialize-with-defaults` value %q to boolean: %w", definition, err)
			}
			codec.deserializeWithdDefaults = value
		case key == "copyright-year":
			codec.generationYear = definition
		case key == "not-for-publication":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `not-for-publication` value %q to boolean: %w", definition, err)
			}
			codec.doNotPublish = value
		case key == "version":
			codec.version = definition
		case strings.HasPrefix(key, "package:"):
			pkgOption, err := parseRustPackageOption(key, definition)
			if err != nil {
				return nil, err
			}
			codec.extraPackages = append(codec.extraPackages, pkgOption.pkg)
			for _, source := range pkgOption.otherNames {
				codec.packageMapping[source] = pkgOption.pkg
			}
		default:
			return nil, fmt.Errorf("unknown Rust codec option %q", key)
		}
	}
	return codec, nil
}

type rustPackageOption struct {
	pkg        *rustPackage
	otherNames []string
}

func parseRustPackageOption(key, definition string) (*rustPackageOption, error) {
	var specificationPackages []string
	pkg := &rustPackage{
		name:            strings.TrimPrefix(key, "package:"),
		defaultFeatures: true,
	}
	for _, element := range strings.Split(definition, ",") {
		s := strings.SplitN(element, "=", 2)
		if len(s) != 2 {
			return nil, fmt.Errorf("the definition for package %q should be a comma-separated list of key=value pairs, got=%q", key, definition)
		}
		switch s[0] {
		case "package":
			pkg.packageName = s[1]
		case "path":
			pkg.path = s[1]
		case "version":
			pkg.version = s[1]
		case "source":
			specificationPackages = append(specificationPackages, s[1])
		case "feature":
			pkg.features = append(pkg.features, strings.Split(s[1], ",")...)
		case "default-features":
			value, err := strconv.ParseBool(s[1])
			if err != nil {
				return nil, fmt.Errorf("cannot convert `default-features` value %q (part of %q) to boolean: %w", definition, s[1], err)
			}
			pkg.defaultFeatures = value
		case "ignore":
			value, err := strconv.ParseBool(s[1])
			if err != nil {
				return nil, fmt.Errorf("cannot convert `ignore` value %q (part of %q) to boolean: %w", definition, s[1], err)
			}
			pkg.ignore = value
		case "force-used":
			value, err := strconv.ParseBool(s[1])
			if err != nil {
				return nil, fmt.Errorf("cannot convert `force-used` value %q (part of %q) to boolean: %w", definition, s[1], err)
			}
			pkg.used = value
		case "used-if":
			pkg.usedIf = append(pkg.usedIf, s[1])
		default:
			return nil, fmt.Errorf("unknown field %q in definition of rust package %q, got=%q", s[0], key, definition)
		}
	}
	if !pkg.ignore && pkg.packageName == "" {
		return nil, fmt.Errorf("missing rust package name for package %s, got=%s", key, definition)
	}
	return &rustPackageOption{pkg: pkg, otherNames: specificationPackages}, nil
}

type rustCodec struct {
	// Package name override. If not empty, overrides the default package name.
	packageNameOverride string
	// The year when the files were first generated.
	generationYear string
	// Generate a module of a larger crate, as opposed to a full crate.
	generateModule bool
	// The full path of the generated module within the crate. This defaults to
	// `model`. When generating only a module within a larger crate (see
	// `GenerateModule`), this overrides the path for elements within the crate.
	// Note that using `self` does not work, as the generated code may contain
	// nested modules for nested messages.
	modulePath string
	// If true, the deserialization functions will accept default values in
	// messages. In almost all cases this should be `true`, but
	deserializeWithdDefaults bool
	// Additional Rust packages imported by this module. The Mustache template
	// hardcodes a number of packages, but some are configured via the
	// command-line.
	extraPackages []*rustPackage
	// A mapping between the specification package names (typically Protobuf),
	// and the Rust package name that contains these types.
	packageMapping map[string]*rustPackage
	// The source package name (e.g. google.iam.v1 in Protobuf). The codec can
	// generate code for one source package at a time.
	sourceSpecificationPackageName string
	// Some packages are not intended for publication. For example, they may be
	// intended only for testing the generator or the SDK, or the service may
	// not be GA.
	doNotPublish bool
	// The version of the generated crate.
	version string
	// True if the API model includes any services
	hasServices bool
}

type rustPackage struct {
	// The name we import this package under.
	name string
	// If true, ignore the package. We anticipate that the top-level
	// `.sidekick.toml` file will have a number of pre-configured dependencies,
	// but these will be ignored by a handful of packages.
	ignore bool
	// What the Rust package calls itself.
	packageName string
	// The path to file the package locally, unused if empty.
	path string
	// The version of the package, unused if empty.
	version string
	// Optional features enabled for the package.
	features []string
	// If true, this package was referenced by a generated message, service, or
	// by the documentation.
	used bool
	// Some packages are used if a particular feature or named pattern is
	// present. For example, the LRO support helpers are used if LROs are found,
	// and the service support functions are used if any service is found.
	usedIf []string
	// If true, the default features are enabled.
	defaultFeatures bool
}

var rustWellKnownMessages = []*api.Message{
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

func rustLoadWellKnownTypes(s *api.APIState) {
	for _, message := range rustWellKnownMessages {
		s.MessageByID[message.ID] = message
	}
}

func rustResolveUsedPackages(model *api.API, extraPackages []*rustPackage) {
	hasServices := len(model.State.ServiceByID) > 0
	hasLROs := false
	for _, s := range model.Services {
		if hasLROs {
			break
		}
		for _, m := range s.Methods {
			if m.OperationInfo != nil {
				hasLROs = true
				break
			}
		}
	}
	for _, pkg := range extraPackages {
		if pkg.used {
			continue
		}
		for _, namedFeature := range pkg.usedIf {
			if namedFeature == "services" && hasServices {
				pkg.used = true
				break
			}
			if namedFeature == "lro" && hasLROs {
				pkg.used = true
				break
			}
		}
	}
}

func scalarFieldType(f *api.Field) string {
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

func rustFieldFormatter(typez api.Typez) string {
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

func rustFieldSkipAttributes(f *api.Field) []string {
	switch f.Typez {
	case api.STRING_TYPE:
		return []string{`#[serde(skip_serializing_if = "String::is_empty")]`}
	case api.BYTES_TYPE:
		return []string{`#[serde(skip_serializing_if = "bytes::Bytes::is_empty")]`}
	default:
		return []string{}
	}
}

func rustFieldBaseAttributes(f *api.Field) []string {
	if rustToCamel(rustToSnake(f.Name)) != f.JSONName {
		return []string{fmt.Sprintf(`#[serde(rename = "%s")]`, f.JSONName)}
	}
	return []string{}
}

func rustWrapperFieldAttributes(f *api.Field, attributes []string) []string {
	// Message fields could be `Vec<..>`, and are always optional:
	if f.Optional {
		attributes = append(attributes, `#[serde(skip_serializing_if = "Option::is_none")]`)
	}
	if f.Repeated {
		attributes = append(attributes, `#[serde(skip_serializing_if = "Vec::is_empty")]`)
	}
	var formatter string
	switch f.TypezID {
	case ".google.protobuf.BytesValue":
		formatter = rustFieldFormatter(api.BYTES_TYPE)
	case ".google.protobuf.UInt64Value":
		formatter = rustFieldFormatter(api.UINT64_TYPE)
	case ".google.protobuf.Int64Value":
		formatter = rustFieldFormatter(api.INT64_TYPE)
	default:
		return attributes
	}
	// A few message types require ad-hoc treatment. Most are just managed with
	// the default handler.
	return append(
		attributes,
		fmt.Sprintf(`#[serde_as(as = "Option<%s>")]`, formatter))
}

func rustFieldAttributes(f *api.Field, state *api.APIState) []string {
	if f.Synthetic {
		return []string{`#[serde(skip)]`}
	}
	attributes := rustFieldBaseAttributes(f)
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
		if f.Optional {
			return append(attributes, `#[serde(skip_serializing_if = "Option::is_none")]`)
		}
		if f.Repeated {
			return append(attributes, `#[serde(skip_serializing_if = "Vec::is_empty")]`)
		}
		return append(attributes, rustFieldSkipAttributes(f)...)

	case api.INT64_TYPE,
		api.UINT64_TYPE,
		api.FIXED64_TYPE,
		api.SFIXED64_TYPE,
		api.SINT64_TYPE,
		api.BYTES_TYPE:
		formatter := rustFieldFormatter(f.Typez)
		if f.Optional {
			attributes = append(attributes, `#[serde(skip_serializing_if = "Option::is_none")]`)
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "Option<%s>")]`, formatter))
		}
		if f.Repeated {
			attributes = append(attributes, `#[serde(skip_serializing_if = "Vec::is_empty")]`)
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "Vec<%s>")]`, formatter))
		}
		attributes = append(attributes, rustFieldSkipAttributes(f)...)
		return append(attributes, fmt.Sprintf(`#[serde_as(as = "%s")]`, formatter))

	case api.MESSAGE_TYPE:
		if message, ok := state.MessageByID[f.TypezID]; ok && message.IsMap {
			// map<> field types require special treatment.
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
			keyFormat := rustFieldFormatter(key.Typez)
			valFormat := rustFieldFormatter(value.Typez)
			if keyFormat == "_" && valFormat == "_" {
				return attributes
			}
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "std::collections::HashMap<%s, %s>")]`, keyFormat, valFormat))
		}
		return rustWrapperFieldAttributes(f, attributes)

	default:
		slog.Error("unexpected field type", "field", *f)
		return attributes
	}
}

func rustFieldType(f *api.Field, state *api.APIState, primitive bool, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	if !primitive && f.IsOneOf {
		return fmt.Sprintf("(%s)", rustBaseFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping))
	}
	if !primitive && f.Repeated {
		return fmt.Sprintf("Vec<%s>", rustBaseFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping))
	}
	if !primitive && f.Optional {
		return fmt.Sprintf("Option<%s>", rustBaseFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping))
	}
	return rustBaseFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping)
}

// Returns the field type, ignoring any repeated or optional attributes.
func rustBaseFieldType(f *api.Field, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	if f.Typez == api.MESSAGE_TYPE {
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			key := rustFieldType(m.Fields[0], state, false, modulePath, sourceSpecificationPackageName, packageMapping)
			val := rustFieldType(m.Fields[1], state, false, modulePath, sourceSpecificationPackageName, packageMapping)
			return "std::collections::HashMap<" + key + "," + val + ">"
		}
		return rustFQMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping)
	} else if f.Typez == api.ENUM_TYPE {
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		return rustFQEnumName(e, modulePath, sourceSpecificationPackageName, packageMapping)
	} else if f.Typez == api.GROUP_TYPE {
		slog.Error("TODO(#39) - better handling of `oneof` fields")
		return ""
	}
	return scalarFieldType(f)

}

func rustAsQueryParameter(f *api.Field) string {
	if f.Typez == api.MESSAGE_TYPE {
		// Query parameters in nested messages are first converted to a
		// `serde_json::Value`` and then recursively merged into the request
		// query. The conversion to `serde_json::Value` is expensive, but very
		// few requests use nested objects as query parameters. Furthermore,
		// the conversion is skipped if the object field is `None`.`
		return fmt.Sprintf("&serde_json::to_value(&req.%s).map_err(Error::serde)?", rustToSnake(f.Name))
	}
	return fmt.Sprintf("&req.%s", rustToSnake(f.Name))
}

func rustTemplatesProvider() templateProvider {
	return func(name string) (string, error) {
		contents, err := rustTemplates.ReadFile(name)
		if err != nil {
			return "", err
		}
		return string(contents), nil
	}
}

func rustGeneratedFiles(generateModule, hasServices bool) []GeneratedFile {
	var root string
	switch {
	case generateModule:
		root = "templates/rust/mod"
	case !hasServices:
		root = "templates/rust/nosvc"
	default:
		root = "templates/rust/crate"
	}
	return walkTemplatesDir(rustTemplates, root)
}

func rustMethodInOutTypeName(id string, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	if id == "" {
		return ""
	}
	m, ok := state.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return rustFQMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping)
}

func rustMessageAttributes(deserializeWithdDefaults bool) []string {
	serde := `#[serde(default, rename_all = "camelCase")]`
	if !deserializeWithdDefaults {
		serde = `#[serde(rename_all = "camelCase")]`
	}
	return []string{
		`#[serde_with::serde_as]`,
		`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
		serde,
		`#[non_exhaustive]`,
	}
}

func rustMessageName(m *api.Message) string {
	return rustToPascal(m.Name)
}

func rustMessageScopeName(m *api.Message, childPackageName, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	rustPkg := func(packageName string) string {
		if packageName == sourceSpecificationPackageName {
			return "crate::" + modulePath
		}
		mapped, ok := rustMapPackage(packageName, packageMapping)
		if !ok {
			return packageName
		}
		// TODO(#158) - maybe google.protobuf should not be this special?
		if packageName == "google.protobuf" {
			return mapped.name
		}
		return mapped.name + "::model"
	}

	if m == nil {
		return rustPkg(childPackageName)
	}
	if m.Parent == nil {
		return rustPkg(m.Package) + "::" + rustToSnake(m.Name)
	}
	return rustMessageScopeName(m.Parent, m.Package, modulePath, sourceSpecificationPackageName, packageMapping) + "::" + rustToSnake(m.Name)
}

func rustEnumScopeName(e *api.Enum, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	return rustMessageScopeName(e.Parent, e.Package, modulePath, sourceSpecificationPackageName, packageMapping)
}

func rustFQMessageName(m *api.Message, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	return rustMessageScopeName(m.Parent, m.Package, modulePath, sourceSpecificationPackageName, packageMapping) + "::" + rustToPascal(m.Name)
}

func rustEnumName(e *api.Enum) string {
	return rustToPascal(e.Name)
}

func rustFQEnumName(e *api.Enum, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	return rustMessageScopeName(e.Parent, e.Package, modulePath, sourceSpecificationPackageName, packageMapping) + "::" + rustToPascal(e.Name)
}

func rustEnumValueName(e *api.EnumValue) string {
	// The Protobuf naming convention is to use SCREAMING_SNAKE_CASE, we do not
	// need to change anything for Rust
	return rustEscapeKeyword(e.Name)
}

func rustFQEnumValueName(v *api.EnumValue, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	return fmt.Sprintf("%s::%s::%s", rustEnumScopeName(v.Parent, modulePath, sourceSpecificationPackageName, packageMapping), rustToSnake(v.Parent.Name), rustEnumValueName(v))
}

func rustOneOfType(o *api.OneOf, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	return rustMessageScopeName(o.Parent, "", modulePath, sourceSpecificationPackageName, packageMapping) + "::" + rustToPascal(o.Name)
}

func rustBodyAccessor(m *api.Method) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + rustToSnake(m.PathInfo.BodyFieldPath)
}

func rustHTTPPathFmt(m *api.PathInfo) string {
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
func rustUnwrapFieldPath(components []string, requestAccess string) (string, string) {
	if len(components) == 1 {
		return requestAccess + "." + rustToSnake(components[0]), components[0]
	}
	unwrap, name := rustUnwrapFieldPath(components[0:len(components)-1], "&req")
	last := components[len(components)-1]
	return fmt.Sprintf("gax::path_parameter::PathParameter::required(%s, \"%s\").map_err(Error::other)?.%s", unwrap, name, last), ""
}

func rustDerefFieldPath(fieldPath string) string {
	components := strings.Split(fieldPath, ".")
	unwrap, _ := rustUnwrapFieldPath(components, "req")
	return unwrap
}

func rustHTTPPathArgs(h *api.PathInfo) []string {
	var args []string
	for _, arg := range h.PathTemplate {
		if arg.FieldPath != nil {
			args = append(args, rustDerefFieldPath(*arg.FieldPath))
		}
	}
	return args
}

// Convert a name to `snake_case`. The Rust naming conventions use this style
// for modules, fields, and functions.
//
// This type of conversion can easily introduce keywords. Consider
//
//	`toSnake("True") -> "true"`
func rustToSnake(symbol string) string {
	return rustEscapeKeyword(rustToSnakeNoMangling(symbol))
}

func rustToSnakeNoMangling(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return symbol
	}
	return strcase.ToSnake(symbol)
}

// Convert a name to `PascalCase`.  Strangley, the `strcase` package calls this
// `ToCamel` while usually `camelCase` starts with a lowercase letter. The
// Rust naming conventions use this style for structs, enums and traits.
//
// This type of conversion rarely introduces keywords. The one example is
//
//	`toPascal("self") -> "Self"`
func rustToPascal(symbol string) string {
	if symbol == "" {
		return ""
	}
	runes := []rune(symbol)
	if unicode.IsUpper(runes[0]) && !strings.ContainsRune(symbol, '_') {
		return rustEscapeKeyword(symbol)
	}
	return rustEscapeKeyword(strcase.ToCamel(symbol))
}

func rustToCamel(symbol string) string {
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
func rustFormatDocComments(documentation string, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) []string {
	inBlockquote := false
	blockquotePrefix := ""

	links := map[string]bool{}
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
			for _, match := range commentLinkRegex.FindAllString(line, -1) {
				match = strings.TrimSuffix(strings.TrimPrefix(match, "]["), "]")
				links[match] = true
			}
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
				var sb strings.Builder
				lastMatch := 0
				for _, pair := range commentUrlRegex.FindAllStringIndex(line, -1) {
					sb.WriteString(line[lastMatch:pair[0]])
					lastMatch = pair[1]
					prior := ""
					if pair[0] != 0 {
						prior = line[pair[0]-1 : pair[0]]
					}
					next := ""
					if pair[1] != len(line) {
						next = line[pair[1] : pair[1]+1]
					}
					match := line[pair[0]:pair[1]]
					switch {
					case strings.HasSuffix(line[0:pair[0]], "]: "):
						// Looks like a markdown link definition [1], no
						// replacement needed.
						// [1]: https://spec.commonmark.org/0.31.2/#link-reference-definitions
						sb.WriteString(match)
					case strings.HasSuffix(line[0:pair[0]], "](") && next == ")":
						// This looks like a link destination [1], no
						// replacement needed.
						// [1]: https://spec.commonmark.org/0.31.2/#links
						sb.WriteString(match)
					case prior == "<" && next == ">":
						// URLs already surrounded by `<...>` need no replacement
						sb.WriteString(match)
					case strings.HasSuffix(match, ".") && pair[1] == len(line):
						// Many comments end with a URL and then a period. In
						// most cases (all cases I could find), the period is punctuation,
						// and not part of the URL.
						sb.WriteString(fmt.Sprintf("<%s>.", strings.TrimSuffix(match, ".")))
					default:
						sb.WriteString(fmt.Sprintf("<%s>", match))
					}
				}
				sb.WriteString(line[lastMatch:])
				line = sb.String()
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
	if len(links) != 0 {
		results = append(results, "///")
	}
	// Sort the links to get stable results.
	var sortedLinks []string
	for link := range links {
		sortedLinks = append(sortedLinks, link)
	}
	sort.Strings(sortedLinks)
	for _, link := range sortedLinks {
		rusty := rustDocLink(link, state, modulePath, sourceSpecificationPackageName, packageMapping)
		if rusty == "" {
			continue
		}
		results = append(results, fmt.Sprintf("/// [%s]: %s", link, rusty))
	}
	return results
}

func rustDocLink(link string, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	id := fmt.Sprintf(".%s", link)
	m, ok := state.MessageByID[id]
	if ok {
		return rustFQMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	e, ok := state.EnumByID[id]
	if ok {
		return rustFQEnumName(e, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	me, ok := state.MethodByID[id]
	if ok {
		return rustMethodRustdocLink(me, state, packageMapping)
	}
	s, ok := state.ServiceByID[id]
	if ok {
		return rustServiceRustdocLink(s, packageMapping)
	}
	rdLink := rustTryFieldRustdocLink(id, state, modulePath, sourceSpecificationPackageName, packageMapping)
	if rdLink != "" {
		return rdLink
	}
	rdLink = rustTryEnumValueRustdocLink(id, state, modulePath, sourceSpecificationPackageName, packageMapping)
	if rdLink != "" {
		return rdLink
	}
	return ""
}

func rustTryFieldRustdocLink(id string, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	idx := strings.LastIndex(id, ".")
	if idx == -1 {
		return ""
	}
	messageId := id[0:idx]
	fieldName := id[idx+1:]
	m, ok := state.MessageByID[messageId]
	if !ok {
		return ""
	}
	for _, f := range m.Fields {
		if f.Name == fieldName {
			if !f.IsOneOf {
				return fmt.Sprintf("%s::%s", rustFQMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping), rustToSnake(f.Name))
			} else {
				return rustTryOneOfRustdocLink(f, m, modulePath, sourceSpecificationPackageName, packageMapping)
			}
		}
	}
	return ""
}

func rustTryOneOfRustdocLink(field *api.Field, message *api.Message, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	for _, o := range message.OneOfs {
		for _, f := range o.Fields {
			if f.ID == field.ID {
				return fmt.Sprintf("%s::%s", rustFQMessageName(message, modulePath, sourceSpecificationPackageName, packageMapping), rustToSnake(o.Name))
			}
		}
	}
	return ""
}

func rustTryEnumValueRustdocLink(id string, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) string {
	idx := strings.LastIndex(id, ".")
	if idx == -1 {
		return ""
	}
	enumId := id[0:idx]
	valueName := id[idx+1:]
	e, ok := state.EnumByID[enumId]
	if !ok {
		return ""
	}
	for _, v := range e.Values {
		if v.Name == valueName {
			return rustFQEnumValueName(v, modulePath, sourceSpecificationPackageName, packageMapping)
		}
	}
	return ""
}

func rustMethodRustdocLink(m *api.Method, state *api.APIState, packageMapping map[string]*rustPackage) string {
	// Sometimes we remove methods from a service. In that case we cannot
	// reference the method.
	if !rustGenerateMethod(m) {
		return ""
	}
	idx := strings.LastIndex(m.ID, ".")
	if idx == -1 {
		return ""
	}
	serviceId := m.ID[0:idx]
	s, ok := state.ServiceByID[serviceId]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s::%s", rustServiceRustdocLink(s, packageMapping), rustToSnake(m.Name))
}

func rustServiceRustdocLink(s *api.Service, packageMapping map[string]*rustPackage) string {
	mapped, ok := rustMapPackage(s.Package, packageMapping)
	if ok {
		return fmt.Sprintf("%s::traits::%s", mapped.name, s.Name)
	}
	return fmt.Sprintf("crate::traits::%s", s.Name)
}

func rustProjectRoot(outdir string) string {
	if outdir == "" {
		return ""
	}
	rel := ".."
	for range strings.Count(outdir, "/") {
		rel = path.Join(rel, "..")
	}
	return rel
}

func rustMapPackage(source string, packageMapping map[string]*rustPackage) (*rustPackage, bool) {
	mapped, ok := packageMapping[source]
	if ok {
		mapped.used = true
	}
	return mapped, ok
}

func rustRequiredPackages(outdir string, extraPackages []*rustPackage) []string {
	lines := []string{}
	for _, pkg := range extraPackages {
		if pkg.ignore {
			continue
		}
		if !pkg.used {
			continue
		}
		components := []string{}
		if pkg.version != "" {
			components = append(components, fmt.Sprintf("version = %q", pkg.version))
		}
		if pkg.path != "" {
			components = append(components, fmt.Sprintf("path = %q", path.Join(rustProjectRoot(outdir), pkg.path)))
		}
		if pkg.packageName != "" && pkg.name != pkg.packageName {
			components = append(components, fmt.Sprintf("package = %q", pkg.packageName))
		}
		if !pkg.defaultFeatures {
			components = append(components, "default-features = false")
		}
		if len(pkg.features) > 0 {
			feats := strings.Join(mapSlice(pkg.features, func(s string) string { return fmt.Sprintf("%q", s) }), ", ")
			components = append(components, fmt.Sprintf("features = [%s]", feats))
		}
		lines = append(lines, fmt.Sprintf("%-10s = { %s }", pkg.name, strings.Join(components, ", ")))
	}
	sort.Strings(lines)
	return lines
}

func rustPackageName(api *api.API, packageNameOverride string) string {
	if len(packageNameOverride) > 0 {
		return packageNameOverride
	}
	name := strings.TrimPrefix(api.PackageName, "google.cloud.")
	name = strings.TrimPrefix(name, "google.")
	name = strings.ReplaceAll(name, ".", "-")
	if name == "" {
		name = api.Name
	}
	return "gcp-sdk-" + name
}

func rustValidate(api *api.API, sourceSpecificationPackageName string) error {
	validatePkg := func(newPackage, elementName string) error {
		if sourceSpecificationPackageName == newPackage {
			return nil
		}
		// Special exceptions for mixin services
		if newPackage == "google.cloud.location" ||
			newPackage == "google.iam.v1" ||
			newPackage == "google.longrunning" {
			return nil
		}
		return fmt.Errorf("rust codec requires all top-level elements to be in the same package want=%s, got=%s for %s",
			sourceSpecificationPackageName, newPackage, elementName)
	}

	for _, s := range api.Services {
		if err := validatePkg(s.Package, s.ID); err != nil {
			return err
		}
	}
	for _, s := range api.Messages {
		if err := validatePkg(s.Package, s.ID); err != nil {
			return err
		}
	}
	for _, s := range api.Enums {
		if err := validatePkg(s.Package, s.ID); err != nil {
			return err
		}
	}
	return nil
}

func rustAddStreamingFeature(data *RustTemplateData, api *api.API, extraPackages []*rustPackage) {
	var hasStreamingRPC bool
	for _, m := range api.Messages {
		if m.IsPageableResponse {
			hasStreamingRPC = true
			break
		}
	}
	if !hasStreamingRPC {
		return
	}
	// Create a list of dependency features we need to enable. To avoid
	// uninteresting changes, always sort the list.
	feature := func(name string) string {
		return fmt.Sprintf(`"%s/unstable-stream"`, name)
	}
	deps := []string{feature("gax")}
	for _, p := range extraPackages {
		if p.ignore || !p.used {
			continue
		}
		// Only mixins are relevant here, and only longrunning and location have
		// streaming features. Hardcoding the list is not a terrible problem.
		if p.name == "location" || p.name == "longrunning" {
			deps = append(deps, feature(p.name))
		}
	}
	sort.Strings(deps)
	features := fmt.Sprintf("unstable-stream = [%s]", strings.Join(deps, ", "))
	data.HasFeatures = true
	data.Features = append(data.Features, features)
}

func rustGenerateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations, we cannot generate working
	// RPCs for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	return !m.ClientSideStreaming && !m.ServerSideStreaming && m.PathInfo != nil && len(m.PathInfo.PathTemplate) != 0
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
