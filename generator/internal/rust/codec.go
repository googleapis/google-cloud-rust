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
	"bytes"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/iancoleman/strcase"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

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
		`([A-Za-z0-9\.\-_]+\.)+` + // Be generous in accepting most of the authority (hostname)
		`[a-zA-Z]{2,63}` + // The root domain is far more strict
		`(/[-a-zA-Z0-9@:%_\+.~#?&/={}\$]*)?`) // Accept just about anything on the query and URL fragments

func newCodec(protobufSource bool, options map[string]string) (*codec, error) {
	var sysParams []systemParameter
	if protobufSource {
		sysParams = append(sysParams, systemParameter{
			Name: "$alt", Value: "json;enum-encoding=int",
		})
	} else {
		sysParams = append(sysParams, systemParameter{
			Name: "$alt", Value: "json",
		})
	}

	year, _, _ := time.Now().Date()
	codec := &codec{
		generationYear:   fmt.Sprintf("%04d", year),
		modulePath:       "crate::model",
		extraPackages:    []*packagez{},
		packageMapping:   map[string]*packagez{},
		version:          "0.0.0",
		releaseLevel:     "preview",
		systemParameters: sysParams,
	}

	for key, definition := range options {
		switch {
		case key == "package-name-override":
			codec.packageNameOverride = definition
		case key == "name-overrides":
			codec.nameOverrides = make(map[string]string)
			for _, override := range strings.Split(definition, ",") {
				tokens := strings.Split(override, "=")
				if len(tokens) != 2 {
					return nil, fmt.Errorf("cannot parse `name-overrides`. Expected input in the form of: 'n1=r1,n2=r2': %q", definition)
				}
				codec.nameOverrides[tokens[0]] = tokens[1]
			}
		case key == "module-path":
			codec.modulePath = definition
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
		case key == "release-level":
			codec.releaseLevel = definition
		case strings.HasPrefix(key, "package:"):
			pkgOption, err := parsePackageOption(key, definition)
			if err != nil {
				return nil, err
			}
			codec.extraPackages = append(codec.extraPackages, pkgOption.pkg)
			for _, source := range pkgOption.otherNames {
				codec.packageMapping[source] = pkgOption.pkg
			}
		case key == "disabled-rustdoc-warnings":
			if definition == "" {
				codec.disabledRustdocWarnings = []string{}
			} else {
				codec.disabledRustdocWarnings = strings.Split(definition, ",")
			}
		case key == "template-override":
			codec.templateOverride = definition
		case key == "include-grpc-only-methods":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `include-grpc-only-methods` value %q to boolean: %w", definition, err)
			}
			codec.includeGrpcOnlyMethods = value
		case key == "per-service-features":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `per-service-features` value %q to boolean: %w", definition, err)
			}
			codec.perServiceFeatures = value
		case key == "has-veneer":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `has-veneer` value %q to boolean: %w", definition, err)
			}
			codec.hasVeneer = value
		case key == "with-generated-serde":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf("cannot convert `with-generated-serde` value %q to boolean: %w", definition, err)
			}
			codec.withGeneratedSerde = value
		default:
			return nil, fmt.Errorf("unknown Rust codec option %q", key)
		}
	}
	return codec, nil
}

type packageOption struct {
	pkg        *packagez
	otherNames []string
}

func parsePackageOption(key, definition string) (*packageOption, error) {
	var specificationPackages []string
	pkg := &packagez{
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
	return &packageOption{pkg: pkg, otherNames: specificationPackages}, nil
}

type codec struct {
	// Package name override. If not empty, overrides the default package name.
	packageNameOverride string
	// Name overrides. Maps IDs to new *unqualified* names, e.g.:
	//   .google.test.Service: Rename
	//   .google.test.Message.conflict_name_oneof: ConflictNameOneOf
	//
	// TODO(#1173) - this only supports services and oneofs at the moment.
	nameOverrides map[string]string
	// The year when the files were first generated.
	generationYear string
	// The full path of the generated module within the crate. This defaults to
	// `model`. When generating only a module within a larger crate (see
	// `GenerateModule`), this overrides the path for elements within the crate.
	// Note that using `self` does not work, as the generated code may contain
	// nested modules for nested messages.
	modulePath string
	// Additional Rust packages imported by this module. The Mustache template
	// hardcodes a number of packages, but some are configured via the
	// command-line.
	extraPackages []*packagez
	// A mapping between the specification package names (typically Protobuf),
	// and the Rust package name that contains these types.
	packageMapping map[string]*packagez
	// Some packages are not intended for publication. For example, they may be
	// intended only for testing the generator or the SDK, or the service may
	// not be GA.
	doNotPublish bool
	// The version of the generated crate.
	version string
	// The "release level" as required by the `.repo-metadata.json` file.
	// Typically "stable" or "preview".
	releaseLevel string
	// True if the API model includes any services
	hasServices bool
	// A list of `rustdoc` warnings disabled for specific services.
	disabledRustdocWarnings []string
	// The default system parameters included in all requests.
	systemParameters []systemParameter
	// Overrides the template sudirectory.
	templateOverride string
	// If true, this includes gRPC-only methods, such as methods without HTTP
	// annotations.
	includeGrpcOnlyMethods bool
	// If true, the generator will produce per-client features.
	perServiceFeatures bool
	// If true, there is a handwritten client surface.
	hasVeneer bool
	// If true, enable helper types for generated serde serialization
	withGeneratedSerde bool
}

type systemParameter struct {
	Name  string
	Value string
}

type packagez struct {
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

var wellKnownMessages = []*api.Message{
	{
		ID:      ".google.protobuf.Any",
		Name:    "Any",
		Package: "google.protobuf",
	},
	{
		ID:      ".google.protobuf.Struct",
		Name:    "Struct",
		Package: "google.protobuf",
	},
	{
		ID:      ".google.protobuf.Value",
		Name:    "Value",
		Package: "google.protobuf",
	},
	{
		ID:      ".google.protobuf.ListValue",
		Name:    "ListValue",
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

func loadWellKnownTypes(s *api.APIState) {
	for _, message := range wellKnownMessages {
		s.MessageByID[message.ID] = message
	}
	s.EnumByID[".google.protobuf.NullValue"] = &api.Enum{
		Name:    "NullValue",
		Package: "google.protobuf",
		ID:      ".google.protobuf.NullValue",
	}
}

func resolveUsedPackages(model *api.API, extraPackages []*packagez) {
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
		out = "std::string::String"
	case api.BYTES_TYPE:
		out = "::bytes::Bytes"
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

func fieldFormatter(typez api.Typez) string {
	switch typez {
	case api.INT64_TYPE, api.SINT64_TYPE, api.SFIXED64_TYPE:
		return "wkt::internal::I64"
	case api.UINT64_TYPE, api.FIXED64_TYPE:
		return "wkt::internal::U64"
	case api.INT32_TYPE, api.SINT32_TYPE, api.SFIXED32_TYPE:
		return "wkt::internal::I32"
	case api.UINT32_TYPE, api.FIXED32_TYPE:
		return "wkt::internal::U32"
	case api.FLOAT_TYPE:
		return "wkt::internal::F32"
	case api.DOUBLE_TYPE:
		return "wkt::internal::F64"
	case api.BYTES_TYPE:
		return "serde_with::base64::Base64"
	default:
		return "_"
	}
}

func keyFieldFormatter(typez api.Typez) string {
	if typez == api.BOOL_TYPE {
		return "serde_with::DisplayFromStr"
	}
	return fieldFormatter(typez)
}

func fieldSkipAttributes(f *api.Field) []string {
	// oneofs have explicit presence, and default values should be serialized:
	// https://protobuf.dev/programming-guides/field_presence/.
	if f.IsOneOf {
		return []string{}
	}
	if f.Optional {
		return []string{`#[serde(skip_serializing_if = "std::option::Option::is_none")]`}
	}
	if f.Repeated {
		return []string{`#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`}
	}
	switch f.Typez {
	case api.STRING_TYPE:
		return []string{`#[serde(skip_serializing_if = "std::string::String::is_empty")]`}
	case api.BYTES_TYPE:
		return []string{`#[serde(skip_serializing_if = "::bytes::Bytes::is_empty")]`}
	case api.DOUBLE_TYPE,
		api.FLOAT_TYPE,
		api.INT64_TYPE,
		api.UINT64_TYPE,
		api.INT32_TYPE,
		api.FIXED64_TYPE,
		api.FIXED32_TYPE,
		api.BOOL_TYPE,
		api.UINT32_TYPE,
		api.SFIXED32_TYPE,
		api.SFIXED64_TYPE,
		api.SINT32_TYPE,
		api.SINT64_TYPE,
		api.ENUM_TYPE:
		return []string{`#[serde(skip_serializing_if = "wkt::internal::is_default")]`}
	default:
		return []string{}
	}
}

func fieldBaseAttributes(f *api.Field) []string {
	// Names starting with `_` are not handled quite right by serde.
	if toCamel(f.Name) != f.JSONName || strings.HasPrefix(f.Name, "_") {
		return []string{fmt.Sprintf(`#[serde(rename = "%s")]`, f.JSONName)}
	}
	return []string{}
}

func messageFieldAttributes(f *api.Field, attributes []string) []string {
	// Message fields could be `Vec<..>`, and are always optional:
	attributes = messageFieldSkipAttributes(f, attributes)
	var formatter string
	switch f.TypezID {
	case ".google.protobuf.BytesValue":
		formatter = fieldFormatter(api.BYTES_TYPE)
	case ".google.protobuf.UInt64Value":
		formatter = fieldFormatter(api.UINT64_TYPE)
	case ".google.protobuf.Int64Value":
		formatter = fieldFormatter(api.INT64_TYPE)
	case ".google.protobuf.UInt32Value":
		formatter = fieldFormatter(api.UINT32_TYPE)
	case ".google.protobuf.Int32Value":
		formatter = fieldFormatter(api.INT32_TYPE)
	case ".google.protobuf.FloatValue":
		formatter = fieldFormatter(api.FLOAT_TYPE)
	case ".google.protobuf.DoubleValue":
		formatter = fieldFormatter(api.DOUBLE_TYPE)
	default:
		formatter = "_"
	}
	if f.IsOneOf {
		if formatter == "_" {
			return attributes
		}
		return append(attributes, fmt.Sprintf(`#[serde_as(as = "%s")]`, oneOfFieldTypeFormatter(f, false, formatter)))
	}
	if f.Optional {
		if f.TypezID == ".google.protobuf.Value" {
			return append(attributes, `#[serde_as(as = "wkt::internal::OptionalValue")]`)
		}
		if formatter == "_" {
			return attributes
		}
		return append(
			attributes,
			fmt.Sprintf(`#[serde_as(as = "std::option::Option<%s>")]`, formatter))
	}
	if f.Repeated {
		return append(
			attributes,
			fmt.Sprintf(`#[serde_as(as = "serde_with::DefaultOnNull<std::vec::Vec<%s>>")]`, formatter))
	}
	return append(
		attributes,
		fmt.Sprintf(`#[serde_as(as = "serde_with::DefaultOnNull<%s>")]`, formatter))
}

func messageFieldSkipAttributes(f *api.Field, attributes []string) []string {
	// oneofs have explicit presence, and default values should be serialized:
	// https://protobuf.dev/programming-guides/field_presence/.
	if f.IsOneOf {
		return attributes
	}
	if f.Optional {
		attributes = append(attributes, `#[serde(skip_serializing_if = "std::option::Option::is_none")]`)
	}
	if f.Repeated && !f.IsOneOf {
		attributes = append(attributes, `#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`)
	}
	return attributes
}

func mapFieldAttributes(f *api.Field, message *api.Message, attributes []string) []string {
	// map<> field types require special treatment.
	if !f.IsOneOf {
		attributes = append(attributes, `#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`)
	}
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
	keyFormat := keyFieldFormatter(key.Typez)
	valFormat := fieldFormatter(value.Typez)
	return append(attributes, fmt.Sprintf(`#[serde_as(as = "serde_with::DefaultOnNull<std::collections::HashMap<%s, %s>>")]`, keyFormat, valFormat))

}

func fieldAttributes(f *api.Field, state *api.APIState) []string {
	if f.Synthetic {
		return []string{`#[serde(skip)]`}
	}
	attributes := fieldBaseAttributes(f)
	switch f.Typez {
	case api.GROUP_TYPE:
		return append(attributes, fieldSkipAttributes(f)...)

	case api.BOOL_TYPE,
		api.STRING_TYPE,
		api.ENUM_TYPE,
		api.INT32_TYPE,
		api.SFIXED32_TYPE,
		api.SINT32_TYPE,
		api.UINT32_TYPE,
		api.FIXED32_TYPE,
		api.INT64_TYPE,
		api.UINT64_TYPE,
		api.FIXED64_TYPE,
		api.SFIXED64_TYPE,
		api.SINT64_TYPE,
		api.BYTES_TYPE,
		api.FLOAT_TYPE,
		api.DOUBLE_TYPE:
		formatter := fieldFormatter(f.Typez)
		attributes = append(attributes, fieldSkipAttributes(f)...)
		if f.Optional {
			if formatter != "_" {
				attributes = append(attributes, fmt.Sprintf(`#[serde_as(as = "std::option::Option<%s>")]`, formatter))
			}
			return attributes
		}
		if f.Repeated {
			return append(attributes, fmt.Sprintf(`#[serde_as(as = "serde_with::DefaultOnNull<std::vec::Vec<%s>>")]`, formatter))
		}
		return append(attributes, fmt.Sprintf(`#[serde_as(as = "serde_with::DefaultOnNull<%s>")]`, formatter))

	case api.MESSAGE_TYPE:
		if message, ok := state.MessageByID[f.TypezID]; ok && message.IsMap {
			return mapFieldAttributes(f, message, attributes)
		}
		return messageFieldAttributes(f, attributes)

	default:
		slog.Error("unexpected field type", "field", *f)
		return attributes
	}
}

func oneOfFieldType(f *api.Field, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	baseType := baseFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping)
	return oneOfFieldTypeFormatter(f, language.FieldIsMap(f, state), baseType)
}

func oneOfFieldTypeFormatter(f *api.Field, fieldIsMap bool, baseType string) string {
	switch {
	case f.Repeated:
		return fmt.Sprintf("std::vec::Vec<%s>", baseType)
	case f.Typez == api.MESSAGE_TYPE:
		if fieldIsMap {
			return baseType
		}
		return fmt.Sprintf("std::boxed::Box<%s>", baseType)
	case f.Optional:
		return fmt.Sprintf("std::option::Option<%s>", baseType)
	default:
		return baseType
	}
}

func fieldType(f *api.Field, state *api.APIState, primitive bool, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	baseType := baseFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping)
	switch {
	case primitive:
		return baseType
	case f.IsOneOf:
		return oneOfFieldType(f, state, modulePath, sourceSpecificationPackageName, packageMapping)
	case f.Repeated:
		return fmt.Sprintf("std::vec::Vec<%s>", baseType)
	case f.Recursive:
		if f.Optional {
			return fmt.Sprintf("std::option::Option<std::boxed::Box<%s>>", baseType)
		}
		if language.FieldIsMap(f, state) {
			// Maps are never boxed.
			return baseType
		}
		return fmt.Sprintf("std::boxed::Box<%s>", baseType)
	case f.Optional:
		return fmt.Sprintf("std::option::Option<%s>", baseType)
	default:
		return baseType
	}
}

func mapType(f *api.Field, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	switch f.Typez {
	case api.MESSAGE_TYPE:
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID, "field", f.ID)
			return ""
		}
		return fullyQualifiedMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping)

	case api.ENUM_TYPE:
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID, "field", f.ID)
			return ""
		}
		return fullyQualifiedEnumName(e, modulePath, sourceSpecificationPackageName, packageMapping)
	default:
		return scalarFieldType(f)
	}
}

// Returns the field type, ignoring any repeated or optional attributes.
func baseFieldType(f *api.Field, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	if f.Typez == api.MESSAGE_TYPE {
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID, "field", f.ID)
			return ""
		}
		if m.IsMap {
			key := mapType(m.Fields[0], state, modulePath, sourceSpecificationPackageName, packageMapping)
			val := mapType(m.Fields[1], state, modulePath, sourceSpecificationPackageName, packageMapping)
			return "std::collections::HashMap<" + key + "," + val + ">"
		}
		return fullyQualifiedMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping)
	} else if f.Typez == api.ENUM_TYPE {
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID, "field", f.ID)
			return ""
		}
		return fullyQualifiedEnumName(e, modulePath, sourceSpecificationPackageName, packageMapping)
	} else if f.Typez == api.GROUP_TYPE {
		slog.Error("TODO(#39) - better handling of `oneof` fields")
		return ""
	}
	return scalarFieldType(f)
}

func addQueryParameter(f *api.Field) string {
	if f.IsOneOf {
		return addQueryParameterOneOf(f)
	}
	fieldName := toSnake(f.Name)
	switch f.Typez {
	case api.ENUM_TYPE:
		if f.Optional || f.Repeated {
			return fmt.Sprintf(`let builder = req.%s.iter().fold(builder, |builder, p| builder.query(&[("%s", p)]));`, fieldName, f.JSONName)
		}
		return fmt.Sprintf(`let builder = builder.query(&[("%s", &req.%s)]);`, f.JSONName, fieldName)
	case api.MESSAGE_TYPE:
		// Query parameters in nested messages are first converted to a
		// `serde_json::Value`` and then recursively merged into the request
		// query. The conversion to `serde_json::Value` is expensive, but very
		// few requests use nested objects as query parameters. Furthermore,
		// the conversion is skipped if the object field is `None`.`
		if f.Optional || f.Repeated {
			return fmt.Sprintf(`let builder = req.%s.as_ref().map(|p| serde_json::to_value(p).map_err(Error::ser) ).transpose()?.into_iter().fold(builder, |builder, v| { use gaxi::query_parameter::QueryParameter; v.add(builder, "%s") });`, fieldName, f.JSONName)
		}
		return fmt.Sprintf(`let builder = { use gaxi::query_parameter::QueryParameter; serde_json::to_value(&req.%s).map_err(Error::ser)?.add(builder, "%s") };`, fieldName, f.JSONName)
	default:
		if f.Optional || f.Repeated {
			return fmt.Sprintf(`let builder = req.%s.iter().fold(builder, |builder, p| builder.query(&[("%s", p)]));`, fieldName, f.JSONName)
		}
		return fmt.Sprintf(`let builder = builder.query(&[("%s", &req.%s)]);`, f.JSONName, fieldName)
	}
}

func addQueryParameterOneOf(f *api.Field) string {
	fieldName := toSnake(f.Name)
	switch f.Typez {
	case api.ENUM_TYPE:
		return fmt.Sprintf(`let builder = req.%s().iter().fold(builder, |builder, p| builder.query(&[("%s", p)]));`, fieldName, f.JSONName)
	case api.MESSAGE_TYPE:
		// Query parameters in nested messages are first converted to a
		// `serde_json::Value`` and then recursively merged into the request
		// query. The conversion to `serde_json::Value` is expensive, but very
		// few requests use nested objects as query parameters. Furthermore,
		// the conversion is skipped if the object field is `None`.`
		return fmt.Sprintf(`let builder = req.%s().map(|p| serde_json::to_value(p).map_err(Error::ser) ).transpose()?.into_iter().fold(builder, |builder, p| { use gaxi::query_parameter::QueryParameter; p.add(builder, "%s") });`, fieldName, f.JSONName)
	default:
		return fmt.Sprintf(`let builder = req.%s().iter().fold(builder, |builder, p| builder.query(&[("%s", p)]));`, fieldName, f.JSONName)
	}
}

func (c *codec) methodInOutTypeName(id string, state *api.APIState, sourceSpecificationPackageName string) string {
	if id == "" {
		return ""
	}
	m, ok := state.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return fullyQualifiedMessageName(m, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
}

func (c *codec) messageAttributes() []string {
	if c.withGeneratedSerde {
		return []string{
			`#[derive(Clone, Debug, Default, PartialEq)]`,
			`#[non_exhaustive]`,
		}
	}
	return []string{
		`#[serde_with::serde_as]`,
		`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
		`#[serde(default, rename_all = "camelCase")]`,
		`#[non_exhaustive]`,
	}
}

func messageScopeName(m *api.Message, childPackageName, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	rustPkg := func(packageName string) string {
		if packageName == sourceSpecificationPackageName {
			return modulePath
		}
		mapped, ok := packageMapping[packageName]
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
		return rustPkg(m.Package) + "::" + toSnake(m.Name)
	}
	return messageScopeName(m.Parent, m.Package, modulePath, sourceSpecificationPackageName, packageMapping) + "::" + toSnake(m.Name)
}

func enumScopeName(e *api.Enum, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	return messageScopeName(e.Parent, e.Package, modulePath, sourceSpecificationPackageName, packageMapping)
}

func fullyQualifiedMessageName(m *api.Message, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	return messageScopeName(m.Parent, m.Package, modulePath, sourceSpecificationPackageName, packageMapping) + "::" + toPascal(m.Name)
}

func enumName(e *api.Enum) string {
	return toPascal(e.Name)
}

func fullyQualifiedEnumName(e *api.Enum, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	return messageScopeName(e.Parent, e.Package, modulePath, sourceSpecificationPackageName, packageMapping) + "::" + toPascal(e.Name)
}

func enumValueName(e *api.EnumValue) string {
	// The Protobuf naming convention is to use SCREAMING_SNAKE_CASE, but
	// sometimes it is not followed.
	return escapeKeyword(toScreamingSnake(e.Name))
}

// enumValueVariantName returns the name of the Rust enumeration variant for a
// given enumeration.
//
// The Protobuf naming convention is to use SCREAMING_SNAKE_CASE, often
// prefixed with the name of the enum, e.g.:
//
// ```proto
//
//	enum MyEnum {
//	    MY_ENUM_UNSPECIFIED = 0;
//	    MY_ENUM_RED            = 1;
//	    MY_ENUM_GREEN          = 2;
//	    MY_ENUM_BLACK_AND_BLUE = 2;
//	    MY_ENUM_123            = 123;
//	}
//
// ```
//
// What we want in this case is something like:
//
// ```rust
// #[non_exhaustive]
//
//	pub enum Syntax {
//	    Unspecified,
//	    Red,
//	    Green,
//	    BlackAndBlue,
//	    MyEnum123,
//	    UnknownVariant(/* implementation detail */),
//	}
//
// ```
// sometimes it is not followed.
func enumValueVariantName(e *api.EnumValue) string {
	// The most common case is trying to strip the prefix for `FOO_BAR_UNSPECIFIED`.
	// The naming conventions being what they are, we need to test with a couple
	// of different combinations. In particular, names with numbers, such as
	// `InstancePrivateIpv6GoogleAccess` may be represented as
	// `INSTANCE_PRIVATE_IPV6_GOOGLE_ACCESS` in enum values, while the automatic
	// transformation would map it as `INSTANCE_PRIVATE_IPV_6_GOOGLE_ACCESS`.
	// Note the extra `_` in `IPV_6` in the second case.
	prefix := toScreamingSnake(e.Parent.Name) + "_"
	trimmed := strings.TrimPrefix(e.Name, prefix)
	if strings.HasPrefix(e.Name, prefix) && strings.IndexFunc(trimmed, unicode.IsLetter) == 0 {
		return toPascal(trimmed)
	}
	trimNumbers := regexp.MustCompile(`_([0-9])`)
	prefix = trimNumbers.ReplaceAllString(prefix, `$1`)
	trimmed = strings.TrimPrefix(e.Name, prefix)
	if strings.HasPrefix(e.Name, prefix) && strings.IndexFunc(trimmed, unicode.IsLetter) == 0 {
		return toPascal(trimmed)
	}
	return toPascal(e.Name)
}

func fullyQualifiedEnumValueName(v *api.EnumValue, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) string {
	return fmt.Sprintf("%s::%s::%s", enumScopeName(v.Parent, modulePath, sourceSpecificationPackageName, packageMapping), enumName(v.Parent), enumValueVariantName(v))
}

func bodyAccessor(m *api.Method) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + toSnake(m.PathInfo.BodyFieldPath)
}

func httpPathFmt(m *api.PathInfo) string {
	binding := m.Bindings[0]
	fmt := ""
	for _, segment := range binding.PathTemplate {
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

func derefFieldExpr(name string, optional bool, nextMessage *api.Message) (string, *api.Message) {
	const (
		optionalFmt = `.%s.as_ref().ok_or_else(|| gaxi::path_parameter::missing("%s"))?`
	)
	if optional {
		return fmt.Sprintf(optionalFmt, name, name), nextMessage
	}
	return fmt.Sprintf(`.%s`, name), nextMessage
}

func derefFieldSingle(name string, message *api.Message, state *api.APIState) (string, *api.Message) {
	for _, field := range message.Fields {
		if name != field.Name {
			continue
		}
		if field.Typez == api.MESSAGE_TYPE {
			if nextMessage, ok := state.MessageByID[field.TypezID]; ok {
				return derefFieldExpr(name, field.Optional, nextMessage)
			}
			slog.Error("cannot find next message for field", "currentMessage", message, "fieldName", name)
		}
		return derefFieldExpr(name, field.Optional, nil)
	}
	return "", nil
}

func derefFieldPath(fieldPath string, message *api.Message, state *api.APIState) string {
	var expression strings.Builder
	components := strings.Split(fieldPath, ".")
	msg := message
	for _, name := range components {
		if msg == nil {
			slog.Error("cannot build full expression", "fieldPath", fieldPath, "message", msg)
		}
		expr, nextMessage := derefFieldSingle(name, msg, state)
		expression.WriteString(expr)
		msg = nextMessage
	}
	return expression.String()
}

func leafFieldTypez(fieldPath string, message *api.Message, state *api.APIState) api.Typez {
	typez := api.UNDEFINED_TYPE
	components := strings.Split(fieldPath, ".")
	msg := message
	for _, name := range components {
		if msg == nil {
			slog.Error("cannot find leaf field type", "fieldPath", fieldPath, "message", msg)
			return typez
		}
		for _, field := range msg.Fields {
			if name != field.Name {
				continue
			}
			typez = field.Typez
			if field.Typez == api.MESSAGE_TYPE {
				msg = state.MessageByID[field.TypezID]
			}
			break
		}
	}
	return typez
}

type pathArg struct {
	Name          string
	Accessor      string
	CheckForEmpty bool
}

func httpPathArgs(h *api.PathInfo, method *api.Method, state *api.APIState) []pathArg {
	message, ok := state.MessageByID[method.InputTypeID]
	if !ok {
		slog.Error("cannot find input message for", "method", method)
		return []pathArg{}
	}
	var params []pathArg
	for _, arg := range h.Bindings[0].PathTemplate {
		if arg.FieldPath != nil {
			leafTypez := leafFieldTypez(*arg.FieldPath, message, state)
			params = append(params, pathArg{
				Name:          *arg.FieldPath,
				Accessor:      derefFieldPath(*arg.FieldPath, message, state),
				CheckForEmpty: leafTypez == api.STRING_TYPE,
			})
		}
	}
	return params
}

// Convert a name to `snake_case`. The Rust naming conventions use this style
// for modules, fields, and functions.
//
// This type of conversion can easily introduce keywords. Consider
//
//	`toSnake("True") -> "true"`
func toSnake(symbol string) string {
	return escapeKeyword(toSnakeNoMangling(symbol))
}

func toSnakeNoMangling(symbol string) string {
	if strings.ToLower(symbol) == symbol {
		return symbol
	}
	return strcase.ToSnake(symbol)
}

// Convert a name to `PascalCase`.  Strangely, the `strcase` package calls this
// `ToCamel` while usually `camelCase` starts with a lowercase letter. The
// Rust naming conventions use this style for structs, enums and traits.
//
// This type of conversion rarely introduces keywords. The one example is
//
//	`toPascal("self") -> "Self"`
func toPascal(symbol string) string {
	if symbol == "" {
		return ""
	}
	// The Rust style guide frowns on all upppercase for struct names, even if
	// they are acronyms (consider `IAM`). In such cases we must use the normal
	// mapping.
	if strings.ToUpper(symbol) == symbol {
		return escapeKeyword(strcase.ToCamel(symbol))
	}
	// Symbols that are already `PascalCase` should need no mapping. This works
	// better than calling `strcase.ToCamel()` in cases like `IAMPolicy`, which
	// would be converted to `IamPolicy`. We are trusting that the original
	// name in Protobuf (or whatever source specification we are using) chose
	// to keep the acronym for a reason.
	runes := []rune(symbol)
	if unicode.IsUpper(runes[0]) && !strings.ContainsRune(symbol, '_') {
		return escapeKeyword(symbol)
	}
	return escapeKeyword(strcase.ToCamel(symbol))
}

func toCamel(symbol string) string {
	return escapeKeyword(strcase.ToLowerCamel(symbol))
}

// Convert a name to `SCREAMING_SNAKE_CASE`. The Rust naming conventions use
// this style for constants.
func toScreamingSnake(symbol string) string {
	if strings.ToUpper(symbol) == symbol {
		return symbol
	}
	return strcase.ToScreamingSnake(symbol)
}

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
func (c *codec) formatDocComments(
	documentation, elementID string, state *api.APIState, scopes []string) []string {
	var results []string
	md := goldmark.New(
		goldmark.WithParserOptions(
			parser.WithAutoHeadingID(),
		),
		goldmark.WithExtensions(),
	)

	documentationBytes := []byte(documentation)
	doc := md.Parser().Parse(text.NewReader(documentationBytes))
	ast.Walk(doc, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		switch node.Kind() {
		case ast.KindCodeBlock:
			if entering {
				formattedOutput := annotateCodeBlock(node, documentationBytes)
				results = append(results, formattedOutput...)
			}
		case ast.KindFencedCodeBlock:
			if entering {
				formattedOutput := annotateFencedCodeBlock(node, documentationBytes)
				results = append(results, formattedOutput...)
			}
		case ast.KindList:
			if entering {
				if node.Parent() != nil && node.Parent().Kind() == ast.KindListItem {
					return ast.WalkContinue, nil
				}
				formattedOutput := processList(node.(*ast.List), 0, documentationBytes, elementID)
				results = append(results, formattedOutput...)
				results = append(results, "\n")
			}
		case ast.KindParagraph:
			if entering {
				// Skip adding list items as they are being taken care of separately.
				if node.Parent() != nil && node.Parent().Kind() == ast.KindListItem {
					return ast.WalkContinue, nil
				}
				formattedOutput := processParagraph(node, documentationBytes)
				results = append(results, formattedOutput...)
			}
		case ast.KindHeading:
			if entering {
				heading := node.(*ast.Heading)
				headingPrefix := strings.Repeat("#", heading.Level)
				results = append(results, fmt.Sprintf("%s %s", headingPrefix, string(heading.BaseBlock.Lines().Value(documentationBytes))))
				results = append(results, "\n")
			}
		}
		return ast.WalkContinue, nil
	})

	for _, link := range protobufLinkMapping(doc, documentationBytes) {
		rusty := c.docLink(link, state, scopes)
		if rusty == "" {
			continue
		}
		results = append(results, fmt.Sprintf("[%s]: %s", link, rusty))
	}

	if len(results) > 0 && results[len(results)-1] == "\n" {
		results = results[:len(results)-1]
	}
	for i, line := range results {
		results[i] = strings.TrimRightFunc(fmt.Sprintf("/// %s", line), unicode.IsSpace)
	}
	return results
}

// protobufLinks() returns additional comment lines to map protobuf links to
// Rustdoc links.
//
// Protobuf comments include links in the form `[Title][Definition]` where
// `Title` is the text that should appear in the documentation and `Definition`
// is the name of a Protobuf entity, e.g., `google.longrunning.Operation`.
//
// We need to map these references from Protobuf names to the corresponding
// entity in the generated code. We do this by appending a number of link
// definitions to the comments, e.g.
//
//	//// [google.longrunning.Operation]: lro::model::Operation
func protobufLinkMapping(doc ast.Node, source []byte) []string {
	protobufLinks := map[string]bool{}
	ast.Walk(doc, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		switch node.Kind() {
		case ast.KindParagraph:
			text := node.Lines().Value(source)
			extractProtoLinks(text, protobufLinks)
			return ast.WalkContinue, nil
		case ast.KindTextBlock:
			text := node.Lines().Value(source)
			extractProtoLinks(text, protobufLinks)
			return ast.WalkContinue, nil
		default:
			return ast.WalkContinue, nil
		}
	})
	var sortedLinks []string
	for link := range protobufLinks {
		sortedLinks = append(sortedLinks, link)
	}
	sort.Strings(sortedLinks)
	return sortedLinks
}

// A regular expression to find cross links in comments.
//
// The Google API documentation (typically in protos) include links to code
// elements in the form `[Thing][google.package.blah.v1.Thing.SubThing]`.
// This regular expression captures the `][...]` part. There is a lot of scaping
// because the brackets are metacharacters in regex.
var commentCrossReferenceLink = regexp.MustCompile(
	`` + // `go fmt` is annoying
		`\]` + // The closing bracket for the `[Thing]`
		`\[` + // The opening bracket for the code element.
		`[A-Za-z][A-Za-z0-9_]*` + // A thing that looks like a Protobuf identifier
		`(\.` + // Followed by (maybe a dot)
		`[A-Za-z][A-Za-z0-9_]*` + // A thing that looks like a Protobuf identifier
		`)*` + // zero or more times
		`\]`) // The closing bracket

// A regular expression to find implied cross reference links.
var commentImpliedCrossReferenceLink = regexp.MustCompile(
	`` + // `go fmt` is annoying
		`\[` +
		`[A-Z-a-z][A-Za-z0-9_]*` + // A thing that looks like a Protobuf identifier
		`(\.[A-Za-z][A-Za-z0-9_]*)*` + // Followed by more identifiers
		`\]\[\]`) // The closing bracket followed by an empty link label

func extractProtoLinks(paragraph []byte, links map[string]bool) {
	for _, match := range commentCrossReferenceLink.FindAll(paragraph, -1) {
		match = bytes.TrimSuffix(bytes.TrimPrefix(match, []byte("][")), []byte("]"))
		links[string(match)] = true
	}
	for _, match := range commentImpliedCrossReferenceLink.FindAll(paragraph, -1) {
		match = bytes.TrimSuffix(bytes.TrimPrefix(match, []byte("[")), []byte("][]"))
		links[string(match)] = true
	}
}

func processCommentLine(node ast.Node, line text.Segment, documentationBytes []byte) string {
	lineString := escapeHTMLTags(node, line, documentationBytes)
	lineString = escapeUrls(lineString)
	return lineString
}

func escapeHTMLTags(node ast.Node, line text.Segment, documentationBytes []byte) string {
	lineContent := line.Value(documentationBytes)
	escapedString := string(lineContent)
	for child := node.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() == ast.KindRawHTML {
			rawHTML := child.(*ast.RawHTML)
			if !isWithinCodeSpan(node) {
				for i := 0; i < rawHTML.Segments.Len(); i++ {
					segment := rawHTML.Segments.At(i)
					segmentContent := string(segment.Value(documentationBytes))
					if segment.Start < line.Start || (segment.Start >= line.Stop) {
						continue
					}
					if strings.HasPrefix(segmentContent, "<br />") || isHyperlink(segment, documentationBytes) {
						continue
					}
					start := int(segment.Start) - line.Start
					end := int(segment.Stop) - line.Start
					escapedHTML := strings.Replace(segmentContent, "<", "\\<", 1)
					escapedHTML = strings.Replace(escapedHTML, ">", "\\>", 1)
					escapedString = strings.ReplaceAll(escapedString, string(lineContent[start:end]), escapedHTML)

				}
			}
		}
	}
	return escapedString
}

func isHyperlink(segment text.Segment, documentationBytes []byte) bool {
	segmentContent := string(segment.Value(documentationBytes))
	if strings.Contains(segmentContent, "href=") || strings.HasSuffix(segmentContent, "</a>") {
		return true
	}
	// Verify for hyperlink that spans multiple lines
	if strings.HasSuffix(string(segment.Value(documentationBytes)), "<a\n") {
		// Check if the next 7 bytes (or more) in documentationBytes start with " href="
		nextBytesStart := int(segment.Stop)
		nextBytesEnd := nextBytesStart + 7
		trimmedNextBytes := strings.TrimSpace(string(documentationBytes[nextBytesStart:nextBytesEnd]))
		return nextBytesEnd <= len(documentationBytes) && strings.HasPrefix(trimmedNextBytes, "href=")
	}
	return false
}

func isWithinCodeSpan(node ast.Node) bool {
	for parent := node.Parent(); parent != nil; parent = parent.Parent() {
		if parent.Kind() == ast.KindCodeSpan {
			return true
		}
	}
	return false
}

// Encloses standalone URLs with angled brackets and escape placeholders.
func escapeUrls(line string) string {
	var escapedLine strings.Builder
	lastIndex := 0

	for _, match := range commentUrlRegex.FindAllStringIndex(line, -1) {
		if isLinkDestination(line, match[0], match[1]) {
			escapedLine.WriteString(line[lastIndex:match[1]])
			lastIndex = match[1]
			continue
		}
		url := line[match[0]:match[1]]
		prefix := line[:match[0]]
		suffix := line[match[1]:]

		if strings.HasSuffix(prefix, "<") && strings.HasPrefix(suffix, ">") {
			// Skip adding <> if the url is already surrounded by angled brackets.
			escapedLine.WriteString(line[lastIndex:match[1]])
			lastIndex = match[1]
		} else if strings.Contains(line[lastIndex:match[0]], "href=") {
			// The url is in a hyperlink, leave it as-is
			escapedLine.WriteString(line[lastIndex:match[1]])
			lastIndex = match[1]
		} else if strings.HasSuffix(line[lastIndex:match[0]], `"`) && strings.HasPrefix(line[match[1]:], `"`) {
			// The URL is in quotes `"`, escape it to appear as verbatim text.
			escapedLine.WriteString(line[lastIndex : match[0]-1])
			escapedLine.WriteString(fmt.Sprintf("`%s`", url))
			lastIndex = match[1] + 1
		} else if strings.HasSuffix(prefix, "]: ") && (suffix == "\n" || suffix == "") {
			// Looks line a link definition, just leave it as-is
			escapedLine.WriteString(line[lastIndex:match[1]])
			lastIndex = match[1]
		} else {
			escapedLine.WriteString(line[lastIndex:match[0]])
			if strings.HasSuffix(url, ".") {
				escapedLine.WriteString(fmt.Sprintf("<%s>.", strings.TrimSuffix(url, ".")))
			} else {
				escapedLine.WriteString(fmt.Sprintf("<%s>", url))
			}
			lastIndex = match[1]
		}

	}
	escapedLine.WriteString(line[lastIndex:])
	return escapedLine.String()
}

// Verifies whether the url is part of a link destination.
func isLinkDestination(line string, matchStart, matchEnd int) bool {
	return strings.HasSuffix(line[:matchStart], "](") && line[matchEnd] == ')'
}

func processList(list *ast.List, indentLevel int, documentationBytes []byte, elementID string) []string {
	var results []string
	listMarker := string(list.Marker)
	if list.IsOrdered() {
		listMarker = "1."
	}
	for child := list.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() == ast.KindListItem {
			listItems := processListItem(child.(*ast.ListItem), indentLevel, listMarker, documentationBytes, elementID)
			results = append(results, listItems...)
		}
	}
	return results
}

func processListItem(listItem *ast.ListItem, indentLevel int, listMarker string, documentationBytes []byte, elementID string) []string {
	var markerIndent int
	switch len(listMarker) {
	case 1:
		markerIndent = 2
	case 2:
		markerIndent = 3
	default:
		markerIndent = 2
	}
	var results []string
	paragraphStart := listMarker
	for child := listItem.FirstChild(); child != nil; child = child.NextSibling() {
		if child.Kind() == ast.KindListItem {
			paragraphStart = listMarker
		}
		if child.Kind() == ast.KindList {
			nestedListItems := processList(child.(*ast.List), indentLevel+markerIndent, documentationBytes, elementID)
			results = append(results, nestedListItems...)
			break
		}
		if child.Kind() == ast.KindParagraph || child.Kind() == ast.KindTextBlock {
			if child.Lines().Len() == 0 {
				// This indicates a bug in the documentation that should be
				// fixed upstream. We continue despite the error because missing
				// a small bit of documentation is better than not generating
				// the full library.
				slog.Warn("ignoring empty list item", "element", elementID)
			}
			for i := 0; i < child.Lines().Len(); i++ {
				line := child.Lines().At(i)
				results = append(results, fmt.Sprintf("%s%s %s\n", indent(indentLevel), paragraphStart, processCommentLine(child, line, documentationBytes)))
				paragraphStart = fmt.Sprintf("%*s", len(listMarker), "")
			}
			if child.Kind() == ast.KindParagraph {
				results = append(results, "\n")
			}
		}
	}
	return results
}

func indent(level int) string {
	return fmt.Sprintf("%*s", level, "")
}

func annotateCodeBlock(node ast.Node, documentationBytes []byte) []string {
	codeBlock := node.(*ast.CodeBlock)
	var results []string
	results = append(results, "```norust")
	for i := 0; i < codeBlock.Lines().Len(); i++ {
		line := codeBlock.Lines().At(i)
		results = append(results, string(line.Value(documentationBytes)))
	}
	results = append(results, "```")
	results = append(results, "\n")
	return results
}

func annotateFencedCodeBlock(node ast.Node, documentationBytes []byte) []string {
	var results []string
	fencedCode := node.(*ast.FencedCodeBlock)
	results = append(results, "```norust")
	for i := 0; i < fencedCode.Lines().Len(); i++ {
		line := fencedCode.Lines().At(i)
		results = append(results, string(line.Value(documentationBytes)))
	}
	results = append(results, "```")
	results = append(results, "\n")
	return results
}

func processParagraph(node ast.Node, documentationBytes []byte) []string {
	var results []string
	var allLinkDefinitions []string
	for i := 0; i < node.Lines().Len(); i++ {
		line := node.Lines().At(i)
		lineString := string(line.Value(documentationBytes))
		results = append(results, processCommentLine(node, line, documentationBytes))
		linkDefinitions := fetchLinkDefinitions(node, lineString, documentationBytes)
		allLinkDefinitions = append(allLinkDefinitions, linkDefinitions...)
	}

	if len(allLinkDefinitions) > 0 {
		results = append(results, "\n")
		results = append(results, allLinkDefinitions...)
	}
	results = append(results, "\n")
	return results
}

func fetchLinkDefinitions(node ast.Node, line string, documentationBytes []byte) []string {
	var linkDefinitions []string
	for c := node.FirstChild(); c != nil; c = c.NextSibling() {
		if c.Kind() == ast.KindLink {
			link := c.(*ast.Link)
			var linkText strings.Builder
			for l := link.FirstChild(); l != nil; l = l.NextSibling() {
				if l.Kind() == ast.KindText {
					linkText.WriteString(string(l.(*ast.Text).Value(documentationBytes)))
					linkText.WriteString(" ")
				}
			}

			// Add link definitions for collapsed reference links.
			trimmedLinkText := strings.TrimSuffix(linkText.String(), " ")
			re := regexp.MustCompile(`\[(.*?)\]\[\]`)
			match := re.FindStringSubmatch(line)
			if len(match) > 1 {
				text := match[1]
				if text == trimmedLinkText {
					linkDefinitions = append(linkDefinitions, fmt.Sprintf("[%s]:", trimmedLinkText))
					linkDefinitions = append(linkDefinitions, fmt.Sprintf(" %s", string(link.Destination)))
				}
			}
		}
	}
	return linkDefinitions
}

func (c *codec) docLink(link string, state *api.APIState, scopes []string) string {
	// Sometimes the documentation uses relative links, so instead of saying:
	//     [google.package.v1.Message]
	// they just say
	//     [Message]
	// we need to lookup the local symbols first.
	for _, s := range scopes {
		localId := fmt.Sprintf(".%s.%s", s, link)
		result := c.tryDocLinkWithId(localId, state, s)
		if result != "" {
			return result
		}
	}
	packageName := ""
	if len(scopes) > 0 {
		packageName = scopes[len(scopes)-1]
	}
	localId := fmt.Sprintf(".%s", link)
	return c.tryDocLinkWithId(localId, state, packageName)
}

func (c *codec) tryDocLinkWithId(id string, state *api.APIState, scope string) string {
	m, ok := state.MessageByID[id]
	if ok {
		return fullyQualifiedMessageName(m, c.modulePath, scope, c.packageMapping)
	}
	e, ok := state.EnumByID[id]
	if ok {
		return fullyQualifiedEnumName(e, c.modulePath, scope, c.packageMapping)
	}
	me, ok := state.MethodByID[id]
	if ok {
		return c.methodRustdocLink(me, state)
	}
	s, ok := state.ServiceByID[id]
	if ok {
		return c.serviceRustdocLink(s)
	}
	rdLink := c.tryFieldRustdocLink(id, state, scope)
	if rdLink != "" {
		return rdLink
	}
	rdLink = c.tryEnumValueRustdocLink(id, state, scope)
	if rdLink != "" {
		return rdLink
	}
	return ""
}

func (c *codec) tryFieldRustdocLink(id string, state *api.APIState, scope string) string {
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
				return fmt.Sprintf("%s::%s", fullyQualifiedMessageName(m, c.modulePath, scope, c.packageMapping), toSnakeNoMangling(f.Name))
			} else {
				return c.tryOneOfRustdocLink(f, m, scope)
			}
		}
	}
	for _, o := range m.OneOfs {
		if o.Name == fieldName {
			return fmt.Sprintf("%s::%s", fullyQualifiedMessageName(m, c.modulePath, scope, c.packageMapping), toSnakeNoMangling(o.Name))
		}
	}
	return ""
}

func (c *codec) tryOneOfRustdocLink(field *api.Field, message *api.Message, scope string) string {
	for _, o := range message.OneOfs {
		for _, f := range o.Fields {
			if f.ID == field.ID {
				return fmt.Sprintf("%s::%s", fullyQualifiedMessageName(message, c.modulePath, scope, c.packageMapping), toSnakeNoMangling(o.Name))
			}
		}
	}
	return ""
}

func (c *codec) tryEnumValueRustdocLink(id string, state *api.APIState, scope string) string {
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
			return fullyQualifiedEnumValueName(v, c.modulePath, scope, c.packageMapping)
		}
	}
	return ""
}

func (c *codec) methodRustdocLink(m *api.Method, state *api.APIState) string {
	// Sometimes we remove methods from a service. In that case we cannot
	// reference the method.
	if !c.generateMethod(m) {
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
	return fmt.Sprintf("%s::%s", c.serviceRustdocLink(s), toSnake(m.Name))
}

func (c *codec) serviceRustdocLink(s *api.Service) string {
	mapped, ok := c.packageMapping[s.Package]
	if ok {
		return fmt.Sprintf("%s::client::%s", mapped.name, toPascal(s.Name))
	}
	return fmt.Sprintf("crate::client::%s", toPascal(s.Name))
}

func usePackage(source string, model *api.API, c *codec) {
	mapped, ok := c.packageMapping[source]
	if ok && source != model.PackageName {
		mapped.used = true
	}
}

func findUsedPackagesMessage(message *api.Message, model *api.API, c *codec, visited map[string]bool) {
	if _, ok := visited[message.ID]; ok {
		return
	}
	visited[message.ID] = true
	usePackage(message.Package, model, c)
	for _, e := range message.Enums {
		usePackage(e.Package, model, c)
	}
	for _, m := range message.Messages {
		findUsedPackagesMessage(m, model, c, visited)
	}
	for _, f := range message.Fields {
		switch f.Typez {
		case api.MESSAGE_TYPE:
			if fm, ok := model.State.MessageByID[f.TypezID]; ok {
				usePackage(fm.Package, model, c)
			}
		case api.ENUM_TYPE:
			if fe, ok := model.State.EnumByID[f.TypezID]; ok {
				usePackage(fe.Package, model, c)
			}
		}
	}
}

func findUsedPackages(model *api.API, c *codec) {
	for _, message := range model.Messages {
		findUsedPackagesMessage(message, model, c, map[string]bool{})
	}
	for _, enum := range model.Enums {
		usePackage(enum.Package, model, c)
	}
	for _, s := range model.Services {
		for _, method := range s.Methods {
			if m, ok := model.State.MessageByID[method.InputTypeID]; ok {
				findUsedPackagesMessage(m, model, c, map[string]bool{})
			}
			if m, ok := model.State.MessageByID[method.OutputTypeID]; ok {
				usePackage(m.Package, model, c)
			}
			if method.OperationInfo != nil {
				if m, ok := model.State.MessageByID[method.OperationInfo.MetadataTypeID]; ok {
					usePackage(m.Package, model, c)
				}
				if m, ok := model.State.MessageByID[method.OperationInfo.ResponseTypeID]; ok {
					usePackage(m.Package, model, c)
				}
			}
		}
	}
}

func requiredPackageLine(pkg *packagez) string {
	if len(pkg.features) > 0 {
		feats := strings.Join(language.MapSlice(pkg.features, func(s string) string { return fmt.Sprintf("%q", s) }), ", ")
		return fmt.Sprintf("%-20s = { workspace = true, features = [%s] }", pkg.name, feats)
	}
	return fmt.Sprintf("%-20s = true", pkg.name+".workspace")
}

func requiredPackages(extraPackages []*packagez) []string {
	lines := []string{}
	for _, pkg := range extraPackages {
		if pkg.ignore {
			continue
		}
		if !pkg.used {
			continue
		}
		lines = append(lines, requiredPackageLine(pkg))
	}
	sort.Strings(lines)
	return lines
}

func externPackages(extraPackages []*packagez) []string {
	names := []string{}
	for _, pkg := range extraPackages {
		if pkg.ignore || !pkg.used {
			continue
		}
		names = append(names, strings.ReplaceAll(pkg.name, "-", "_"))
	}
	sort.Strings(names)
	return names
}

func PackageName(api *api.API, packageNameOverride string) string {
	if len(packageNameOverride) > 0 {
		return packageNameOverride
	}
	name := strings.TrimPrefix(api.PackageName, "google.cloud.")
	name = strings.TrimPrefix(name, "google.")
	name = strings.ReplaceAll(name, ".", "-")
	if name == "" {
		name = api.Name
	}
	return "google-cloud-" + name
}

func (c *codec) ServiceName(service *api.Service) string {
	if override, ok := c.nameOverrides[service.ID]; ok {
		return override
	}
	return service.Name
}

func (c *codec) OneOfEnumName(oneof *api.OneOf) string {
	if override, ok := c.nameOverrides[oneof.ID]; ok {
		return override
	}
	return toPascal(oneof.Name)
}

func (c *codec) generateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations, we cannot generate working
	// RPCs for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	if m.ClientSideStreaming || m.ServerSideStreaming {
		return false
	}
	if c.includeGrpcOnlyMethods {
		return true
	}
	if m.PathInfo == nil || len(m.PathInfo.Bindings) == 0 {
		return false
	}
	return len(m.PathInfo.Bindings[0].PathTemplate) != 0
}

// The list of Rust keywords and reserved words can be found at:
//
//	https://doc.rust-lang.org/reference/keywords.html
func escapeKeyword(symbol string) string {
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
