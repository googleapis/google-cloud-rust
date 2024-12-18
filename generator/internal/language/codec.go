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

import "github.com/googleapis/google-cloud-rust/generator/internal/api"

// Codec is an adapter used to transform values into language idiomatic
// representations. This is used to manipulate the data the is fed into
// templates to generate clients.
type Codec interface {
	// TemplatesProvider returns a function that loads templates from whatever
	// filesystem the Codec is using.
	TemplatesProvider() TemplateProvider
	// GeneratedFiles returns the list of input templates and output files.
	GeneratedFiles() []GeneratedFile
	// LoadWellKnownTypes allows a language to load information into the state
	// for any wellknown types. For example defining how timestamppb should be
	// represented in a given language or wrappers around operations.
	LoadWellKnownTypes(s *api.APIState)
	// FieldAttributes returns a (possibly empty) list of "attributes" included
	// immediately before the field definition.
	FieldAttributes(f *api.Field, state *api.APIState) []string
	// FieldType returns a string representation of a message field type.
	FieldType(f *api.Field, state *api.APIState) string
	// The field when used to build the query.
	AsQueryParameter(f *api.Field, state *api.APIState) string
	// The name of a message type ID when used as an input or output argument
	// in the client methods.
	MethodInOutTypeName(id string, state *api.APIState) string
	// Returns a (possibly empty) list of "attributes" included immediately
	// before the message definition.
	MessageAttributes(m *api.Message, state *api.APIState) []string
	// The (unqualified) message name, as used when defining the type to
	// represent it.
	MessageName(m *api.Message, state *api.APIState) string
	// The fully-qualified message name, as used when referring to the name from
	// another place in the package.
	FQMessageName(m *api.Message, state *api.APIState) string
	// The (unqualified) enum name, as used when defining the type to
	// represent it.
	EnumName(e *api.Enum, state *api.APIState) string
	// The fully-qualified enum name, as used when referring to the name from
	// another place in the package.
	FQEnumName(e *api.Enum, state *api.APIState) string
	// The (unqualified) enum value name, as used when defining the constant,
	// variable, or enum value that holds it.
	EnumValueName(e *api.EnumValue, state *api.APIState) string
	// The fully qualified enum value name, as used when using the constant,
	// variable, or enum value that holds it.
	FQEnumValueName(e *api.EnumValue, state *api.APIState) string
	// OneOfType returns a string representation of a one-of field type.
	OneOfType(o *api.OneOf, state *api.APIState) string
	// BodyAccessor returns a string representation of the accessor used to
	// get the body out of a request. For instance this might return `.Body()`.
	BodyAccessor(m *api.Method, state *api.APIState) string
	// HTTPPathFmt returns a format string used for adding path arguments to a
	// URL. The replacements should align in both order and value from what is
	// returned from HTTPPathArgs.
	HTTPPathFmt(m *api.PathInfo, state *api.APIState) string
	// HTTPPathArgs returns a string representation of the path arguments. This
	// should be used in conjunction with HTTPPathFmt. An example return value
	// might be `, req.PathParam()`
	HTTPPathArgs(h *api.PathInfo, state *api.APIState) []string
	// QueryParams returns key-value pairs of name to accessor for query params.
	// An example return value might be
	// `&Pair{Key: "secretId", Value: "req.SecretId()"}`
	QueryParams(m *api.Method, state *api.APIState) []*api.Field
	// ToSnake converts a symbol name to `snake_case`, applying any mangling
	// required by the language, e.g., to avoid clashes with reserved words.
	ToSnake(string) string
	// ToSnakeNoMangling converts a symbol name to `snake_case`, without any
	// mangling to avoid reserved words. This is useful when the template is
	// already going to mangle the name, e.g., by adding a prefix or suffix.
	// Since the templates are language specific, their authors can determine
	// when to use `ToSnake` or `ToSnakeNoMangling`.
	ToSnakeNoMangling(string) string
	// ToPascal converts a symbol name to `PascalCase`, applying any mangling
	// required by the language, e.g., to avoid clashes with reserved words.
	ToPascal(string) string
	// ToCamel converts a symbol name to `camelCase` (sometimes called
	// "lowercase CamelCase"), applying any mangling required by the language,
	// e.g., to avoid clashes with reserved words.
	ToCamel(string) string
	// Reformat ${Lang}Doc comments according to the language-specific rules.
	// For example,
	//   - The protos in googleapis include cross-references in the format
	//     `[Foo][proto.package.name.Foo]`, this should become links to the
	//     language entities, in the language documentation.
	//   - Rust requires a `norust` annotation in all blockquotes, that is,
	//     any ```-sections. Without this annotation Rustdoc assumes the
	//     blockquote is an Rust code snippet and attempts to compile it.
	FormatDocComments(string, *api.APIState) []string
	// Returns a extra set of lines to insert in the module file.
	// The format of these lines is specific to each language.
	RequiredPackages() []string
	// The package name in the destination language. May be empty, some
	// languages do not have a package manager.
	PackageName(api *api.API) string
	// Some languages need a package version.
	PackageVersion() string
	// Validate an API, some codecs impose restrictions on the input API.
	Validate(api *api.API) error
	// The year when this package was first generated.
	CopyrightYear() string
	// Pass language-specific information from the Codec to the template engine.
	// Prefer using specific methods when the information is applicable to most
	// (or many) languages. Use this method when the information is application
	// to only one language.
	AdditionalContext(api *api.API) any
	// Imports to add.
	Imports() []string
	// Some packages are not intended for publication. For example, they may be
	// intended only for testing the generator or the SDK, or the service may
	// not be GA.
	NotForPublication() bool
}

// This represents an input template and its corresponding output file.
type GeneratedFile struct {
	// The name of the template file, relative to the Codec's filesystem root.
	TemplatePath string
	// The name of the output file, relative to the output directory.
	OutputPath string
}

// A provider for Mustache template contents.
//
// The function is expected to accept a template name, including its full path
// and the `.mustache` extension, such as `rust/crate/src/lib.rs.mustach` and
// tuen return the full contents of the template (or an error).
type TemplateProvider func(templateName string) (string, error)
