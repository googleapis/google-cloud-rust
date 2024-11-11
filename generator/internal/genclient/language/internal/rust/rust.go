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
	"strings"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/iancoleman/strcase"
)

func NewCodec() *Codec {
	return &Codec{}
}

type Codec struct{}

func (c *Codec) LoadWellKnownTypes(s *genclient.APIState) {
	// TODO(#77) - replace these placeholders with real types
	wellKnown := []*genclient.Message{
		{
			ID:   ".google.protobuf.Any",
			Name: "serde_json::Value",
		},
		{
			ID:   ".google.protobuf.FieldMask",
			Name: "gax_placeholder::FieldMask",
		},
		{
			ID:   ".google.protobuf.Duration",
			Name: "gax_placeholder::Duration",
		},
		{
			ID:   ".google.protobuf.Timestamp",
			Name: "gax_placeholder::Timestamp",
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
		if strings.HasPrefix(m.ID, ".google.protobuf.") {
			// TODO(#77): Better handling of well-known types
			return m.Name
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

func (c *Codec) TemplateDir() string {
	return "rust"
}

func (c *Codec) MethodInOutTypeName(id string, s *genclient.APIState) string {
	if id == "" {
		return ""
	}
	m, ok := s.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return c.ToPascal(m.Name)
}

func (c *Codec) MessageName(m *genclient.Message, state *genclient.APIState) string {
	if m.Package != "" {
		return m.Package + "." + c.ToPascal(m.Name)
	}
	return c.ToPascal(m.Name)
}

func (c *Codec) messageScopeName(m *genclient.Message) string {
	if m == nil {
		return "crate::model"
	}
	return c.messageScopeName(m.Parent) + "::" + c.ToSnake(m.Name)
}

func (c *Codec) enumScopeName(e *genclient.Enum) string {
	return c.messageScopeName(e.Parent)
}

func (c *Codec) FQMessageName(m *genclient.Message, _ *genclient.APIState) string {
	return c.messageScopeName(m.Parent) + "::" + c.ToPascal(m.Name)
}

func (c *Codec) EnumName(e *genclient.Enum, state *genclient.APIState) string {
	return c.ToPascal(e.Name)
}

func (c *Codec) FQEnumName(e *genclient.Enum, _ *genclient.APIState) string {
	return c.messageScopeName(e.Parent) + "::" + c.ToPascal(e.Name)
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
	return c.messageScopeName(o.Parent) + "::" + c.ToPascal(o.Name)
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
	return fmt.Sprintf("gax::path_parameter::PathParameter::required(%s, \"%s\")?.%s", unwrap, name, last), ""
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

func (c *Codec) QueryParams(m *genclient.Method, state *genclient.APIState) []*genclient.Pair {
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup request type", "id", m.InputTypeID)
		return nil
	}

	var queryParams []*genclient.Pair
	for _, field := range msg.Fields {
		if !m.PathInfo.QueryParameters[field.Name] {
			continue
		}
		queryParams = append(queryParams, &genclient.Pair{
			Key:   field.JSONName,
			Value: c.ToSnake(field.Name)})
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
