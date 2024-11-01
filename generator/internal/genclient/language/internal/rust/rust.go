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

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/iancoleman/strcase"
)

func NewCodec() *Codec {
	return &Codec{}
}

type Codec struct{}

func (c *Codec) LoadWellKnownTypes(s *genclient.APIState) {
	timestamp := &genclient.Message{
		ID:   ".google.protobuf.Timestamp",
		Name: "String",
	}
	duration := &genclient.Message{
		ID:   ".google.protobuf.Duration",
		Name: "String",
	}
	s.MessageByID[timestamp.ID] = timestamp
	s.MessageByID[duration.ID] = duration
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
	if f.Optional {
		return fmt.Sprintf("Option<%s>", out)
	}
	return out
}

func (c *Codec) FieldType(f *genclient.Field, state *genclient.APIState) string {
	if f.Typez == genclient.MESSAGE_TYPE {
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			key := c.FieldType(m.Fields[0], state)
			val := c.FieldType(m.Fields[1], state)
			return "Option<std::collections::HashMap<" + key + "," + val + ">>"
		}
		return "Option<" + c.MessageName(m, state) + ">"
	} else if f.Typez == genclient.ENUM_TYPE {
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		return c.EnumName(e, state)
	} else if f.Typez == genclient.GROUP_TYPE {
		slog.Error("TODO(#39) - better handling of `oneof` fields")
		return ""
	}
	return ScalarFieldType(f)
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
	if m.Parent != nil {
		return c.MessageName(m.Parent, state) + "_" + strcase.ToCamel(m.Name)
	}
	if m.Package != "" {
		return m.Package + "." + strcase.ToCamel(m.Name)
	}
	return c.ToPascal(m.Name)
}

func (c *Codec) EnumName(e *genclient.Enum, state *genclient.APIState) string {
	if e.Parent != nil {
		return c.MessageName(e.Parent, state) + "_" + strcase.ToCamel(e.Name)
	}
	return c.ToPascal(e.Name)
}

func (c *Codec) EnumValueName(e *genclient.EnumValue, state *genclient.APIState) string {
	if e.Parent.Parent != nil {
		return c.MessageName(e.Parent.Parent, state) + "_" + strcase.ToCamel(e.Name)
	}
	return c.ToPascal(e.Name)
}

func (c *Codec) BodyAccessor(m *genclient.Method, state *genclient.APIState) string {
	if m.HTTPInfo.Body == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + c.ToSnake(m.HTTPInfo.Body)
}

func (c *Codec) HTTPPathFmt(m *genclient.HTTPInfo, state *genclient.APIState) string {
	return genclient.HTTPPathVarRegex.ReplaceAllStringFunc(m.RawPath, func(s string) string { return "{}" })
}

func (c *Codec) HTTPPathArgs(h *genclient.HTTPInfo, state *genclient.APIState) []string {
	var args []string
	rawArgs := h.PathArgs()
	for _, arg := range rawArgs {
		args = append(args, "req."+c.ToSnake(arg))
	}
	return args
}

func (c *Codec) QueryParams(m *genclient.Method, state *genclient.APIState) []*genclient.Pair {
	notQuery := m.NotQueryParams()
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup type", "id", m.InputTypeID)
		return nil
	}

	var queryParams []*genclient.Pair
	for _, field := range msg.Fields {
		if field.JSONName != "" && !notQuery[field.JSONName] {
			queryParams = append(queryParams, &genclient.Pair{Key: field.JSONName, Value: "req." + c.ToSnake(field.JSONName) + ".as_str()"})
		}
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
