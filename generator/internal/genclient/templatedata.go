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

package genclient

import (
	"strings"

	"github.com/iancoleman/strcase"
)

// newTemplateData returns a struct that is used as input to the mustache
// templates. Methods on the types defined in this file are directly associated
// with the mustache tags. For instances the mustache tag {{#Services}} calls
// the [templateData.Services] method. templateData uses the raw input of the
// [API] and uses a [lang.Codec] to transform the input into language
// idiomatic representations.
func newTemplateData(model *API, codec LanguageCodec) *templateData {
	codec.LoadWellKnownTypes(model.State)
	return &templateData{
		s: model,
		c: codec,
	}
}

type templateData struct {
	s *API
	c LanguageCodec
}

func (t *templateData) Services() []*service {
	return mapSlice(t.s.Services, func(s *Service) *service {
		return &service{
			s:     s,
			c:     t.c,
			state: t.s.State,
		}
	})
}

func (t *templateData) Messages() []*message {
	return mapSlice(t.s.Messages, func(m *Message) *message {
		return &message{
			s:     m,
			c:     t.c,
			state: t.s.State,
		}
	})
}

func (t *templateData) NameToLower() string {
	return strings.ToLower(t.s.Name)
}

// service represents a service in an API.
type service struct {
	s     *Service
	c     LanguageCodec
	state *APIState
}

func (s *service) Methods() []*method {
	return mapSlice(s.s.Methods, func(m *Method) *method {
		return &method{
			s:     m,
			c:     s.c,
			state: s.state,
		}
	})
}

// NameToSnake converts Name to snake_case.
func (s *service) NameToSnake() string {
	return s.c.ToSnake(s.s.Name)
}

// NameToPascanl converts a Name to PascalCase.
func (s *service) NameToPascal() string {
	return s.ServiceNameToPascal()
}

// NameToPascal converts a Name to PascalCase.
func (s *service) ServiceNameToPascal() string {
	return s.c.ToPascal(s.s.Name)
}

// NameToCamel coverts Name to camelCase
func (s *service) NameToCamel() string {
	return s.c.ToCamel(s.s.Name)
}

func (s *service) DocLines() []string {
	return strings.Split(s.s.Documentation, "\n")
}

func (s *service) DefaultHost() string {
	return s.s.DefaultHost
}

// method defines a RPC belonging to a Service.
type method struct {
	s     *Method
	c     LanguageCodec
	state *APIState
}

// NameToSnake converts a Name to snake_case.
func (m *method) NameToSnake() string {
	return strcase.ToSnake(m.s.Name)
}

// NameToCamel converts a Name to camelCase.
func (m *method) NameToCamel() string {
	return strcase.ToCamel(m.s.Name)
}

func (m *method) DocLines() []string {
	return strings.Split(m.s.Documentation, "\n")
}

func (m *method) InputTypeName() string {
	return m.c.MethodInOutTypeName(m.s.InputTypeID, m.state)
}

func (m *method) OutputTypeName() string {
	return m.c.MethodInOutTypeName(m.s.OutputTypeID, m.state)
}

func (m *method) HTTPMethod() string {
	return m.s.HTTPInfo.Method
}

func (m *method) HTTPMethodToLower() string {
	return strings.ToLower(m.s.HTTPInfo.Method)
}

func (m *method) HTTPPathFmt() string {
	return m.c.HTTPPathFmt(m.s.HTTPInfo, m.state)
}

func (m *method) HTTPPathArgs() []string {
	return m.c.HTTPPathArgs(m.s.HTTPInfo, m.state)
}

func (m *method) QueryParams() []*Pair {
	return m.c.QueryParams(m.s, m.state)
}

func (m *method) HasBody() bool {
	return m.s.HTTPInfo.Body != ""
}

func (m *method) BodyAccessor() string {
	return m.c.BodyAccessor(m.s, m.state)
}

// message defines a message used in request or response handling.
type message struct {
	s     *Message
	c     LanguageCodec
	state *APIState
}

func (m *message) Fields() []*field {
	return mapSlice(m.s.Fields, func(s *Field) *field {
		return &field{
			s:     s,
			c:     m.c,
			state: m.state,
		}
	})
}

func (m *message) NestedMessages() []*message {
	return mapSlice(m.s.Messages, func(s *Message) *message {
		return &message{
			s:     s,
			c:     m.c,
			state: m.state,
		}
	})
}

func (m *message) Enums() []*enum {
	return mapSlice(m.s.Enums, func(s *Enum) *enum {
		return &enum{
			s:     s,
			c:     m.c,
			state: m.state,
		}
	})
}

// NameToCamel converts a Name to CamelCase.
func (m *message) Name() string {
	return m.c.MessageName(m.s, m.state)
}

func (m *message) DocLines() []string {
	// TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/33
	ss := strings.Split(m.s.Documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimSpace(ss[i])
	}
	return ss
}

func (m *message) IsMap() bool {
	return m.s.IsMap
}

type enum struct {
	s     *Enum
	c     LanguageCodec
	state *APIState
}

func (e *enum) Name() string {
	return e.c.EnumName(e.s, e.state)
}

func (e *enum) DocLines() []string {
	ss := strings.Split(e.s.Documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimSpace(ss[i])
	}
	return ss
}

func (e *enum) Values() []*enumValue {
	return mapSlice(e.s.Values, func(s *EnumValue) *enumValue {
		return &enumValue{
			s:     s,
			e:     e.s,
			c:     e.c,
			state: e.state,
		}
	})
}

type enumValue struct {
	s     *EnumValue
	e     *Enum
	c     LanguageCodec
	state *APIState
}

func (e *enumValue) DocLines() []string {
	ss := strings.Split(e.s.Documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimSpace(ss[i])
	}
	return ss
}

func (e *enumValue) Name() string {
	return e.c.EnumValueName(e.s, e.state)
}

func (e *enumValue) Number() int32 {
	return e.s.Number
}

func (e *enumValue) EnumType() string {
	return e.c.EnumName(e.e, e.state)
}

// field defines a field in a Message.
type field struct {
	s     *Field
	c     LanguageCodec
	state *APIState
}

// NameToSnake converts a Name to snake_case.
func (f *field) NameToSnake() string {
	return f.c.ToSnake(f.s.Name)
}

// NameToCamel converts a Name to camelCase.
func (f *field) NameToCamel() string {
	return f.c.ToCamel(f.s.Name)
}

func (f *field) DocLines() []string {
	return strings.Split(f.s.Documentation, "\n")
}

func (f *field) FieldType() string {
	return f.c.FieldType(f.s, f.state)
}

func (f *field) JSONName() string {
	return f.s.JSONName
}

func mapSlice[T, R any](s []T, f func(T) R) []R {
	r := make([]R, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}
