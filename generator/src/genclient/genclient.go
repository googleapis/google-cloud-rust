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

// Package genclient is a Schema and Language agnostic code generator that applies
// an API model to a mustache template.
package genclient

import (
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cbroglie/mustache"
)

// LanguageCodec is an adapter used to transform values into language idiomatic
// representations. This is used to manipulate the data the is fed into
// templates to generate clients.
type LanguageCodec interface {
	// TemplateDir returns the directory containing the templates.
	TemplateDir() string
	// LoadWellKnownTypes allows a language to load information into the state
	// for any wellknown types. For example defining how timestamppb should be
	// represented in a given language or wrappers around operations.
	LoadWellKnownTypes(s *APIState)
	// FieldType returns a string representation of a message field type.
	FieldType(f *Field, state *APIState) string
	MethodInOutTypeName(id string, state *APIState) string
	// MessageName returns a string representation of a message name.
	MessageName(m *Message, state *APIState) string
	// EnumName returns a string representation of an enum name.
	EnumName(e *Enum, state *APIState) string
	// EnumValueName returns a string representation of an enum value name.
	EnumValueName(e *EnumValue, state *APIState) string
	// BodyAccessor returns a string representation of the accessor used to
	// get the body out of a request. For instance this might return `.Body()`.
	BodyAccessor(m *Method, state *APIState) string
	// HTTPPathFmt returns a format string used for adding path arguments to a
	// URL. The replacements should align in both order and value from what is
	// returned from HTTPPathArgs.
	HTTPPathFmt(m *HTTPInfo, state *APIState) string
	// HTTPPathArgs returns a string representation of the path arguments. This
	// should be used in conjunction with HTTPPathFmt. An example return value
	// might be `, req.PathParam()`
	HTTPPathArgs(h *HTTPInfo, state *APIState) []string
	// QueryParams returns key-value pairs of name to accessor for query params.
	// An example return value might be
	// `&Pair{Key: "secretId", Value: "req.SecretId()"}`
	QueryParams(m *Method, state *APIState) []*Pair
}

// GenerateRequest used to generate clients.
type GenerateRequest struct {
	// The in memory representation of a parsed input.
	API *API
	// An adapter to transform values into language idiomatic representations.
	Codec LanguageCodec
	// OutDir is the path to the output directory.
	OutDir string
	// Template directory
	TemplateDir string
}

func (r *GenerateRequest) outDir() string {
	if r.OutDir == "" {
		wd, _ := os.Getwd()
		return wd
	}
	return r.OutDir
}

// Output of generation.
type Output struct {
	// TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/32
}

// Generate takes some state and applies it to a template to create a client
// library.
func Generate(req *GenerateRequest) (*Output, error) {
	data := newTemplateData(req.API, req.Codec)
	root := filepath.Join(req.TemplateDir, req.Codec.TemplateDir())
	slog.Info(root)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".mustache" {
			return nil
		}
		if strings.Count(d.Name(), ".") == 1 {
			// skipping partials
			return nil
		}
		s, err := mustache.RenderFile(path, data)
		if err != nil {
			return err
		}
		fn := filepath.Join(req.outDir(), filepath.Dir(strings.TrimPrefix(path, root)), strings.TrimSuffix(d.Name(), ".mustache"))
		if err := os.WriteFile(fn, []byte(s), os.ModePerm); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		slog.Error("error walking templates", "err", err.Error())
		return nil, err
	}

	var output *Output
	return output, nil
}
