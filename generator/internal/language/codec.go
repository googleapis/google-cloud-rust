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
	"log/slog"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

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
// and the `.mustache` extension, such as `rust/crate/src/lib.rs.mustache` and
// then return the full contents of the template (or an error).
type TemplateProvider func(templateName string) (string, error)

func PathParams(m *api.Method, state *api.APIState) []*api.Field {
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup request type", "id", m.InputTypeID)
		return nil
	}
	pathNames := []string{}
	for _, arg := range m.PathInfo.Bindings[0].LegacyPathTemplate {
		if arg.FieldPath != nil {
			components := strings.Split(*arg.FieldPath, ".")
			pathNames = append(pathNames, components[0])
		}
	}

	var params []*api.Field
	duplicate := map[string]bool{}
	for _, name := range pathNames {
		for _, field := range msg.Fields {
			if field.Name == name && !duplicate[name] {
				params = append(params, field)
				duplicate[name] = true
				break
			}
		}
	}
	return params
}

func QueryParams(m *api.Method, b *api.PathBinding) []*api.Field {
	var queryParams []*api.Field
	for _, field := range m.InputType.Fields {
		if !b.QueryParameters[field.Name] {
			continue
		}
		queryParams = append(queryParams, field)
	}
	return queryParams
}

func FilterSlice[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0)
	for _, v := range slice {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result
}

func MapSlice[T, R any](s []T, f func(T) R) []R {
	r := make([]R, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}

func HasNestedTypes(m *api.Message) bool {
	if len(m.Enums) > 0 || len(m.OneOfs) > 0 {
		return true
	}
	for _, child := range m.Messages {
		if !child.IsMap {
			return true
		}
	}
	return false
}

func FieldIsMap(f *api.Field, state *api.APIState) bool {
	if f.Typez != api.MESSAGE_TYPE {
		return false
	}
	if m, ok := state.MessageByID[f.TypezID]; ok {
		return m.IsMap
	}
	return false
}
