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
// and the `.mustache` extension, such as `rust/crate/src/lib.rs.mustach` and
// tuen return the full contents of the template (or an error).
type templateProvider func(templateName string) (string, error)

func PathParams(m *api.Method, state *api.APIState) []*api.Field {
	msg, ok := state.MessageByID[m.InputTypeID]
	if !ok {
		slog.Error("unable to lookup request type", "id", m.InputTypeID)
		return nil
	}
	pathNames := []string{}
	for _, arg := range m.PathInfo.PathTemplate {
		if arg.FieldPath != nil {
			components := strings.Split(*arg.FieldPath, ".")
			pathNames = append(pathNames, components[0])
		}
	}

	var params []*api.Field
	for _, name := range pathNames {
		for _, field := range msg.Fields {
			if field.Name == name {
				params = append(params, field)
				break
			}
		}
	}
	return params
}

func QueryParams(m *api.Method, state *api.APIState) []*api.Field {
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

func filterSlice[T any](slice []T, predicate func(T) bool) []T {
	result := make([]T, 0)
	for _, v := range slice {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result
}
