// Copyright 2025 Google LLC
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

package dart

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	"github.com/iancoleman/strcase"
)

type modelAnnotations struct {
	// The Dart package name (e.g. google_cloud_secretmanager).
	PackageName string
	// The version of the generated package.
	PackageVersion string
	// Name of the API in snake_format (e.g. secretmanager).
	MainFileName      string
	SourcePackageName string
	HasServices       bool
	CopyrightYear     string
	BoilerPlate       []string
	DefaultHost       string
	DocLines          []string
	HasDependencies   bool
	// A reference to a hand-written part file.
	PartFileReference   string
	PackageDependencies []packageDependency
	Imports             []string
	// Whether the generated package specified any dev_dependencies.
	HasDevDependencies bool
	DevDependencies    []string
	DoNotPublish       bool
}

type serviceAnnotations struct {
	// The service name using Dart naming conventions.
	Name        string
	DocLines    []string
	FieldName   string
	StructName  string
	DefaultHost string
}

type messageAnnotation struct {
	Name           string
	DocLines       []string
	HasNestedTypes bool
	// The FQN is the source specification
	SourceFQN   string
	BasicFields []*api.Field
}

type methodAnnotation struct {
	// The method name using Dart naming conventions.
	Name         string
	RequestType  string
	ResponseType string
	DocLines     []string
	PathParams   []*api.Field
	QueryParams  []*api.Field
	BodyAccessor string
}

type pathInfoAnnotation struct {
	Method      string
	PathFmt     string
	PathArgs    []string
	HasPathArgs bool
	HasBody     bool
}

type oneOfAnnotation struct {
	Name     string
	DocLines []string
}

type fieldAnnotation struct {
	Name             string
	Type             string
	DocLines         []string
	AsQueryParameter string
}

type enumAnnotation struct {
	Name     string
	DocLines []string
}

type enumValueAnnotation struct {
	Name     string
	DocLines []string
}

type packageDependency struct {
	Name       string
	Constraint string
}

// annotateModel creates a struct used as input for Mustache templates.
// Fields and methods defined in this struct directly correspond to Mustache
// tags. For example, the Mustache tag {{#Services}} uses the
// [Template.Services] field.
func annotateModel(model *api.API, options map[string]string) (*modelAnnotations, error) {
	var (
		packageNameOverride string
		generationYear      string
		packageVersion      string
		// The Dart imports discovered while annotating the API model.
		imports           = map[string]string{}
		partFileReference string
		devDependencies   = []string{}
		doNotPublish      bool
		// A mapping from protobuf packages to Dart import URLs.
		packageMapping = map[string]string{}
	)

	for key, definition := range options {
		switch {
		case key == "package-name-override":
			packageNameOverride = definition
		case key == "copyright-year":
			generationYear = definition
		case key == "version":
			packageVersion = definition
		case key == "part-file":
			partFileReference = definition
		case key == "dev-dependencies":
			devDependencies = strings.Split(definition, ",")
		case key == "not-for-publication":
			value, err := strconv.ParseBool(definition)
			if err != nil {
				return nil, fmt.Errorf(
					"cannot convert `not-for-publication` value %q to boolean: %w",
					definition,
					err,
				)
			}
			doNotPublish = value
		case strings.HasPrefix(key, "proto:"):
			// "proto:google.protobuf" = "package:google_cloud_protobuf/protobuf.dart"
			keys := strings.Split(key, ":")
			if len(keys) != 2 {
				return nil, fmt.Errorf("key should be in the format proto:<proto-package>, got=%q", key)
			}
			protoPackage := keys[1]
			packageMapping[protoPackage] = definition
		}
	}

	// Traverse and annotate the enums defined in this API.
	for _, e := range model.Enums {
		annotateEnum(e, model.State)
	}

	// Traverse and annotate the messages defined in this API.
	for _, m := range model.Messages {
		traverseMessage(m, model.State, packageMapping, imports)
	}

	for _, s := range model.Services {
		annotateService(s, model.State, packageMapping, imports)
	}

	// Remove our self-reference.
	delete(imports, model.PackageName)

	deps := calculateDependencies(imports)

	ann := &modelAnnotations{
		PackageName:    modelPackageName(model, packageNameOverride),
		PackageVersion: packageVersion,
		MainFileName:   strcase.ToSnake(model.Name),
		HasServices:    len(model.Services) > 0,
		CopyrightYear:  generationYear,
		BoilerPlate: append(license.LicenseHeaderBulk(),
			"",
			" Code generated by sidekick. DO NOT EDIT."),
		DefaultHost: func() string {
			if len(model.Services) > 0 {
				return model.Services[0].DefaultHost
			}
			return ""
		}(),
		DocLines:            formatDocComments(model.Description, model.State),
		HasDependencies:     len(deps) > 0,
		PartFileReference:   partFileReference,
		PackageDependencies: deps,
		Imports:             calculateImports(imports),
		HasDevDependencies:  len(devDependencies) > 0,
		DevDependencies:     devDependencies,
		DoNotPublish:        doNotPublish,
	}

	model.Codec = ann
	return ann, nil
}

// Calculate package dependencies based on `package:` imports.
func calculateDependencies(imports map[string]string) []packageDependency {
	var deps []packageDependency

	for _, imp := range imports {
		if strings.HasPrefix(imp, "package:") {
			name := strings.TrimPrefix(imp, "package:")
			name = strings.Split(name, "/")[0]

			deps = append(deps, packageDependency{Name: name, Constraint: "any"})
		}
	}

	sort.SliceStable(deps, func(i, j int) bool {
		return deps[i].Name < deps[j].Name
	})

	return deps
}

func calculateImports(usedImports map[string]string) []string {
	var dartImports []string
	for _, imp := range usedImports {
		dartImports = append(dartImports, imp)
	}
	sort.Strings(dartImports)

	previousImportType := ""
	var imports []string

	for _, imp := range dartImports {
		// Emit a blank line when changing between 'dart:' and 'package:' imports.
		importType := strings.Split(imp, ":")[0]
		if previousImportType != "" && previousImportType != importType {
			imports = append(imports, "")
		}
		previousImportType = importType

		imports = append(imports, fmt.Sprintf("import '%s';", imp))
	}

	return imports
}

func annotateService(s *api.Service, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	// Require package:http when generating services.
	imports["http"] = httpImport

	// Some methods are skipped.
	methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
		return generateMethod(m)
	})
	for _, m := range methods {
		annotateMethod(m, state, packageMapping, imports)
	}
	ann := &serviceAnnotations{
		Name:        s.Name,
		DocLines:    formatDocComments(s.Documentation, state),
		FieldName:   strcase.ToLowerCamel(s.Name),
		StructName:  s.Name,
		DefaultHost: s.DefaultHost,
	}
	s.Codec = ann
}

func traverseMessage(m *api.Message, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	annotateMessage(m, state, packageMapping, imports)

	for _, e := range m.Enums {
		annotateEnum(e, state)
	}

	for _, m := range m.Messages {
		traverseMessage(m, state, packageMapping, imports)
	}
}

func annotateMessage(m *api.Message, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	for _, f := range m.Fields {
		annotateField(f, state, packageMapping, imports)
	}
	for _, f := range m.OneOfs {
		annotateOneOf(f, state)
	}
	m.Codec = &messageAnnotation{
		Name:           messageName(m),
		DocLines:       formatDocComments(m.Documentation, state),
		HasNestedTypes: language.HasNestedTypes(m),
		SourceFQN:      strings.TrimPrefix(m.ID, "."),
		BasicFields: language.FilterSlice(m.Fields, func(s *api.Field) bool {
			return !s.IsOneOf
		}),
	}
}

func annotateMethod(method *api.Method, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	pathInfoAnnotation := &pathInfoAnnotation{
		Method:   method.PathInfo.Verb,
		PathFmt:  httpPathFmt(method.PathInfo),
		PathArgs: httpPathArgs(method.PathInfo),
		HasBody:  method.PathInfo.BodyFieldPath != "",
	}
	method.PathInfo.Codec = pathInfoAnnotation
	annotation := &methodAnnotation{
		Name:         strcase.ToLowerCamel(method.Name),
		RequestType:  resolveTypeName(state.MessageByID[method.InputTypeID], packageMapping, imports),
		ResponseType: resolveTypeName(state.MessageByID[method.OutputTypeID], packageMapping, imports),
		DocLines:     formatDocComments(method.Documentation, state),
		BodyAccessor: bodyAccessor(method),
		PathParams:   language.PathParams(method, state),
		QueryParams:  language.QueryParams(method, state),
	}
	method.Codec = annotation
}

func annotateOneOf(field *api.OneOf, state *api.APIState) {
	field.Codec = &oneOfAnnotation{
		Name:     strcase.ToLowerCamel(field.Name),
		DocLines: formatDocComments(field.Documentation, state),
	}
}

func annotateField(field *api.Field, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	field.Codec = &fieldAnnotation{
		Name:     strcase.ToLowerCamel(field.Name),
		Type:     fieldType(field, state, packageMapping, imports),
		DocLines: formatDocComments(field.Documentation, state),
	}
}

func annotateEnum(enum *api.Enum, state *api.APIState) {
	for _, ev := range enum.Values {
		annotateEnumValue(ev, state)
	}
	enum.Codec = &enumAnnotation{
		Name:     enumName(enum),
		DocLines: formatDocComments(enum.Documentation, state),
	}
}

func annotateEnumValue(ev *api.EnumValue, state *api.APIState) {
	ev.Codec = &enumValueAnnotation{
		Name:     enumValueName(ev),
		DocLines: formatDocComments(ev.Documentation, state),
	}
}
