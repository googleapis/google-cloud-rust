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
		importMap           = map[string]*dartImport{}
		partFileReference   string
		devDependencies     = []string{}
		doNotPublish        bool
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
		case strings.HasPrefix(key, "import-mapping"):
			keys := strings.Split(key, ":")
			if len(keys) != 2 {
				return nil, fmt.Errorf("key should be in the format import-mapping:proto.path, got=%q", key)
			}
			defs := strings.Split(definition, ";")
			if len(defs) != 2 {
				return nil, fmt.Errorf("%s should be in the format path;name, got=%q", definition, keys[1])
			}
			// TODO(#1034): Handle updating Dart imports.
		}
	}

	loadWellKnownTypes(model.State)
	for _, e := range model.State.EnumByID {
		annotateEnum(e, model.State)
	}
	for _, m := range model.State.MessageByID {
		annotateMessage(m, model.State, importMap)
	}
	for _, s := range model.Services {
		annotateService(s, model.State, importMap)
	}

	deps := calculateDependencies(importMap)

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
		Imports:             calculateImports(importMap),
		HasDevDependencies:  len(devDependencies) > 0,
		DevDependencies:     devDependencies,
		DoNotPublish:        doNotPublish,
	}

	model.Codec = ann
	return ann, nil
}

// Calculate package dependencies based on `package:` imports.
func calculateDependencies(importMap map[string]*dartImport) []packageDependency {
	var deps []packageDependency

	for _, imp := range importMap {
		if strings.HasPrefix(imp.DartImport, "package:") {
			name := strings.TrimPrefix(imp.DartImport, "package:")
			name = strings.Split(name, "/")[0]

			deps = append(deps, packageDependency{Name: name, Constraint: "any"})
		}
	}

	sort.SliceStable(deps, func(i, j int) bool {
		return deps[i].Name < deps[j].Name
	})

	return deps
}

func calculateImports(importMap map[string]*dartImport) []string {
	var dartImports []string
	for _, imp := range importMap {
		dartImports = append(dartImports, imp.DartImport)
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

func annotateService(s *api.Service, state *api.APIState, importMap map[string]*dartImport) {
	// Require package:http when generating services.
	importMap[httpImport.Package] = httpImport

	// Some methods are skipped.
	methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
		return generateMethod(m)
	})
	for _, m := range methods {
		annotateMethod(m, state)
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

func annotateMessage(m *api.Message, state *api.APIState, importMap map[string]*dartImport) {
	for _, f := range m.Fields {
		annotateField(f, state, importMap)
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

func annotateMethod(m *api.Method, state *api.APIState) {
	pathInfoAnnotation := &pathInfoAnnotation{
		Method:   m.PathInfo.Verb,
		PathFmt:  httpPathFmt(m.PathInfo),
		PathArgs: httpPathArgs(m.PathInfo),
		HasBody:  m.PathInfo.BodyFieldPath != "",
	}
	m.PathInfo.Codec = pathInfoAnnotation
	annotation := &methodAnnotation{
		Name:         strcase.ToLowerCamel(m.Name),
		RequestType:  methodInOutTypeName(m.InputTypeID, state),
		ResponseType: methodInOutTypeName(m.OutputTypeID, state),
		DocLines:     formatDocComments(m.Documentation, state),
		BodyAccessor: bodyAccessor(m),
		PathParams:   language.PathParams(m, state),
		QueryParams:  language.QueryParams(m, state),
	}
	m.Codec = annotation
}

func annotateOneOf(field *api.OneOf, state *api.APIState) {
	field.Codec = &oneOfAnnotation{
		Name:     strcase.ToLowerCamel(field.Name),
		DocLines: formatDocComments(field.Documentation, state),
	}
}

func annotateField(field *api.Field, state *api.APIState, importMap map[string]*dartImport) {
	field.Codec = &fieldAnnotation{
		Name:     strcase.ToLowerCamel(field.Name),
		Type:     fieldType(field, state, importMap),
		DocLines: formatDocComments(field.Documentation, state),
	}
}

func annotateEnum(e *api.Enum, state *api.APIState) {
	for _, ev := range e.Values {
		annotateEnumValue(ev, state)
	}
	e.Codec = &enumAnnotation{
		Name:     enumName(e),
		DocLines: formatDocComments(e.Documentation, state),
	}
}

func annotateEnumValue(ev *api.EnumValue, state *api.APIState) {
	ev.Codec = &enumValueAnnotation{
		Name:     enumValueName(ev),
		DocLines: formatDocComments(ev.Documentation, state),
	}
}
