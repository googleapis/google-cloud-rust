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
	"slices"
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
	Methods     []*api.Method
	FieldName   string
	StructName  string
	DefaultHost string
}

type messageAnnotation struct {
	Name              string
	DocLines          []string
	ConstructorBody   string // A custom body for the message's constructor.
	HasFields         bool
	HasCustomEncoding bool
	HasToStringLines  bool
	ToStringLines     []string
}

type methodAnnotation struct {
	// The method name using Dart naming conventions.
	Name            string
	RequestMethod   string
	RequestType     string
	ResponseType    string
	DocLines        []string
	HasBody         bool
	ReturnsValue    bool
	BodyMessageName string
	PathParams      []*api.Field
	QueryParams     []*api.Field
}

type pathInfoAnnotation struct {
	PathFmt  string
	PathArgs []string
}

type oneOfAnnotation struct {
	Name     string
	DocLines []string
}

type fieldAnnotation struct {
	Name     string
	Type     string
	DocLines []string
	Required bool
	Nullable bool
	FromJson string
	ToJson   string
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

	// Register any missing WKT.
	registerMissingWkt(model.State)

	// Calculate required fields.
	requiredFields := calculateRequiredFields(model)

	// Traverse and annotate the enums defined in this API.
	for _, e := range model.Enums {
		annotateEnum(e, model.State)
	}

	// Traverse and annotate the messages defined in this API.
	for _, m := range model.Messages {
		annotateMessage(m, model.State, packageMapping, imports, requiredFields)
	}

	for _, s := range model.Services {
		annotateService(s, model.State, packageMapping, imports)
	}

	// Remove our self-reference.
	delete(imports, model.PackageName)

	// Add the import for the google_cloud_gax package.
	imports["cloud_gax"] = commonImport

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

func registerMissingWkt(state *api.APIState) {
	// If these definitions weren't provided by protoc then provide our own
	// placeholders.
	for _, message := range []struct {
		ID      string
		Name    string
		Package string
	}{
		{".google.protobuf.Any", "Any", "google.protobuf"},
		{".google.protobuf.Empty", "Empty", "google.protobuf"},
	} {
		_, ok := state.MessageByID[message.ID]
		if !ok {
			state.MessageByID[message.ID] = &api.Message{
				ID:      message.ID,
				Name:    message.Name,
				Package: message.Package,
			}
		}
	}
}

// Calculate package dependencies based on `package:` imports.
func calculateDependencies(imports map[string]string) []packageDependency {
	var deps []packageDependency

	for _, imp := range imports {
		if strings.HasPrefix(imp, "package:") {
			name := strings.TrimPrefix(imp, "package:")
			name = strings.Split(name, "/")[0]

			if !slices.ContainsFunc(deps, func(dep packageDependency) bool {
				return dep.Name == name
			}) {
				deps = append(deps, packageDependency{Name: name, Constraint: "any"})
			}
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

		// The package:http import should be imported with a prefix.
		prefix := ""
		if imp == httpImport {
			prefix = " as http"
		}

		imports = append(imports, fmt.Sprintf("import '%s'%s;", imp, prefix))
	}

	return imports
}

func calculateRequiredFields(model *api.API) map[string]*api.Field {
	required := map[string]*api.Field{}

	for _, s := range model.Services {
		// Some methods are skipped.
		methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
			return generateMethod(m)
		})

		for _, method := range methods {
			for _, field := range language.PathParams(method, model.State) {
				required[field.ID] = field
			}

			for _, field := range method.InputType.Fields {
				if field.Name == method.PathInfo.BodyFieldPath {
					required[field.ID] = field
				}
			}
		}
	}

	return required
}

func annotateService(s *api.Service, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	// Add a package:http import if we're generating a service.
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
		Methods:     methods,
		FieldName:   strcase.ToLowerCamel(s.Name),
		StructName:  s.Name,
		DefaultHost: s.DefaultHost,
	}
	s.Codec = ann
}

func annotateMessage(m *api.Message, state *api.APIState, packageMapping map[string]string,
	imports map[string]string, requiredFields map[string]*api.Field) {
	// Add the import for the common JSON helpers.
	imports["cloud_gax_helpers"] = commonHelpersImport

	for _, f := range m.Fields {
		annotateField(f, state, packageMapping, imports, requiredFields)
	}
	for _, f := range m.OneOfs {
		annotateOneOf(f, state)
	}
	for _, e := range m.Enums {
		annotateEnum(e, state)
	}
	for _, m := range m.Messages {
		annotateMessage(m, state, packageMapping, imports, requiredFields)
	}

	constructorBody := ";"
	_, needsValidation := needsCtorValidation[m.ID]
	if needsValidation {
		constructorBody = " {\n    _validate();\n  }"
	}

	_, hasCustomEncoding := usesCustomEncoding[m.ID]
	toStringLines := createToStringLines(m)

	m.Codec = &messageAnnotation{
		Name:              messageName(m),
		DocLines:          formatDocComments(m.Documentation, state),
		ConstructorBody:   constructorBody,
		HasFields:         len(m.Fields) > 0,
		HasCustomEncoding: hasCustomEncoding,
		HasToStringLines:  len(toStringLines) > 0,
		ToStringLines:     toStringLines,
	}
}

func createFromJsonLine(field *api.Field, state *api.APIState, required bool) string {
	name := strcase.ToLowerCamel(field.Name)
	message := state.MessageByID[field.TypezID]
	typeName := ""

	isList := field.Repeated
	isMessage := field.Typez == api.MESSAGE_TYPE
	isEnum := field.Typez == api.ENUM_TYPE
	isMap := message != nil && message.IsMap
	isMessageMap := isMap && message.Fields[1].Typez == api.MESSAGE_TYPE

	if isMessage {
		typeName = messageName(message)
	} else if isEnum {
		enum := state.EnumByID[field.TypezID]
		typeName = enumName(enum)
	}

	data := "json['" + name + "']"
	fn := typeName + ".fromJson"
	opt := ""
	bang := "!"
	if !required {
		opt = "?"
		bang = ""
	}

	if isMap {
		if isMessageMap {
			// message maps: decodeMap(json['name'], Status.fromJson)!,
			return "decodeMap(" + data + ", " + fn + ")" + bang
		} else {
			// primitive maps: (json['name'] as Map?)?.cast(),
			return "(" + data + " as Map" + opt + ")" + opt + ".cast()"
		}
	} else if isList {
		if isMessage {
			// message lists, custom lists: decodeList(json['name'], FieldMask.fromJson)!,
			return "decodeList(" + data + ", " + fn + ")" + bang
		} else {
			// primitive lists: (json['name'] as List?)?.cast(),
			return "(" + data + " as List" + opt + ")" + opt + ".cast()"
		}
	} else if isMessage || isEnum {
		// enum or message
		if required {
			// FieldMask.fromJson(json['name']),
			return fn + "(" + data + ")"
		} else {
			// decode(json['name'], FieldMask.fromJson),
			return "decode(" + data + ", " + fn + ")"
		}
	} else {
		// json['name']
		return data
	}
}

func createToJsonLine(field *api.Field, state *api.APIState, required bool) string {
	name := strcase.ToLowerCamel(field.Name)
	message := state.MessageByID[field.TypezID]

	isList := field.Repeated
	isMessage := field.Typez == api.MESSAGE_TYPE
	isEnum := field.Typez == api.ENUM_TYPE
	isMap := message != nil && message.IsMap
	isMessageMap := isMap && message.Fields[1].Typez == api.MESSAGE_TYPE
	bang := "!"
	if required {
		bang = ""
	}

	if isMessageMap {
		// message maps: encodeMap(name)
		return "encodeMap(" + name + ")"
	} else if isList && (isMessage || isEnum) {
		// message lists, custom lists, and enum lists: encodeList(name)
		return "encodeList(" + name + ")"
	} else if isMap {
		// primitive maps
		return name
	} else if isMessage || isEnum {
		// message, enum, and custom: name!.toJson()
		return name + bang + ".toJson()"
	} else {
		// primitive, primitive lists
		return name
	}
}

func createToStringLines(message *api.Message) []string {
	lines := []string{}

	for _, field := range message.Fields {
		codec := field.Codec.(*fieldAnnotation)
		name := codec.Name

		isList := field.Repeated
		isMessage := field.Typez == api.MESSAGE_TYPE

		// Don't generate toString() entries for lists, maps, or messages.
		if isList || isMessage {
			continue
		}

		if codec.Required {
			// 'name=$name',
			lines = append(lines, fmt.Sprintf("'%s=$%s',", name, name))
		} else {
			// if (name != null) 'name=$name',
			lines = append(lines, fmt.Sprintf("if (%s != null) '%s=$%s',", name, name, name))
		}
	}

	return lines
}

func annotateMethod(method *api.Method, state *api.APIState, packageMapping map[string]string, imports map[string]string) {
	// Ignore imports added from the input and output messages.
	tempImports := map[string]string{}
	tempRequiredFields := map[string]*api.Field{}
	if method.InputType.Codec == nil {
		annotateMessage(method.InputType, state, packageMapping, tempImports, tempRequiredFields)
	}
	if method.OutputType.Codec == nil {
		annotateMessage(method.OutputType, state, packageMapping, tempImports, tempRequiredFields)
	}

	pathInfoAnnotation := &pathInfoAnnotation{
		PathFmt:  httpPathFmt(method.PathInfo),
		PathArgs: httpPathArgs(method.PathInfo),
	}
	method.PathInfo.Codec = pathInfoAnnotation

	bodyMessageName := method.PathInfo.BodyFieldPath
	if bodyMessageName == "*" {
		bodyMessageName = "request"
	} else if bodyMessageName != "" {
		bodyMessageName = "request." + strcase.ToLowerCamel(bodyMessageName)
	}

	annotation := &methodAnnotation{
		Name:            strcase.ToLowerCamel(method.Name),
		RequestMethod:   strings.ToLower(method.PathInfo.Verb),
		RequestType:     resolveTypeName(state.MessageByID[method.InputTypeID], packageMapping, imports),
		ResponseType:    resolveTypeName(state.MessageByID[method.OutputTypeID], packageMapping, imports),
		DocLines:        formatDocComments(method.Documentation, state),
		HasBody:         method.PathInfo.BodyFieldPath != "",
		ReturnsValue:    method.OutputTypeID != ".google.protobuf.Empty",
		BodyMessageName: bodyMessageName,
		PathParams:      language.PathParams(method, state),
		QueryParams:     language.QueryParams(method, state),
	}
	method.Codec = annotation
}

func annotateOneOf(field *api.OneOf, state *api.APIState) {
	field.Codec = &oneOfAnnotation{
		Name:     strcase.ToLowerCamel(field.Name),
		DocLines: formatDocComments(field.Documentation, state),
	}
}

func annotateField(field *api.Field, state *api.APIState, packageMapping map[string]string,
	imports map[string]string, requiredFields map[string]*api.Field) {
	_, required := requiredFields[field.ID]

	field.Codec = &fieldAnnotation{
		Name:     strcase.ToLowerCamel(field.Name),
		Type:     fieldType(field, state, packageMapping, imports),
		DocLines: formatDocComments(field.Documentation, state),
		Required: required,
		Nullable: !required,
		FromJson: createFromJsonLine(field, state, required),
		ToJson:   createToJsonLine(field, state, required),
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
