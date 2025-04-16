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
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	"github.com/iancoleman/strcase"
)

var omitGeneration = map[string]string{
	".google.longrunning.Operation": "",
	".google.protobuf.Value":        "",
}

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
	// A reference to an optional hand-written part file.
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
	QualifiedName     string
	DocLines          []string
	OmitGeneration    bool
	ConstructorBody   string // A custom body for the message's constructor.
	HasFields         bool
	HasCustomEncoding bool
	HasToStringLines  bool
	ToStringLines     []string
}

type methodAnnotation struct {
	// The method name using Dart naming conventions.
	Name              string
	RequestMethod     string
	RequestType       string
	ResponseType      string
	DocLines          []string
	HasBody           bool
	ReturnsValue      bool
	BodyMessageName   string
	PathParams        []*api.Field
	QueryParams       []*api.Field
	IsLROGetOperation bool
}

type pathInfoAnnotation struct {
	PathFmt  string
	PathArgs []string
}

type oneOfAnnotation struct {
	Name     string
	DocLines []string
}

type operationInfoAnnotation struct {
	ResponseType string
	MetadataType string
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

type annotateModel struct {
	// The API model we're annotating.
	model *api.API
	// Mappings from IDs to types.
	state *api.APIState
	// The set of imports that have been calculated.
	imports map[string]string
	// The mapping from protobuf packages to Dart import statements.
	packageMapping map[string]string
	// A mapping from field IDs to fields for the fields we know to be required.
	requiredFields map[string]*api.Field
}

func newAnnotateModel(model *api.API) *annotateModel {
	return &annotateModel{
		model:          model,
		state:          model.State,
		imports:        map[string]string{},
		packageMapping: map[string]string{},
	}
}

// annotateModel creates a struct used as input for Mustache templates.
// Fields and methods defined in this struct directly correspond to Mustache
// tags. For example, the Mustache tag {{#Services}} uses the
// [Template.Services] field.
func (annotate *annotateModel) annotateModel(options map[string]string) (*modelAnnotations, error) {
	var (
		packageNameOverride string
		generationYear      string
		packageVersion      string
		partFileReference   string
		doNotPublish        bool
		devDependencies     = []string{}
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
			annotate.packageMapping[protoPackage] = definition
		case key == "extra-imports":
			extraImports := strings.Split(definition, ",")
			for _, item := range extraImports {
				annotate.imports[item] = item
			}
		}
	}

	// Register any missing WKTs.
	registerMissingWkt(annotate.state)

	model := annotate.model

	// Calculate required fields.
	annotate.requiredFields = calculateRequiredFields(model)

	// Traverse and annotate the enums defined in this API.
	for _, e := range model.Enums {
		annotate.annotateEnum(e)
	}

	// Traverse and annotate the messages defined in this API.
	for _, m := range model.Messages {
		annotate.annotateMessage(m, annotate.imports)
	}

	for _, s := range model.Services {
		annotate.annotateService(s)
	}

	// Remove our package self-reference.
	delete(annotate.imports, model.PackageName)

	// Add a dev dependency on package:lints.
	devDependencies = append(devDependencies, "lints")

	// Add the import for the google_cloud_gax package.
	annotate.imports["cloud_gax"] = commonImport

	packageDependencies := calculateDependencies(annotate.imports)

	ann := &modelAnnotations{
		PackageName:    packageName(model, packageNameOverride),
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
		Imports:             calculateImports(annotate.imports),
		PartFileReference:   partFileReference,
		HasDependencies:     len(packageDependencies) > 0,
		PackageDependencies: packageDependencies,
		HasDevDependencies:  len(devDependencies) > 0,
		DevDependencies:     devDependencies,
		DoNotPublish:        doNotPublish,
	}

	model.Codec = ann
	return ann, nil
}

func calculateRequiredFields(model *api.API) map[string]*api.Field {
	required := map[string]*api.Field{}

	for _, s := range model.Services {
		// Some methods are skipped.
		methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
			return shouldGenerateMethod(m)
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

func calculateImports(imports map[string]string) []string {
	var dartImports []string
	for _, imp := range imports {
		dartImports = append(dartImports, imp)
	}
	sort.Strings(dartImports)

	previousImportType := ""
	var results []string

	for _, imp := range dartImports {
		// Emit a blank line when changing between 'dart:' and 'package:' imports.
		importType := strings.Split(imp, ":")[0]
		if previousImportType != "" && previousImportType != importType {
			results = append(results, "")
		}
		previousImportType = importType

		// Wrap the first part of the import (or the whole import) in single quotes.
		index := strings.IndexAny(imp, " ")
		if index != -1 {
			imp = "'" + imp[0:index] + "'" + imp[index:]
		} else {
			imp = "'" + imp + "'"
		}

		results = append(results, fmt.Sprintf("import %s;", imp))
	}

	return results
}

func (annotate *annotateModel) annotateService(s *api.Service) {
	// Add a package:http import if we're generating a service.
	annotate.imports["http"] = httpImport

	// Some methods are skipped.
	methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
		return shouldGenerateMethod(m)
	})

	for _, m := range methods {
		annotate.annotateMethod(m)
	}
	ann := &serviceAnnotations{
		Name:        s.Name,
		DocLines:    formatDocComments(s.Documentation, annotate.state),
		Methods:     methods,
		FieldName:   strcase.ToLowerCamel(s.Name),
		StructName:  s.Name,
		DefaultHost: s.DefaultHost,
	}
	s.Codec = ann
}

func (annotate *annotateModel) annotateMessage(m *api.Message, imports map[string]string) {
	// Add the import for the common JSON helpers.
	imports["cloud_gax_helpers"] = commonHelpersImport

	for _, f := range m.Fields {
		annotate.annotateField(f)
	}
	for _, o := range m.OneOfs {
		annotate.annotateOneOf(o)
	}
	for _, e := range m.Enums {
		annotate.annotateEnum(e)
	}
	for _, m := range m.Messages {
		annotate.annotateMessage(m, imports)
	}

	constructorBody := ";"
	_, needsValidation := needsCtorValidation[m.ID]
	if needsValidation {
		constructorBody = " {\n" +
			"    _validate();\n" +
			"  }"
	}

	_, hasCustomEncoding := usesCustomEncoding[m.ID]
	toStringLines := createToStringLines(m)

	_, omit := omitGeneration[m.ID]

	m.Codec = &messageAnnotation{
		Name:              messageName(m),
		QualifiedName:     qualifiedName(m),
		DocLines:          formatDocComments(m.Documentation, annotate.state),
		OmitGeneration:    omit || m.IsMap,
		ConstructorBody:   constructorBody,
		HasFields:         len(m.Fields) > 0,
		HasCustomEncoding: hasCustomEncoding,
		HasToStringLines:  len(toStringLines) > 0,
		ToStringLines:     toStringLines,
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

func (annotate *annotateModel) annotateMethod(method *api.Method) {
	// Ignore imports added from the input and output messages.
	tempImports := map[string]string{}
	if method.InputType.Codec == nil {
		annotate.annotateMessage(method.InputType, tempImports)
	}
	if method.OutputType.Codec == nil {
		annotate.annotateMessage(method.OutputType, tempImports)
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

	state := annotate.state

	// For 'GetOperation' mixins, we augment the method generation with
	// additional generic type parameters.
	isGetOperation := method.Name == "GetOperation" &&
		method.OutputTypeID == ".google.longrunning.Operation"
	if method.ID == ".google.longrunning.Operations.GetOperation" {
		isGetOperation = false
	}

	if method.OperationInfo != nil {
		annotate.annotateOperationInfo(method.OperationInfo)
	}

	annotation := &methodAnnotation{
		Name:              strcase.ToLowerCamel(method.Name),
		RequestMethod:     strings.ToLower(method.PathInfo.Verb),
		RequestType:       annotate.resolveTypeName(state.MessageByID[method.InputTypeID]),
		ResponseType:      annotate.resolveTypeName(state.MessageByID[method.OutputTypeID]),
		DocLines:          formatDocComments(method.Documentation, state),
		HasBody:           method.PathInfo.BodyFieldPath != "",
		ReturnsValue:      method.OutputTypeID != ".google.protobuf.Empty",
		BodyMessageName:   bodyMessageName,
		PathParams:        language.PathParams(method, state),
		QueryParams:       language.QueryParams(method, state),
		IsLROGetOperation: isGetOperation,
	}
	method.Codec = annotation
}

func (annotate *annotateModel) annotateOperationInfo(operationInfo *api.OperationInfo) {
	response := annotate.state.MessageByID[operationInfo.ResponseTypeID]
	metadata := annotate.state.MessageByID[operationInfo.MetadataTypeID]

	operationInfo.Codec = &operationInfoAnnotation{
		ResponseType: messageName(response),
		MetadataType: messageName(metadata),
	}
}

func (annotate *annotateModel) annotateOneOf(oneof *api.OneOf) {
	oneof.Codec = &oneOfAnnotation{
		Name:     strcase.ToLowerCamel(oneof.Name),
		DocLines: formatDocComments(oneof.Documentation, annotate.state),
	}
}

func (annotate *annotateModel) annotateField(field *api.Field) {
	_, required := annotate.requiredFields[field.ID]
	state := annotate.state

	field.Codec = &fieldAnnotation{
		Name:     fieldName(field),
		Type:     annotate.fieldType(field),
		DocLines: formatDocComments(field.Documentation, state),
		Required: required,
		Nullable: !required,
		FromJson: createFromJsonLine(field, state, required),
		ToJson:   createToJsonLine(field, state, required),
	}
}

func createFromJsonLine(field *api.Field, state *api.APIState, required bool) string {
	name := fieldName(field)
	message := state.MessageByID[field.TypezID]

	isList := field.Repeated
	isMap := message != nil && message.IsMap

	data := fmt.Sprintf("json['%s']", name)

	bang := "!"
	if !required {
		bang = ""
	}

	switch {
	case isList:
		switch {
		case field.Typez == api.BYTES_TYPE:
			return fmt.Sprintf("decodeListBytes(%s)%s", data, bang)
		case field.Typez == api.ENUM_TYPE:
			typeName := enumName(state.EnumByID[field.TypezID])
			return fmt.Sprintf("decodeListEnum(%s, %s.fromJson)%s", data, typeName, bang)
		case field.Typez == api.MESSAGE_TYPE:
			_, hasCustomEncoding := usesCustomEncoding[field.TypezID]
			typeName := messageName(state.MessageByID[field.TypezID])
			if hasCustomEncoding {
				return fmt.Sprintf("decodeListMessageCustom(%s, %s.fromJson)%s", data, typeName, bang)
			} else {
				return fmt.Sprintf("decodeListMessage(%s, %s.fromJson)%s", data, typeName, bang)
			}
		default:
			return fmt.Sprintf("decodeList(%s)%s", data, bang)
		}
	case isMap:
		valueField := message.Fields[1]

		switch {
		case valueField.Typez == api.BYTES_TYPE:
			return fmt.Sprintf("decodeMapBytes(%s)%s", data, bang)
		case valueField.Typez == api.ENUM_TYPE:
			typeName := enumName(state.EnumByID[valueField.TypezID])
			return fmt.Sprintf("decodeMapEnum(%s, %s.fromJson)%s", data, typeName, bang)
		case valueField.Typez == api.MESSAGE_TYPE:
			_, hasCustomEncoding := usesCustomEncoding[valueField.TypezID]
			typeName := messageName(state.MessageByID[valueField.TypezID])
			if hasCustomEncoding {
				return fmt.Sprintf("decodeMapMessageCustom(%s, %s.fromJson)%s", data, typeName, bang)
			} else {
				return fmt.Sprintf("decodeMapMessage(%s, %s.fromJson)%s", data, typeName, bang)
			}
		default:
			return fmt.Sprintf("decodeMap(%s)%s", data, bang)
		}
	case field.Typez == api.INT64_TYPE || field.Typez == api.UINT64_TYPE:
		return fmt.Sprintf("decodeInt64(%s)%s", data, bang)
	case field.Typez == api.FLOAT_TYPE || field.Typez == api.DOUBLE_TYPE:
		return fmt.Sprintf("decodeDouble(%s)%s", data, bang)
	case field.Typez == api.BYTES_TYPE:
		return fmt.Sprintf("decodeBytes(%s)%s", data, bang)
	case field.Typez == api.ENUM_TYPE:
		typeName := enumName(state.EnumByID[field.TypezID])
		return fmt.Sprintf("decodeEnum(%s, %s.fromJson)%s", data, typeName, bang)
	case field.Typez == api.MESSAGE_TYPE:
		_, hasCustomEncoding := usesCustomEncoding[field.TypezID]
		typeName := messageName(state.MessageByID[field.TypezID])
		if hasCustomEncoding {
			return fmt.Sprintf("decodeCustom(%s, %s.fromJson)%s", data, typeName, bang)
		} else {
			return fmt.Sprintf("decode(%s, %s.fromJson)%s", data, typeName, bang)
		}
	}

	// No decoding necessary.
	return data
}

func createToJsonLine(field *api.Field, state *api.APIState, required bool) string {
	name := fieldName(field)
	message := state.MessageByID[field.TypezID]

	isList := field.Repeated
	isMap := message != nil && message.IsMap

	bang := "!"
	if required {
		bang = ""
	}

	switch {
	case isList:
		switch {
		case field.Typez == api.BYTES_TYPE:
			return fmt.Sprintf("encodeListBytes(%s)", name)
		case field.Typez == api.MESSAGE_TYPE || field.Typez == api.ENUM_TYPE:
			return fmt.Sprintf("encodeList(%s)", name)
		default:
			// identity
			return name
		}
	case isMap:
		valueField := message.Fields[1]

		switch {
		case valueField.Typez == api.BYTES_TYPE:
			return fmt.Sprintf("encodeMapBytes(%s)", name)
		case valueField.Typez == api.MESSAGE_TYPE || valueField.Typez == api.ENUM_TYPE:
			return fmt.Sprintf("encodeMap(%s)", name)
		default:
			// identity
			return name
		}
	case field.Typez == api.MESSAGE_TYPE || field.Typez == api.ENUM_TYPE:
		return fmt.Sprintf("%s%s.toJson()", name, bang)
	case field.Typez == api.BYTES_TYPE:
		return fmt.Sprintf("encodeBytes(%s)", name)
	case field.Typez == api.INT64_TYPE || field.Typez == api.UINT64_TYPE:
		return fmt.Sprintf("encodeInt64(%s)", name)
	default:
	}

	// No encoding necessary.
	return name
}

func (annotate *annotateModel) annotateEnum(enum *api.Enum) {
	for _, ev := range enum.Values {
		annotate.annotateEnumValue(ev)
	}
	enum.Codec = &enumAnnotation{
		Name:     enumName(enum),
		DocLines: formatDocComments(enum.Documentation, annotate.state),
	}
}

func (annotate *annotateModel) annotateEnumValue(ev *api.EnumValue) {
	ev.Codec = &enumValueAnnotation{
		Name:     enumValueName(ev),
		DocLines: formatDocComments(ev.Documentation, annotate.state),
	}
}

func (annotate *annotateModel) fieldType(f *api.Field) string {
	var out string

	switch f.Typez {
	case api.BOOL_TYPE:
		out = "bool"
	case api.INT32_TYPE:
		out = "int"
	case api.INT64_TYPE:
		out = "int"
	case api.UINT32_TYPE:
		out = "int"
	case api.UINT64_TYPE:
		out = "int"
	case api.FLOAT_TYPE:
		out = "double"
	case api.DOUBLE_TYPE:
		out = "double"
	case api.STRING_TYPE:
		out = "String"
	case api.BYTES_TYPE:
		annotate.imports[typedDataImport] = typedDataImport
		out = "Uint8List"
	case api.MESSAGE_TYPE:
		message, ok := annotate.state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if message.IsMap {
			key := annotate.fieldType(message.Fields[0])
			val := annotate.fieldType(message.Fields[1])
			out = "Map<" + key + ", " + val + ">"
		} else {
			out = annotate.resolveTypeName(message)
		}
	case api.ENUM_TYPE:
		e, ok := annotate.state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		annotate.updateUsedPackages(e.Package)
		out = enumName(e)
	default:
		slog.Error("unhandled fieldType", "type", f.Typez, "id", f.TypezID)
	}

	if f.Repeated {
		out = "List<" + out + ">"
	}

	return out
}

func (annotate *annotateModel) resolveTypeName(message *api.Message) string {
	if message == nil {
		slog.Error("unable to lookup type")
		return ""
	}

	if message.ID == ".google.protobuf.Empty" {
		return "void"
	}

	annotate.updateUsedPackages(message.Package)

	return messageName(message)
}

func (annotate *annotateModel) updateUsedPackages(packageName string) {
	selfReference := annotate.model.PackageName == packageName
	if !selfReference {
		// Use the packageMapping info to add any necessary import.
		dartImport, ok := annotate.packageMapping[packageName]
		if ok {
			annotate.imports[packageName] = dartImport
		} else {
			println("missing proto package mapping: " + packageName)
		}
	}
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
