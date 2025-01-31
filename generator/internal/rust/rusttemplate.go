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
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	"github.com/iancoleman/strcase"
)

type modelAnnotations struct {
	PackageName      string
	PackageVersion   string
	PackageNamespace string
	RequiredPackages []string
	ExternPackages   []string
	HasServices      bool
	HasLROs          bool
	CopyrightYear    string
	BoilerPlate      []string
	DefaultHost      string
	// Services without methods create a lot of warnings in Rust. The dead code
	// analysis is extremely good, and can determine that several types and
	// member variables are going unused if the service does not have any
	// generated methods. Filter out the services to the subset that will
	// produce at least one method.
	Services          []*api.Service
	NameToLower       string
	NotForPublication bool
	HasFeatures       bool
	Features          []string
	// When bootstrapping the well-known types crate the templates add some
	// ad-hoc code.
	IsWktCrate bool
}

type serviceAnnotations struct {
	// The name of the service. The Rust naming conventions requires this to be
	// in `PascalCase`. Notably, names like `IAM` *must* become `Iam`, but
	// `IAMService` can stay unchanged.
	Name string
	// For each service we generate a module containing all its builders.
	// The Rust naming conventions required this to be `snake_case` format.
	ModuleName string
	DocLines   []string
	// Only a subset of the methods is generated.
	Methods     []*api.Method
	DefaultHost string
	// If true, this service includes methods that return long-running operations.
	HasLROs  bool
	APITitle string
}

type messageAnnotation struct {
	Name          string
	ModuleName    string
	QualifiedName string
	// The FQN is the source specification
	SourceFQN         string
	MessageAttributes []string
	DocLines          []string
	HasNestedTypes    bool
	// All the fields except OneOfs.
	BasicFields []*api.Field
	// The subset of `BasicFields` that are neither maps, nor repeated.
	SingularFields []*api.Field
	// The subset of `BasicFields` that are repeated (`Vec<T>` in Rust).
	RepeatedFields []*api.Field
	// The subset of `BasicFields` that are maps (`HashMap<K, V>` in Rust).
	MapFields []*api.Field
	// If true, this is a synthetic message, some generation is skipped for
	// synthetic messages
	HasSyntheticFields bool
}

type methodAnnotation struct {
	Name                string
	BuilderName         string
	DocLines            []string
	PathInfo            *api.PathInfo
	PathParams          []*api.Field
	QueryParams         []*api.Field
	BodyAccessor        string
	ServiceNameToPascal string
	ServiceNameToCamel  string
	ServiceNameToSnake  string
	OperationInfo       *operationInfo
}

type pathInfoAnnotation struct {
	Method        string
	MethodToLower string
	PathFmt       string
	PathArgs      []string
	HasPathArgs   bool
	HasBody       bool
}

type operationInfo struct {
	MetadataType       string
	ResponseType       string
	MetadataTypeInDocs string
	ResponseTypeInDocs string
	PackageNamespace   string
}

type oneOfAnnotation struct {
	// In Rust, `oneof` fields are fields inside a struct. These must be
	// `snake_case`. Possibly mangled with `r#` if the name is a Rust reserved
	// word.
	FieldName string
	// In Rust, each field gets a `set_{{FieldName}}` setter. These must be
	// `snake_case`, but are never mangled with a `r#` prefix.
	SetterName string
	// The `oneof` is represented by a Rust `enum`, these need to be `PascalCase`.
	EnumName string
	// The Rust `enum` may be in a deeply nested scope. This is a shortcut.
	FQEnumName string
	FieldType  string
	DocLines   []string
	// The subset of the oneof fields that are neither maps, nor repeated.
	SingularFields []*api.Field
	// The subset of the oneof fields that are repeated (`Vec<T>` in Rust).
	RepeatedFields []*api.Field
	// The subset of the oneof fields that are maps (`HashMap<K, V>` in Rust).
	MapFields []*api.Field
}

type fieldAnnotations struct {
	// In Rust, message fields are fields inside a struct. These must be
	// `snake_case`. Possibly mangled with `r#` if the name is a Rust reserved
	// word.
	FieldName string
	// In Rust, each fields gets a `set_{{FieldName}}` setter. These must be
	// `snake_case`, but are never mangled with a `r#` prefix.
	SetterName string
	// In Rust, fields that appear in a OneOf also appear as a enum branch.
	// These must be in `PascalCase`.
	BranchName string
	// The fully qualified name of the containing message.
	FQMessageName      string
	DocLines           []string
	Attributes         []string
	FieldType          string
	PrimitiveFieldType string
	AddQueryParameter  string
	// For fields that are maps, these are the type of the key and value,
	// respectively.
	KeyType   string
	ValueType string
}

type enumAnnotation struct {
	Name       string
	ModuleName string
	DocLines   []string
}

type enumValueAnnotation struct {
	Name     string
	EnumType string
	DocLines []string
}

// annotateModel creates a struct used as input for Mustache templates.
// Fields and methods defined in this struct directly correspond to Mustache
// tags. For example, the Mustache tag {{#Services}} uses the
// [Template.Services] field.
func annotateModel(model *api.API, c *codec, outdir string) (*modelAnnotations, error) {
	c.hasServices = len(model.State.ServiceByID) > 0

	// Set the source package. We should always take the first service registered
	// as the source package. Services with mixins will register those after the
	// source package.
	if len(model.Services) > 0 {
		c.sourceSpecificationPackageName = model.Services[0].Package
	} else if len(model.Messages) > 0 {
		c.sourceSpecificationPackageName = model.Messages[0].Package
	}
	if err := validateModel(model, c.sourceSpecificationPackageName); err != nil {
		return nil, err
	}

	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, c.extraPackages)
	packageName := packageName(model, c.packageNameOverride)
	packageNamespace := strings.ReplaceAll(packageName, "-", "_")
	// Only annotate enums and messages that we intend to generate. In the
	// process we discover the external dependencies and trim the list of
	// packages used by this API.
	for _, e := range model.Enums {
		annotateEnum(e, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	}
	for _, m := range model.Messages {
		annotateMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	}
	hasLROs := false
	for _, s := range model.Services {
		for _, m := range s.Methods {
			if m.OperationInfo != nil {
				hasLROs = true
			}
			if !generateMethod(m) {
				continue
			}
			annotateMethod(m, s, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping, packageNamespace)
			if m := m.InputType; m != nil {
				annotateMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
			}
			if m := m.OutputType; m != nil {
				annotateMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
			}
		}
		annotateService(s, model, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	}

	servicesSubset := language.FilterSlice(model.Services, func(s *api.Service) bool {
		for _, m := range s.Methods {
			if generateMethod(m) {
				return true
			}
		}
		return false
	})

	// Delay this until the Codec had a chance to compute what packages are
	// used.
	findUsedPackages(model, c)
	ann := &modelAnnotations{
		PackageName:      packageName,
		PackageNamespace: packageNamespace,
		PackageVersion:   c.version,
		RequiredPackages: requiredPackages(outdir, c.extraPackages),
		ExternPackages:   externPackages(c.extraPackages),
		HasServices:      len(servicesSubset) > 0,
		HasLROs:          hasLROs,
		CopyrightYear:    c.generationYear,
		BoilerPlate: append(license.LicenseHeaderBulk(),
			"",
			" Code generated by sidekick. DO NOT EDIT."),
		DefaultHost: func() string {
			if len(model.Services) > 0 {
				return model.Services[0].DefaultHost
			}
			return ""
		}(),
		Services:          servicesSubset,
		NameToLower:       strings.ToLower(model.Name),
		NotForPublication: c.doNotPublish,
		IsWktCrate:        c.sourceSpecificationPackageName == "google.protobuf",
	}

	addStreamingFeature(ann, model, c.extraPackages)
	model.Codec = ann
	return ann, nil
}

func annotateService(s *api.Service, model *api.API, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) {
	// Some codecs skip some methods.
	methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
		return generateMethod(m)
	})
	hasLROs := false
	for _, m := range methods {
		if m.OperationInfo != nil {
			hasLROs = true
			break
		}
	}
	ann := &serviceAnnotations{
		Name:        toPascal(s.Name),
		ModuleName:  toSnake(s.Name),
		DocLines:    formatDocComments(s.Documentation, model.State, modulePath, sourceSpecificationPackageName, packageMapping),
		Methods:     methods,
		DefaultHost: s.DefaultHost,
		HasLROs:     hasLROs,
		APITitle:    model.Title,
	}
	s.Codec = ann
}

type fieldPartition struct {
	singularFields []*api.Field
	repeatedFields []*api.Field
	mapFields      []*api.Field
}

func partitionFields(fields []*api.Field, state *api.APIState) fieldPartition {
	isMap := func(f *api.Field) bool {
		if f.Typez != api.MESSAGE_TYPE {
			return false
		}
		if m, ok := state.MessageByID[f.TypezID]; ok {
			return m.IsMap
		}
		return false
	}
	isRepeated := func(f *api.Field) bool {
		return f.Repeated && !isMap(f)
	}
	return fieldPartition{
		singularFields: language.FilterSlice(fields, func(f *api.Field) bool {
			return !isRepeated(f) && !isMap(f)
		}),
		repeatedFields: language.FilterSlice(fields, func(f *api.Field) bool {
			return isRepeated(f)
		}),
		mapFields: language.FilterSlice(fields, func(f *api.Field) bool {
			return isMap(f)
		}),
	}
}

// annotateMessage annotates the message, its fields, its nested
// messages, and its nested enums.
func annotateMessage(m *api.Message, state *api.APIState, deserializeWithDefaults bool, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) {
	for _, f := range m.Fields {
		annotateField(f, m, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	for _, f := range m.OneOfs {
		annotateOneOf(f, m, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	for _, e := range m.Enums {
		annotateEnum(e, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	for _, child := range m.Messages {
		annotateMessage(child, state, deserializeWithDefaults, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	hasSyntheticFields := false
	for _, f := range m.Fields {
		if f.Synthetic {
			hasSyntheticFields = true
			break
		}
	}
	basicFields := language.FilterSlice(m.Fields, func(f *api.Field) bool {
		return !f.IsOneOf
	})
	partition := partitionFields(basicFields, state)
	m.Codec = &messageAnnotation{
		Name:               toPascal(m.Name),
		ModuleName:         toSnake(m.Name),
		QualifiedName:      fullyQualifiedMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping),
		SourceFQN:          strings.TrimPrefix(m.ID, "."),
		DocLines:           formatDocComments(m.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		MessageAttributes:  messageAttributes(deserializeWithDefaults),
		HasNestedTypes:     language.HasNestedTypes(m),
		BasicFields:        basicFields,
		SingularFields:     partition.singularFields,
		RepeatedFields:     partition.repeatedFields,
		MapFields:          partition.mapFields,
		HasSyntheticFields: hasSyntheticFields,
	}
}

func annotateMethod(m *api.Method, s *api.Service, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez, packageNamespace string) {
	pathInfoAnnotation := &pathInfoAnnotation{
		Method:        m.PathInfo.Verb,
		MethodToLower: strings.ToLower(m.PathInfo.Verb),
		PathFmt:       httpPathFmt(m.PathInfo),
		PathArgs:      httpPathArgs(m.PathInfo, m, state),
		HasBody:       m.PathInfo.BodyFieldPath != "",
	}
	pathInfoAnnotation.HasPathArgs = len(pathInfoAnnotation.PathArgs) > 0

	m.PathInfo.Codec = pathInfoAnnotation
	annotation := &methodAnnotation{
		Name:                strcase.ToSnake(m.Name),
		BuilderName:         toPascal(m.Name),
		BodyAccessor:        bodyAccessor(m),
		DocLines:            formatDocComments(m.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		PathInfo:            m.PathInfo,
		PathParams:          language.PathParams(m, state),
		QueryParams:         language.QueryParams(m, state),
		ServiceNameToPascal: toPascal(s.Name),
		ServiceNameToCamel:  toCamel(s.Name),
		ServiceNameToSnake:  toSnake(s.Name),
	}
	if m.OperationInfo != nil {
		metadataType := methodInOutTypeName(m.OperationInfo.MetadataTypeID, state, modulePath, sourceSpecificationPackageName, packageMapping)
		responseType := methodInOutTypeName(m.OperationInfo.ResponseTypeID, state, modulePath, sourceSpecificationPackageName, packageMapping)
		m.OperationInfo.Codec = &operationInfo{
			MetadataType:       metadataType,
			ResponseType:       responseType,
			MetadataTypeInDocs: strings.TrimPrefix(metadataType, "crate::"),
			ResponseTypeInDocs: strings.TrimPrefix(responseType, "crate::"),
			PackageNamespace:   packageNamespace,
		}
	}
	m.Codec = annotation
}

func annotateOneOf(oneof *api.OneOf, message *api.Message, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) {
	partition := partitionFields(oneof.Fields, state)
	scope := messageScopeName(message, "", modulePath, sourceSpecificationPackageName, packageMapping)
	enumName := toPascal(oneof.Name)
	fqEnumName := fmt.Sprintf("%s::%s", scope, enumName)
	oneof.Codec = &oneOfAnnotation{
		FieldName:      toSnake(oneof.Name),
		SetterName:     toSnakeNoMangling(oneof.Name),
		EnumName:       enumName,
		FQEnumName:     fqEnumName,
		FieldType:      fmt.Sprintf("%s::%s", scope, toPascal(oneof.Name)),
		DocLines:       formatDocComments(oneof.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		SingularFields: partition.singularFields,
		RepeatedFields: partition.repeatedFields,
		MapFields:      partition.mapFields,
	}
}

func annotateField(field *api.Field, message *api.Message, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) {
	ann := &fieldAnnotations{
		FieldName:          toSnake(field.Name),
		SetterName:         toSnakeNoMangling(field.Name),
		FQMessageName:      fullyQualifiedMessageName(message, modulePath, sourceSpecificationPackageName, packageMapping),
		BranchName:         toPascal(field.Name),
		DocLines:           formatDocComments(field.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		Attributes:         fieldAttributes(field, state),
		FieldType:          fieldType(field, state, false, modulePath, sourceSpecificationPackageName, packageMapping),
		PrimitiveFieldType: fieldType(field, state, true, modulePath, sourceSpecificationPackageName, packageMapping),
		AddQueryParameter:  addQueryParameter(field),
	}
	field.Codec = ann
	if field.Typez != api.MESSAGE_TYPE {
		return
	}
	mapMessage, ok := state.MessageByID[field.TypezID]
	if !ok || !mapMessage.IsMap {
		return
	}
	ann.KeyType = mapType(mapMessage.Fields[0], state, modulePath, sourceSpecificationPackageName, packageMapping)
	ann.ValueType = mapType(mapMessage.Fields[1], state, modulePath, sourceSpecificationPackageName, packageMapping)
}

func annotateEnum(e *api.Enum, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) {
	for _, ev := range e.Values {
		annotateEnumValue(ev, e, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	e.Codec = &enumAnnotation{
		Name:       enumName(e),
		ModuleName: toSnake(enumName(e)),
		DocLines:   formatDocComments(e.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
	}
}

func annotateEnumValue(ev *api.EnumValue, e *api.Enum, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*packagez) {
	ev.Codec = &enumValueAnnotation{
		DocLines: formatDocComments(ev.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		Name:     enumValueName(ev),
		EnumType: enumName(e),
	}
}
