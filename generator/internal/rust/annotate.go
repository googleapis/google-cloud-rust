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

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	"github.com/iancoleman/strcase"
)

type modelAnnotations struct {
	PackageName      string
	PackageVersion   string
	ReleaseLevel     string
	PackageNamespace string
	RequiredPackages []string
	ExternPackages   []string
	HasServices      bool
	HasLROs          bool
	CopyrightYear    string
	BoilerPlate      []string
	DefaultHost      string
	DefaultHostShort string
	// Services without methods create a lot of warnings in Rust. The dead code
	// analysis is extremely good, and can determine that several types and
	// member variables are going unused if the service does not have any
	// generated methods. Filter out the services to the subset that will
	// produce at least one method.
	Services          []*api.Service
	NameToLower       string
	NotForPublication bool
	// When bootstrapping the well-known types crate the templates add some
	// ad-hoc code.
	IsWktCrate bool
	// If true, disable rustdoc warnings known to be triggered by our generated
	// documentation.
	DisabledRustdocWarnings []string
	// Sets the default system parameters
	DefaultSystemParameters []systemParameter
}

type serviceAnnotations struct {
	// The name of the service. The Rust naming conventions requires this to be
	// in `PascalCase`. Notably, names like `IAM` *must* become `Iam`, but
	// `IAMService` can stay unchanged.
	Name string
	// The source specification package name mapped to Rust modules. That is,
	// `google.service.v1` becomes `google::service::v1`.
	PackageModuleName string
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
	Name       string
	ModuleName string
	// The fully qualified name, including the `codec.modulePath` prefix. For
	// messages in external packages this includes the package name.
	QualifiedName string
	// The fully qualified name, relative to `codec.modulePath`. Typically this
	// is the `QualifiedName` with the `crate::model::` prefix removed.
	RelativeName string
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
	SystemParameters    []systemParameter
}

type pathInfoAnnotation struct {
	Method        string
	MethodToLower string
	PathFmt       string
	PathArgs      []pathArg
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
	QualifiedName string
	// The fully qualified name, relative to `codec.modulePath`. Typically this
	// is the `QualifiedName` with the `crate::model::` prefix removed.
	RelativeName string
	FieldType    string
	DocLines     []string
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
	// The templates need to generate different code for boxed fields.
	IsBoxed bool
	// Simplify the templates for Protobuf => sidekick type conversion.
	ToProto      string
	KeyToProto   string
	ValueToProto string
}

type enumAnnotation struct {
	Name        string
	ModuleName  string
	DocLines    []string
	UniqueNames []*api.EnumValue
	// The fully qualified name, including the `codec.modulePath`
	// (typically `crate::model::`) prefix. For external enums this is prefixed
	// by the external crate name.
	QualifiedName string
	// The fully qualified name, relative to `codec.modulePath`. Typically this
	// is the `QualifiedName` with the `crate::model::` prefix removed.
	RelativeName string
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
func annotateModel(model *api.API, codec *codec, outdir string) *modelAnnotations {
	codec.hasServices = len(model.State.ServiceByID) > 0

	loadWellKnownTypes(model.State)
	resolveUsedPackages(model, codec.extraPackages)
	packageName := PackageName(model, codec.packageNameOverride)
	packageNamespace := strings.ReplaceAll(packageName, "-", "_")
	// Only annotate enums and messages that we intend to generate. In the
	// process we discover the external dependencies and trim the list of
	// packages used by this API.
	for _, e := range model.Enums {
		codec.annotateEnum(e, model.State, model.PackageName)
	}
	for _, m := range model.Messages {
		codec.annotateMessage(m, model.State, model.PackageName)
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
			codec.annotateMethod(m, s, model.State, model.PackageName, packageNamespace)
			if m := m.InputType; m != nil {
				codec.annotateMessage(m, model.State, model.PackageName)
			}
			if m := m.OutputType; m != nil {
				codec.annotateMessage(m, model.State, model.PackageName)
			}
		}
		codec.annotateService(s, model)
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
	findUsedPackages(model, codec)
	defaultHost := func() string {
		if len(model.Services) > 0 {
			return model.Services[0].DefaultHost
		}
		return ""
	}()
	defaultHostShort := func() string {
		idx := strings.Index(defaultHost, ".")
		if idx == -1 {
			return defaultHost
		}
		return defaultHost[:idx]
	}()
	ann := &modelAnnotations{
		PackageName:      packageName,
		PackageNamespace: packageNamespace,
		PackageVersion:   codec.version,
		ReleaseLevel:     codec.releaseLevel,
		RequiredPackages: requiredPackages(outdir, codec.extraPackages),
		ExternPackages:   externPackages(codec.extraPackages),
		HasServices:      len(servicesSubset) > 0,
		HasLROs:          hasLROs,
		CopyrightYear:    codec.generationYear,
		BoilerPlate: append(license.LicenseHeaderBulk(),
			"",
			" Code generated by sidekick. DO NOT EDIT."),
		DefaultHost:             defaultHost,
		DefaultHostShort:        defaultHostShort,
		Services:                servicesSubset,
		NameToLower:             strings.ToLower(model.Name),
		NotForPublication:       codec.doNotPublish,
		IsWktCrate:              model.PackageName == "google.protobuf",
		DisabledRustdocWarnings: codec.disabledRustdocWarnings,
	}

	model.Codec = ann
	return ann
}

func (c *codec) annotateService(s *api.Service, model *api.API) {
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
	components := strings.Split(s.Package, ".")
	for i, c := range components {
		components[i] = toSnake(c)
	}
	ann := &serviceAnnotations{
		Name:              toPascal(s.Name),
		PackageModuleName: strings.Join(components, "::"),
		ModuleName:        toSnake(s.Name),
		DocLines: formatDocComments(
			s.Documentation, s.ID, model.State, c.modulePath, []string{s.ID, s.Package}, c.packageMapping),
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
func (c *codec) annotateMessage(m *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	for _, f := range m.Fields {
		c.annotateField(f, m, state, sourceSpecificationPackageName)
	}
	for _, f := range m.OneOfs {
		c.annotateOneOf(f, m, state, sourceSpecificationPackageName)
	}
	for _, e := range m.Enums {
		c.annotateEnum(e, state, sourceSpecificationPackageName)
	}
	for _, child := range m.Messages {
		c.annotateMessage(child, state, sourceSpecificationPackageName)
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
	qualifiedName := fullyQualifiedMessageName(m, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	relativeName := strings.TrimPrefix(qualifiedName, c.modulePath+"::")
	m.Codec = &messageAnnotation{
		Name:               toPascal(m.Name),
		ModuleName:         toSnake(m.Name),
		QualifiedName:      qualifiedName,
		RelativeName:       relativeName,
		SourceFQN:          strings.TrimPrefix(m.ID, "."),
		DocLines:           formatDocComments(m.Documentation, m.ID, state, c.modulePath, m.Scopes(), c.packageMapping),
		MessageAttributes:  messageAttributes(),
		HasNestedTypes:     language.HasNestedTypes(m),
		BasicFields:        basicFields,
		SingularFields:     partition.singularFields,
		RepeatedFields:     partition.repeatedFields,
		MapFields:          partition.mapFields,
		HasSyntheticFields: hasSyntheticFields,
	}
}

func (c *codec) annotateMethod(m *api.Method, s *api.Service, state *api.APIState, sourceSpecificationPackageName string, packageNamespace string) {
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
		DocLines:            formatDocComments(m.Documentation, m.ID, state, c.modulePath, s.Scopes(), c.packageMapping),
		PathInfo:            m.PathInfo,
		PathParams:          language.PathParams(m, state),
		QueryParams:         language.QueryParams(m, state),
		ServiceNameToPascal: toPascal(s.Name),
		ServiceNameToCamel:  toCamel(s.Name),
		ServiceNameToSnake:  toSnake(s.Name),
		SystemParameters:    c.systemParameters,
	}
	if m.OperationInfo != nil {
		metadataType := methodInOutTypeName(m.OperationInfo.MetadataTypeID, state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
		responseType := methodInOutTypeName(m.OperationInfo.ResponseTypeID, state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
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

func (c *codec) annotateOneOf(oneof *api.OneOf, message *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	partition := partitionFields(oneof.Fields, state)
	scope := messageScopeName(message, "", c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	enumName := toPascal(oneof.Name)
	qualifiedName := fmt.Sprintf("%s::%s", scope, enumName)
	relativeEnumName := strings.TrimPrefix(qualifiedName, c.modulePath+"::")
	oneof.Codec = &oneOfAnnotation{
		FieldName:      toSnake(oneof.Name),
		SetterName:     toSnakeNoMangling(oneof.Name),
		EnumName:       enumName,
		QualifiedName:  qualifiedName,
		RelativeName:   relativeEnumName,
		FieldType:      fmt.Sprintf("%s::%s", scope, toPascal(oneof.Name)),
		DocLines:       formatDocComments(oneof.Documentation, oneof.ID, state, c.modulePath, message.Scopes(), c.packageMapping),
		SingularFields: partition.singularFields,
		RepeatedFields: partition.repeatedFields,
		MapFields:      partition.mapFields,
	}
}

func (c *codec) annotateField(field *api.Field, message *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	ann := &fieldAnnotations{
		FieldName:          toSnake(field.Name),
		SetterName:         toSnakeNoMangling(field.Name),
		FQMessageName:      fullyQualifiedMessageName(message, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		BranchName:         toPascal(field.Name),
		DocLines:           formatDocComments(field.Documentation, field.ID, state, c.modulePath, message.Scopes(), c.packageMapping),
		Attributes:         fieldAttributes(field, state),
		FieldType:          fieldType(field, state, false, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		PrimitiveFieldType: fieldType(field, state, true, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		AddQueryParameter:  addQueryParameter(field),
		ToProto:            toProto(field),
	}
	if field.Recursive || (field.Typez == api.MESSAGE_TYPE && field.IsOneOf) {
		ann.IsBoxed = true
	}
	field.Codec = ann
	if field.Typez != api.MESSAGE_TYPE {
		return
	}
	mapMessage, ok := state.MessageByID[field.TypezID]
	if !ok || !mapMessage.IsMap {
		return
	}
	ann.KeyType = mapType(mapMessage.Fields[0], state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	ann.ValueType = mapType(mapMessage.Fields[1], state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	ann.KeyToProto = toProto(mapMessage.Fields[0])
	ann.ValueToProto = toProto(mapMessage.Fields[1])
}

func (c *codec) annotateEnum(e *api.Enum, state *api.APIState, sourceSpecificationPackageName string) {
	for _, ev := range e.Values {
		c.annotateEnumValue(ev, e, state)
	}
	// For BigQuery (and so far only BigQuery), the enum values conflict when
	// converted to the Rust style [1]. Basically, there are several enum values
	// in this service that differ only in case, such as `FULL` vs. `full`.
	//
	// We create a list with the duplicates removed to avoid conflicts in the
	// generated code.
	//
	// [1]: Both Rust and Protobuf use `SCREAMING_SNAKE_CASE` for these, but
	//      some services do not follow the Protobuf convention.
	seen := map[string]*api.EnumValue{}
	var unique []*api.EnumValue
	for _, ev := range e.Values {
		name := enumValueName(ev)
		if existing, ok := seen[name]; ok {
			if existing.Number != ev.Number {
				slog.Warn("conflicting names for enum values", "enum.ID", e.ID)
			}
		} else {
			unique = append(unique, ev)
			seen[name] = ev
		}
	}
	qualifiedName := fullyQualifiedEnumName(e, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	relativeName := strings.TrimPrefix(qualifiedName, c.modulePath+"::")
	e.Codec = &enumAnnotation{
		Name:          enumName(e),
		ModuleName:    toSnake(enumName(e)),
		DocLines:      formatDocComments(e.Documentation, e.ID, state, c.modulePath, e.Scopes(), c.packageMapping),
		UniqueNames:   unique,
		QualifiedName: qualifiedName,
		RelativeName:  relativeName,
	}
}

func (c *codec) annotateEnumValue(ev *api.EnumValue, e *api.Enum, state *api.APIState) {
	ev.Codec = &enumValueAnnotation{
		DocLines: formatDocComments(ev.Documentation, ev.ID, state, c.modulePath, ev.Scopes(), c.packageMapping),
		Name:     enumValueName(ev),
		EnumType: enumName(e),
	}
}
