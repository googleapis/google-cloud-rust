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
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	"github.com/iancoleman/strcase"
)

type RustTemplateData struct {
	Name              string
	Title             string
	Description       string
	PackageName       string
	PackageVersion    string
	PackageNamespace  string
	RequiredPackages  []string
	ExternPackages    []string
	HasServices       bool
	HasLROs           bool
	CopyrightYear     string
	BoilerPlate       []string
	Imports           []string
	DefaultHost       string
	Services          []*RustService
	Messages          []*api.Message
	Enums             []*api.Enum
	NameToLower       string
	NotForPublication bool
	HasFeatures       bool
	Features          []string
	// When bootstrapping the well-known types crate the templates add some
	// ad-hoc code.
	IsWktCrate bool
}

type RustService struct {
	Methods             []*RustMethod
	NameToSnake         string
	NameToPascal        string
	ServiceNameToPascal string
	NameToCamel         string
	ServiceName         string
	DocLines            []string
	DefaultHost         string
	// If true, this service includes methods that return long-running operations.
	HasLROs bool
}

type rustMessageAnnotation struct {
	Name          string
	ModuleName    string
	QualifiedName string
	// The FQN is the source specification
	SourceFQN         string
	MessageAttributes []string
	DocLines          []string
	HasNestedTypes    bool
	BasicFields       []*api.Field
	// If true, this is a synthetic message, some generation is skipped for
	// synthetic messages
	HasSyntheticFields bool
}

type RustMethod struct {
	NameToSnake         string
	NameToCamel         string
	NameToPascal        string
	DocLines            []string
	InputTypeName       string
	OutputTypeName      string
	PathInfo            *api.PathInfo
	PathParams          []*api.Field
	QueryParams         []*api.Field
	BodyAccessor        string
	IsPageable          bool
	ServiceNameToPascal string
	ServiceNameToCamel  string
	ServiceNameToSnake  string
	InputTypeID         string
	InputType           *api.Message
	OperationInfo       *RustOperationInfo
}

type rustPathInfoAnnotation struct {
	Method        string
	MethodToLower string
	PathFmt       string
	PathArgs      []string
	HasPathArgs   bool
	HasBody       bool
}

type RustOperationInfo struct {
	MetadataType       string
	ResponseType       string
	MetadataTypeInDocs string
	ResponseTypeInDocs string
	PackageNamespace   string
}

type rustOneOfAnnotation struct {
	// In Rust, `oneof` fields are fields inside a struct. These must be
	// `snake_case`. Possibly mangled with `r#` if the name is a Rust reserved
	// word.
	FieldName string
	// In Rust, each field gets a `set_{{FieldName}}` setter. These must be
	// `snake_case`, but are never mangled with a `r#` prefix.
	SetterName string
	// The `oneof` is represented by a Rust `enum`, these need to be `PascalCase`.
	EnumName              string
	NameToPascal          string
	NameToSnake           string
	NameToSnakeNoMangling string
	FieldType             string
	DocLines              []string
}

type rustFieldAnnotations struct {
	// In Rust, message fields are fields inside a struct. These must be
	// `snake_case`. Possibly mangled with `r#` if the name is a Rust reserved
	// word.
	FieldName string
	// In Rust, each fields gets a `set_{{FieldName}}` setter. These must be
	// `snake_case`, but are never mangled with a `r#` prefix.
	SetterName string
	// In Rust, fields that appear in a OneOf also appear as a enum branch.
	// These must be in `PascalCase`.
	BranchName         string
	DocLines           []string
	Attributes         []string
	FieldType          string
	PrimitiveFieldType string
	AddQueryParameter  string
}

type rustEnumAnnotation struct {
	Name       string
	ModuleName string
	DocLines   []string
}

type rustEnumValueAnnotation struct {
	Name     string
	EnumType string
	DocLines []string
}

// newRustTemplateData creates a struct used as input for Mustache templates.
// Fields and methods defined in this struct directly correspond to Mustache
// tags. For example, the Mustache tag {{#Services}} uses the
// [Template.Services] field.
func newRustTemplateData(model *api.API, c *rustCodec, outdir string) (*RustTemplateData, error) {
	c.hasServices = len(model.State.ServiceByID) > 0

	// Set the source package. We should always take the first service registered
	// as the source package. Services with mixins will register those after the
	// source package.
	if len(model.Services) > 0 {
		c.sourceSpecificationPackageName = model.Services[0].Package
	} else if len(model.Messages) > 0 {
		c.sourceSpecificationPackageName = model.Messages[0].Package
	}
	if err := rustValidate(model, c.sourceSpecificationPackageName); err != nil {
		return nil, err
	}

	rustLoadWellKnownTypes(model.State)
	rustResolveUsedPackages(model, c.extraPackages)
	packageName := rustPackageName(model, c.packageNameOverride)
	packageNamespace := strings.ReplaceAll(packageName, "-", "_")
	// Only annotate enums and messages that we intend to generate. In the
	// process we discover the external dependencies and trim the list of
	// packages used by this API.
	for _, e := range model.Enums {
		rustAnnotateEnum(e, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	}
	for _, m := range model.Messages {
		rustAnnotateMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	}
	data := &RustTemplateData{
		Name:             model.Name,
		Title:            model.Title,
		Description:      model.Description,
		PackageName:      packageName,
		PackageNamespace: packageNamespace,
		PackageVersion:   c.version,
		HasServices:      len(model.Services) > 0,
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
		Services: mapSlice(model.Services, func(s *api.Service) *RustService {
			return newRustService(s, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping, packageNamespace)
		}),
		Messages:          model.Messages,
		Enums:             model.Enums,
		NameToLower:       strings.ToLower(model.Name),
		NotForPublication: c.doNotPublish,
		IsWktCrate:        c.sourceSpecificationPackageName == "google.protobuf",
	}
	// Services without methods create a lot of warnings in Rust. The dead code
	// analysis is extremely good, and can determine that several types and
	// member variables are going unused.
	data.Services = filterSlice(data.Services, func(s *RustService) bool {
		return len(s.Methods) > 0
	})
	// Determine if any service has an LRO.
	for _, s := range data.Services {
		if s.HasLROs {
			data.HasLROs = true
			break
		}
	}

	// Delay this until the Codec had a chance to compute what packages are
	// used.
	data.RequiredPackages = rustRequiredPackages(outdir, c.extraPackages)
	data.ExternPackages = rustExternPackages(c.extraPackages)
	rustAddStreamingFeature(data, model, c.extraPackages)

	for _, s := range data.Services {
		for _, method := range s.Methods {
			if m, ok := model.State.MessageByID[method.InputTypeID]; ok {
				rustAnnotateMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
				method.InputType = m
			}
		}
	}
	return data, nil
}

func newRustService(s *api.Service, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage, packageNamespace string) *RustService {
	// Some codecs skip some methods.
	methods := filterSlice(s.Methods, func(m *api.Method) bool {
		return rustGenerateMethod(m)
	})
	hasLROs := false
	for _, m := range s.Methods {
		if m.OperationInfo != nil {
			hasLROs = true
			break
		}
	}
	return &RustService{
		Methods: mapSlice(methods, func(m *api.Method) *RustMethod {
			return newRustMethod(m, s, state, modulePath, sourceSpecificationPackageName, packageMapping, packageNamespace)
		}),
		NameToSnake:         rustToSnake(s.Name),
		NameToPascal:        rustToPascal(s.Name),
		ServiceNameToPascal: rustToPascal(s.Name), // Alias for clarity
		NameToCamel:         rustToCamel(s.Name),
		ServiceName:         s.Name,
		DocLines:            rustFormatDocComments(s.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		DefaultHost:         s.DefaultHost,
		HasLROs:             hasLROs,
	}
}

func rustAnnotateMessage(m *api.Message, state *api.APIState, deserializeWithDefaults bool, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) {
	hasSyntheticFields := false
	for _, f := range m.Fields {
		if f.Synthetic {
			hasSyntheticFields = true
		}
		rustAnnotateField(f, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	for _, f := range m.OneOfs {
		rustAnnotateOneOf(f, m, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	for _, e := range m.Enums {
		rustAnnotateEnum(e, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	for _, child := range m.Messages {
		rustAnnotateMessage(child, state, deserializeWithDefaults, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	m.Codec = &rustMessageAnnotation{
		Name:              rustToPascal(m.Name),
		ModuleName:        rustToSnake(m.Name),
		QualifiedName:     rustFQMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping),
		SourceFQN:         strings.TrimPrefix(m.ID, "."),
		DocLines:          rustFormatDocComments(m.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		MessageAttributes: rustMessageAttributes(deserializeWithDefaults),
		HasNestedTypes:    hasNestedTypes(m),
		BasicFields: filterSlice(m.Fields, func(s *api.Field) bool {
			return !s.IsOneOf
		}),
		HasSyntheticFields: hasSyntheticFields,
	}
}

func newRustMethod(m *api.Method, s *api.Service, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage, packageNamespace string) *RustMethod {
	pathInfoAnnotation := &rustPathInfoAnnotation{
		Method:        m.PathInfo.Verb,
		MethodToLower: strings.ToLower(m.PathInfo.Verb),
		PathFmt:       rustHTTPPathFmt(m.PathInfo),
		PathArgs:      rustHTTPPathArgs(m.PathInfo, m, state),
		HasBody:       m.PathInfo.BodyFieldPath != "",
	}
	pathInfoAnnotation.HasPathArgs = len(pathInfoAnnotation.PathArgs) > 0

	m.PathInfo.Codec = pathInfoAnnotation
	method := &RustMethod{
		BodyAccessor:        rustBodyAccessor(m),
		DocLines:            rustFormatDocComments(m.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		PathInfo:            m.PathInfo,
		InputTypeName:       rustMethodInOutTypeName(m.InputTypeID, state, modulePath, sourceSpecificationPackageName, packageMapping),
		NameToCamel:         strcase.ToCamel(m.Name),
		NameToPascal:        rustToPascal(m.Name),
		NameToSnake:         strcase.ToSnake(m.Name),
		OutputTypeName:      rustMethodInOutTypeName(m.OutputTypeID, state, modulePath, sourceSpecificationPackageName, packageMapping),
		PathParams:          PathParams(m, state),
		QueryParams:         QueryParams(m, state),
		IsPageable:          m.IsPageable,
		ServiceNameToPascal: rustToPascal(s.Name),
		ServiceNameToCamel:  rustToCamel(s.Name),
		ServiceNameToSnake:  rustToSnake(s.Name),
		InputTypeID:         m.InputTypeID,
	}
	if m.OperationInfo != nil {
		metadataType := rustMethodInOutTypeName(m.OperationInfo.MetadataTypeID, state, modulePath, sourceSpecificationPackageName, packageMapping)
		responseType := rustMethodInOutTypeName(m.OperationInfo.ResponseTypeID, state, modulePath, sourceSpecificationPackageName, packageMapping)
		method.OperationInfo = &RustOperationInfo{
			MetadataType:       metadataType,
			ResponseType:       responseType,
			MetadataTypeInDocs: strings.TrimPrefix(metadataType, "crate::"),
			ResponseTypeInDocs: strings.TrimPrefix(responseType, "crate::"),
			PackageNamespace:   packageNamespace,
		}
	}
	return method
}

func rustAnnotateOneOf(oneof *api.OneOf, message *api.Message, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) {
	scope := rustMessageScopeName(message, "", modulePath, sourceSpecificationPackageName, packageMapping)
	oneof.Codec = &rustOneOfAnnotation{
		FieldName:  rustToSnake(oneof.Name),
		SetterName: rustToSnakeNoMangling(oneof.Name),
		EnumName:   rustToPascal(oneof.Name),
		FieldType:  fmt.Sprintf("%s::%s", scope, rustToPascal(oneof.Name)),
		DocLines:   rustFormatDocComments(oneof.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
	}
}

func rustAnnotateField(field *api.Field, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) {
	if field == nil {
		return
	}
	field.Codec = &rustFieldAnnotations{
		FieldName:          rustToSnake(field.Name),
		SetterName:         rustToSnakeNoMangling(field.Name),
		BranchName:         rustToPascal(field.Name),
		DocLines:           rustFormatDocComments(field.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		Attributes:         rustFieldAttributes(field, state),
		FieldType:          rustFieldType(field, state, false, modulePath, sourceSpecificationPackageName, packageMapping),
		PrimitiveFieldType: rustFieldType(field, state, true, modulePath, sourceSpecificationPackageName, packageMapping),
		AddQueryParameter:  rustAddQueryParameter(field),
	}
}

func rustAnnotateEnum(e *api.Enum, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) {
	for _, ev := range e.Values {
		rustAnnotateEnumValue(ev, e, state, modulePath, sourceSpecificationPackageName, packageMapping)
	}
	e.Codec = &rustEnumAnnotation{
		Name:       rustEnumName(e),
		ModuleName: rustToSnake(rustEnumName(e)),
		DocLines:   rustFormatDocComments(e.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
	}
}

func rustAnnotateEnumValue(ev *api.EnumValue, e *api.Enum, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) {
	ev.Codec = &rustEnumValueAnnotation{
		DocLines: rustFormatDocComments(ev.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		Name:     rustEnumValueName(ev),
		EnumType: rustEnumName(e),
	}
}
