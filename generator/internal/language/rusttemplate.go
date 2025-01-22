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
	Messages          []*RustMessage
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

type RustMessage struct {
	Fields             []*api.Field
	BasicFields        []*api.Field
	ExplicitOneOfs     []*api.OneOf
	NestedMessages     []*RustMessage
	Enums              []*api.Enum
	MessageAttributes  []string
	Name               string
	QualifiedName      string
	NameSnakeCase      string
	HasNestedTypes     bool
	DocLines           []string
	IsMap              bool
	IsPageableResponse bool
	PageableItem       *api.Field
	ID                 string
	// The FQN is the source specification
	SourceFQN string
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
	InputType           *RustMessage
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

type RustOneOfAnnotation struct {
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

type RustFieldAnnotations struct {
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
	for _, e := range model.State.EnumByID {
		rustAnnotateEnum(e, model.State, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
	}
	packageName := rustPackageName(model, c.packageNameOverride)
	packageNamespace := strings.ReplaceAll(packageName, "-", "_")
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
		Messages: mapSlice(model.Messages, func(m *api.Message) *RustMessage {
			return newRustMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
		}),
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

	messagesByID := map[string]*RustMessage{}
	for _, m := range data.Messages {
		messagesByID[m.ID] = m
	}
	for _, s := range data.Services {
		for _, method := range s.Methods {
			if msg, ok := messagesByID[method.InputTypeID]; ok {
				method.InputType = msg
			} else if m, ok := model.State.MessageByID[method.InputTypeID]; ok {
				method.InputType = newRustMessage(m, model.State, c.deserializeWithdDefaults, c.modulePath, c.sourceSpecificationPackageName, c.packageMapping)
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
			return newRustMethod(m, state, modulePath, sourceSpecificationPackageName, packageMapping, packageNamespace)
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

func newRustMessage(m *api.Message, state *api.APIState, deserializeWithDefaults bool, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) *RustMessage {
	hasSyntheticFields := false
	for _, f := range m.Fields {
		if f.Synthetic {
			hasSyntheticFields = true
			break
		}
	}
	fields := mapSlice(m.Fields, func(s *api.Field) *api.Field {
		return newRustField(s, state, modulePath, sourceSpecificationPackageName, packageMapping)
	})
	return &RustMessage{
		Fields: fields,
		BasicFields: filterSlice(fields, func(s *api.Field) bool {
			return !s.IsOneOf
		}),
		ExplicitOneOfs: mapSlice(m.OneOfs, func(s *api.OneOf) *api.OneOf {
			return newRustOneOf(s, state, modulePath, sourceSpecificationPackageName, packageMapping)
		}),
		NestedMessages: mapSlice(m.Messages, func(s *api.Message) *RustMessage {
			return newRustMessage(s, state, deserializeWithDefaults, modulePath, sourceSpecificationPackageName, packageMapping)
		}),
		Enums:             m.Enums,
		MessageAttributes: rustMessageAttributes(deserializeWithDefaults),
		Name:              rustToPascal(m.Name),
		QualifiedName:     rustFQMessageName(m, modulePath, sourceSpecificationPackageName, packageMapping),
		NameSnakeCase:     rustToSnake(m.Name),
		HasNestedTypes: func() bool {
			if len(m.Enums) > 0 || len(m.OneOfs) > 0 {
				return true
			}
			for _, child := range m.Messages {
				if !child.IsMap {
					return true
				}
			}
			return false
		}(),
		DocLines:           rustFormatDocComments(m.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		IsMap:              m.IsMap,
		IsPageableResponse: m.IsPageableResponse,
		PageableItem:       newRustField(m.PageableItem, state, modulePath, sourceSpecificationPackageName, packageMapping),
		ID:                 m.ID,
		SourceFQN:          strings.TrimPrefix(m.ID, "."),
		HasSyntheticFields: hasSyntheticFields,
	}
}

func newRustMethod(m *api.Method, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage, packageNamespace string) *RustMethod {
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
		ServiceNameToPascal: rustToPascal(m.Parent.Name),
		ServiceNameToCamel:  rustToCamel(m.Parent.Name),
		ServiceNameToSnake:  rustToSnake(m.Parent.Name),
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

func newRustOneOf(oneOf *api.OneOf, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) *api.OneOf {
	oneOf.Codec = &RustOneOfAnnotation{
		FieldName:  rustToSnake(oneOf.Name),
		SetterName: rustToSnakeNoMangling(oneOf.Name),
		EnumName:   rustToPascal(oneOf.Name),
		FieldType:  rustOneOfType(oneOf, modulePath, sourceSpecificationPackageName, packageMapping),
		DocLines:   rustFormatDocComments(oneOf.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
	}
	return oneOf
}

func newRustField(field *api.Field, state *api.APIState, modulePath, sourceSpecificationPackageName string, packageMapping map[string]*rustPackage) *api.Field {
	if field == nil {
		return nil
	}
	field.Codec = &RustFieldAnnotations{
		FieldName:          rustToSnake(field.Name),
		SetterName:         rustToSnakeNoMangling(field.Name),
		BranchName:         rustToPascal(field.Name),
		DocLines:           rustFormatDocComments(field.Documentation, state, modulePath, sourceSpecificationPackageName, packageMapping),
		Attributes:         rustFieldAttributes(field, state),
		FieldType:          rustFieldType(field, state, false, modulePath, sourceSpecificationPackageName, packageMapping),
		PrimitiveFieldType: rustFieldType(field, state, true, modulePath, sourceSpecificationPackageName, packageMapping),
		AddQueryParameter:  rustAddQueryParameter(field),
	}
	return field
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
