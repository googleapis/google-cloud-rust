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
	"slices"
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
	// Enables per-service features
	PerServiceFeatures bool
}

// HasServices returns true if there are any services in the model
func (m *modelAnnotations) HasServices() bool {
	return len(m.Services) > 0
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
	// If set, gate this service under a feature named `ModuleName`.
	PerServiceFeatures bool
	// If set, skip the builder documentation.
	SkipBuilderDocs bool
}

func (a *serviceAnnotations) FeatureName() string {
	return strcase.ToKebab(a.ModuleName)
}

func (a *messageAnnotation) MultiFeatureGates() bool {
	return len(a.FeatureGates) > 1
}

func (a *enumAnnotation) MultiFeatureGates() bool {
	return len(a.FeatureGates) > 1
}

func (a *oneOfAnnotation) MultiFeatureGates() bool {
	return len(a.FeatureGates) > 1
}

func (a *messageAnnotation) SingleFeatureGate() bool {
	return len(a.FeatureGates) == 1
}

func (a *enumAnnotation) SingleFeatureGate() bool {
	return len(a.FeatureGates) == 1
}

func (a *oneOfAnnotation) SingleFeatureGate() bool {
	return len(a.FeatureGates) == 1
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
	// The package name mapped to Rust modules. That is, `google.service.v1`
	// becomes `google::service::v1`.
	PackageModuleName string
	// The FQN is the source specification
	SourceFQN         string
	MessageAttributes []string
	DocLines          []string
	HasNestedTypes    bool
	// All the fields except OneOfs.
	BasicFields []*api.Field
	// If true, this is a synthetic message, some generation is skipped for
	// synthetic messages
	HasSyntheticFields bool
	// If set, this message is only enabled when some features are enabled.
	FeatureGates   []string
	FeatureGatesOp string
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
	ReturnType          string
	SkipBuilderDocs     bool
}

type pathInfoAnnotation struct {
	Method        string
	MethodToLower string
	PathFmt       string
	PathArgs      []pathArg
	HasPathArgs   bool
	HasBody       bool
}

// Returns true if the HTTP request requires a payload. This is relevant for
// POST and PUT requests that do not have a body parameter.
func (a *pathInfoAnnotation) RequiresContentLength() bool {
	return a.Method == "POST" || a.Method == "PUT"
}

type operationInfo struct {
	MetadataType       string
	ResponseType       string
	MetadataTypeInDocs string
	ResponseTypeInDocs string
	PackageNamespace   string
}

type routingVariantAnnotations struct {
	FirstVariant     bool
	FieldAccessors   []string
	PrefixSegments   []string
	MatchingSegments []string
	SuffixSegments   []string
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
	// The Rust `struct` that contains this oneof, fully qualified
	StructQualifiedName string
	FieldType           string
	DocLines            []string
	// If set, this enum is only enabled when some features are enabled.
	FeatureGates   []string
	FeatureGatesOp string
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
	KeyType    string
	KeyField   *api.Field
	ValueType  string
	ValueField *api.Field
	// The templates need to generate different code for boxed fields.
	IsBoxed bool
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
	// If set, this enum is only enabled when some features are enabled
	FeatureGates   []string
	FeatureGatesOp string
}

type enumValueAnnotation struct {
	Name        string
	VariantName string
	EnumType    string
	DocLines    []string
}

// annotateModel creates a struct used as input for Mustache templates.
// Fields and methods defined in this struct directly correspond to Mustache
// tags. For example, the Mustache tag {{#Services}} uses the
// [Template.Services] field.
func annotateModel(model *api.API, codec *codec) *modelAnnotations {
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
			if !codec.generateMethod(m) {
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
			if codec.generateMethod(m) {
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
		RequiredPackages: requiredPackages(codec.extraPackages),
		ExternPackages:   externPackages(codec.extraPackages),
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
		PerServiceFeatures:      codec.perServiceFeatures && len(servicesSubset) > 0,
	}

	codec.addFeatureAnnotations(model, ann)

	model.Codec = ann
	return ann
}

func (c *codec) addFeatureAnnotations(model *api.API, ann *modelAnnotations) {
	if !c.perServiceFeatures {
		return
	}
	var allFeatures []string
	for _, service := range ann.Services {
		svcAnn := service.Codec.(*serviceAnnotations)
		allFeatures = append(allFeatures, svcAnn.FeatureName())
		deps := api.FindServiceDependencies(model, service.ID)
		for _, id := range deps.Enums {
			enum, ok := model.State.EnumByID[id]
			// Some messages are not annotated (e.g. external messages).
			if !ok || enum.Codec == nil {
				continue
			}
			annotation := enum.Codec.(*enumAnnotation)
			annotation.FeatureGates = append(annotation.FeatureGates, svcAnn.FeatureName())
			slices.Sort(annotation.FeatureGates)
			annotation.FeatureGatesOp = "any"
		}
		for _, id := range deps.Messages {
			msg, ok := model.State.MessageByID[id]
			// Some messages are not annotated (e.g. external messages).
			if !ok || msg.Codec == nil {
				continue
			}
			annotation := msg.Codec.(*messageAnnotation)
			annotation.FeatureGates = append(annotation.FeatureGates, svcAnn.FeatureName())
			slices.Sort(annotation.FeatureGates)
			annotation.FeatureGatesOp = "any"
			for _, one := range msg.OneOfs {
				if one.Codec == nil {
					continue
				}
				annotation := one.Codec.(*oneOfAnnotation)
				annotation.FeatureGates = append(annotation.FeatureGates, svcAnn.FeatureName())
				slices.Sort(annotation.FeatureGates)
				annotation.FeatureGatesOp = "any"
			}
		}
	}
	// Rarely, some messages and enums are not used by any service. These
	// will lack any feature gates, but may depend on messages that do.
	// Change them to work only if all features are enabled.
	slices.Sort(allFeatures)
	for _, msg := range model.State.MessageByID {
		if msg.Codec == nil {
			continue
		}
		annotation := msg.Codec.(*messageAnnotation)
		if len(annotation.FeatureGates) > 0 {
			continue
		}
		annotation.FeatureGatesOp = "all"
		annotation.FeatureGates = allFeatures
	}
	for _, enum := range model.State.EnumByID {
		if enum.Codec == nil {
			continue
		}
		annotation := enum.Codec.(*enumAnnotation)
		if len(annotation.FeatureGates) > 0 {
			continue
		}
		annotation.FeatureGatesOp = "all"
		annotation.FeatureGates = allFeatures
	}
}

// Maps "google.foo.v1" to "google::foo::v1"
func packageToModuleName(p string) string {
	components := strings.Split(p, ".")
	for i, c := range components {
		components[i] = toSnake(c)
	}
	return strings.Join(components, "::")
}

func (c *codec) annotateService(s *api.Service, model *api.API) {
	// Some codecs skip some methods.
	methods := language.FilterSlice(s.Methods, func(m *api.Method) bool {
		return c.generateMethod(m)
	})
	hasLROs := false
	for _, m := range methods {
		if m.OperationInfo != nil {
			hasLROs = true
			break
		}
	}
	moduleName := toSnake(s.Name)
	ann := &serviceAnnotations{
		Name:              toPascal(s.Name),
		PackageModuleName: packageToModuleName(s.Package),
		ModuleName:        moduleName,
		DocLines: c.formatDocComments(
			s.Documentation, s.ID, model.State, []string{s.ID, s.Package}),
		Methods:            methods,
		DefaultHost:        s.DefaultHost,
		HasLROs:            hasLROs,
		APITitle:           model.Title,
		PerServiceFeatures: c.perServiceFeatures,
		SkipBuilderDocs:    c.skipBuilderDocs,
	}
	s.Codec = ann
}

// annotateMessage annotates the message, its fields, its nested
// messages, and its nested enums.
func (c *codec) annotateMessage(m *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	for _, f := range m.Fields {
		c.annotateField(f, m, state, sourceSpecificationPackageName)
	}
	for _, o := range m.OneOfs {
		c.annotateOneOf(o, m, state, sourceSpecificationPackageName)
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
	qualifiedName := fullyQualifiedMessageName(m, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	relativeName := strings.TrimPrefix(qualifiedName, c.modulePath+"::")
	m.Codec = &messageAnnotation{
		Name:               toPascal(m.Name),
		ModuleName:         toSnake(m.Name),
		QualifiedName:      qualifiedName,
		RelativeName:       relativeName,
		PackageModuleName:  packageToModuleName(m.Package),
		SourceFQN:          strings.TrimPrefix(m.ID, "."),
		DocLines:           c.formatDocComments(m.Documentation, m.ID, state, m.Scopes()),
		MessageAttributes:  messageAttributes(),
		HasNestedTypes:     language.HasNestedTypes(m),
		BasicFields:        basicFields,
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

	for _, routing := range m.Routing {
		for index, variant := range routing.Variants {
			routingVariantAnnotations := &routingVariantAnnotations{
				FirstVariant:     index == 0,
				FieldAccessors:   c.annotateRoutingAccessors(variant, m, state),
				PrefixSegments:   annotateSegments(variant.Prefix.Segments),
				MatchingSegments: annotateSegments(variant.Matching.Segments),
				SuffixSegments:   annotateSegments(variant.Suffix.Segments),
			}
			variant.Codec = routingVariantAnnotations
		}
	}

	m.PathInfo.Codec = pathInfoAnnotation
	returnType := c.methodInOutTypeName(m.OutputTypeID, state, sourceSpecificationPackageName)
	if m.ReturnsEmpty {
		returnType = "()"
	}
	annotation := &methodAnnotation{
		Name:                strcase.ToSnake(m.Name),
		BuilderName:         toPascal(m.Name),
		BodyAccessor:        bodyAccessor(m),
		DocLines:            c.formatDocComments(m.Documentation, m.ID, state, s.Scopes()),
		PathInfo:            m.PathInfo,
		PathParams:          language.PathParams(m, state),
		QueryParams:         language.QueryParams(m, state),
		ServiceNameToPascal: toPascal(s.Name),
		ServiceNameToCamel:  toCamel(s.Name),
		ServiceNameToSnake:  toSnake(s.Name),
		SystemParameters:    c.systemParameters,
		ReturnType:          returnType,
		SkipBuilderDocs:     c.skipBuilderDocs,
	}
	if m.OperationInfo != nil {
		metadataType := c.methodInOutTypeName(m.OperationInfo.MetadataTypeID, state, sourceSpecificationPackageName)
		responseType := c.methodInOutTypeName(m.OperationInfo.ResponseTypeID, state, sourceSpecificationPackageName)
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

func (c *codec) annotateRoutingAccessors(variant *api.RoutingInfoVariant, m *api.Method, state *api.APIState) []string {
	findField := func(name string, message *api.Message) *api.Field {
		for _, f := range message.Fields {
			if f.Name == name {
				return f
			}
		}
		return nil
	}
	var accessors []string
	message := m.InputType
	for _, name := range variant.FieldPath {
		field := findField(name, message)
		if field == nil {
			slog.Error("invalid routing field for request message", "field", name, "message ID", message.ID)
			continue
		}
		switch {
		case field.Optional:
			accessors = append(accessors, fmt.Sprintf(".and_then(|v| v.%s.as_ref())", name))
		case field.Typez == api.STRING_TYPE:
			accessors = append(accessors, fmt.Sprintf(".map(|v| v.%s.as_str())", name))
		default:
			accessors = append(accessors, fmt.Sprintf(".map(|v| &v.%s)", name))
		}
		if field.Typez == api.MESSAGE_TYPE {
			if fieldMessage, ok := state.MessageByID[field.TypezID]; ok {
				message = fieldMessage
			}
		}
	}
	return accessors
}

func annotateSegments(segments []string) []string {
	var ann []string
	for index, segment := range segments {
		switch {
		case segment == api.RoutingMultiSegmentWildcard:
			if len(segments) == 1 {
				ann = append(ann, "Segment::MultiWildcard")
			} else if len(segments) != index+1 {
				ann = append(ann, "Segment::MultiWildcard")
			} else {
				ann = append(ann, "Segment::TrailingMultiWildcard")
			}
		case segment == api.RoutingSingleSegmentWildcard:
			if index != 0 {
				ann = append(ann, `Segment::Literal("/")`)
			}
			ann = append(ann, "Segment::SingleWildcard")
		default:
			if index != 0 {
				ann = append(ann, `Segment::Literal("/")`)
			}
			ann = append(ann, fmt.Sprintf(`Segment::Literal("%s")`, segment))
		}
	}
	return ann
}

func (c *codec) annotateOneOf(oneof *api.OneOf, message *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	scope := messageScopeName(message, "", c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	enumName := toPascal(oneof.Name)
	qualifiedName := fmt.Sprintf("%s::%s", scope, enumName)
	relativeEnumName := strings.TrimPrefix(qualifiedName, c.modulePath+"::")
	structQualifiedName := fullyQualifiedMessageName(message, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	oneof.Codec = &oneOfAnnotation{
		FieldName:           toSnake(oneof.Name),
		SetterName:          toSnakeNoMangling(oneof.Name),
		EnumName:            enumName,
		QualifiedName:       qualifiedName,
		RelativeName:        relativeEnumName,
		StructQualifiedName: structQualifiedName,
		FieldType:           fmt.Sprintf("%s::%s", scope, toPascal(oneof.Name)),
		DocLines:            c.formatDocComments(oneof.Documentation, oneof.ID, state, message.Scopes()),
	}
}

func (c *codec) annotateField(field *api.Field, message *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	ann := &fieldAnnotations{
		FieldName:          toSnake(field.Name),
		SetterName:         toSnakeNoMangling(field.Name),
		FQMessageName:      fullyQualifiedMessageName(message, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		BranchName:         toPascal(field.Name),
		DocLines:           c.formatDocComments(field.Documentation, field.ID, state, message.Scopes()),
		Attributes:         fieldAttributes(field, state),
		FieldType:          fieldType(field, state, false, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		PrimitiveFieldType: fieldType(field, state, true, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		AddQueryParameter:  addQueryParameter(field),
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
	ann.KeyField = mapMessage.Fields[0]
	ann.KeyType = mapType(mapMessage.Fields[0], state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	ann.ValueField = mapMessage.Fields[1]
	ann.ValueType = mapType(mapMessage.Fields[1], state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
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
		name := enumValueVariantName(ev)
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
		DocLines:      c.formatDocComments(e.Documentation, e.ID, state, e.Scopes()),
		UniqueNames:   unique,
		QualifiedName: qualifiedName,
		RelativeName:  relativeName,
	}
}

func (c *codec) annotateEnumValue(ev *api.EnumValue, e *api.Enum, state *api.APIState) {
	ev.Codec = &enumValueAnnotation{
		DocLines:    c.formatDocComments(ev.Documentation, ev.ID, state, ev.Scopes()),
		Name:        enumValueName(ev),
		EnumType:    enumName(e),
		VariantName: enumValueVariantName(ev),
	}
}

// Returns "true" if the method is idempotent by default, and "false", if not.
func (p *pathInfoAnnotation) IsIdempotent() string {
	if p.Method == "GET" || p.Method == "PUT" || p.Method == "DELETE" {
		return "true"
	}
	return "false"
}
