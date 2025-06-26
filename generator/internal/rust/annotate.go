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
	// If true, at lease one service has a method we cannot wrap (yet).
	Incomplete bool
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
	// A set of all types involved in an LRO, whether used as metadata or
	// response.
	LROTypes []*api.Message
	APITitle string
	// If set, gate this service under a feature named `ModuleName`.
	PerServiceFeatures bool
	// If true, there is a handwritten client surface.
	HasVeneer bool
	// If true, the service has a method we cannot wrap (yet).
	Incomplete bool
}

// If true, this service includes methods that return long-running operations.
func (s *serviceAnnotations) HasLROs() bool {
	return len(s.LROTypes) > 0
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
	SourceFQN      string
	DocLines       []string
	HasNestedTypes bool
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
	QueryParams         []*api.Field
	BodyAccessor        string
	ServiceNameToPascal string
	ServiceNameToCamel  string
	ServiceNameToSnake  string
	OperationInfo       *operationInfo
	SystemParameters    []systemParameter
	ReturnType          string
	HasVeneer           bool
	Attributes          []string
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
	MetadataType     string
	ResponseType     string
	PackageNamespace string
}

func (info *operationInfo) OnlyMetadataIsEmpty() bool {
	return info.MetadataType == "wkt::Empty" && info.ResponseType != "wkt::Empty"
}

func (info *operationInfo) OnlyResponseIsEmpty() bool {
	return info.MetadataType != "wkt::Empty" && info.ResponseType == "wkt::Empty"
}

func (info *operationInfo) BothAreEmpty() bool {
	return info.MetadataType == "wkt::Empty" && info.ResponseType == "wkt::Empty"
}

func (info *operationInfo) NoneAreEmpty() bool {
	return info.MetadataType != "wkt::Empty" && info.ResponseType != "wkt::Empty"
}

type routingVariantAnnotations struct {
	FirstVariant     bool
	FieldAccessors   []string
	PrefixSegments   []string
	MatchingSegments []string
	SuffixSegments   []string
}

type bindingSubstitution struct {
	// Rust code to access the leaf field, given a `req`
	//
	// This field can be deeply nested. We need to capture code for the entire
	// chain. This accessor always returns an `Option<&T>`, even for fields
	// which are always present. This simplifies the mustache templates.
	//
	// The accessor should not
	// - copy any fields
	// - move any fields
	// - panic
	// - assume context i.e. use the try operator: `?`
	FieldAccessor string

	// The field name
	//
	// Nested fields are '.'-separated.
	//
	// e.g. "message_field.nested_field"
	FieldName string

	// The path template to match this substitution against
	//
	// e.g. ["projects", "*"]
	Template []string
}

// Rust code that yields an array of path segments.
//
// This array is supplied as an argument to `gaxi::path_parameter::try_match()`,
// and `gaxi::path_parameter::PathMismatchBuilder`.
//
// e.g.: `&[Segment::Literal("projects/"), Segment::SingleWildcard]`
func (s *bindingSubstitution) TemplateAsArray() string {
	return "&[" + strings.Join(annotateSegments(s.Template), ", ") + "]"
}

// The expected template, which can be used as a static string.
//
// e.g.: "projects/*"
func (s *bindingSubstitution) TemplateAsString() string {
	return strings.Join(s.Template, "/")
}

type pathBindingAnnotation struct {
	// The path format string for this binding
	//
	// e.g. "/v1/projects/{}/locations/{}"
	PathFmt string

	// The fields to be sent as query parameters for this binding
	QueryParams []*api.Field

	// The variables to be substituted into the path
	Substitutions []*bindingSubstitution
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
	// If true, it requires a serde_with::serde_as() transformation.
	SerdeAs string
	// If true, use `wkt::internal::is_default()` to skip the field
	SkipIfIsDefault bool
	// If true, this is a `wkt::Value` field, and requires super-extra custom
	// deserialization.
	IsWktValue bool
	// If true, this is a `wkt::NullValue` field, and also requires super-extra
	// custom deserialization.
	IsWktNullValue bool
}

func (a *fieldAnnotations) SkipIfIsEmpty() bool {
	return !a.SkipIfIsDefault
}

func (a *fieldAnnotations) RequiresSerdeAs() bool {
	return a.SerdeAs != ""
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
		return slices.ContainsFunc(s.Methods, func(m *api.Method) bool { return codec.generateMethod(m) })
	})
	// The maximum (15) was chosen more or less arbitrarily circa 2025-06. At
	// the time, only a handful of services exceeded this number of services.
	if len(servicesSubset) > 15 && !codec.perServiceFeatures {
		slog.Warn("package has more than 15 services, consider enabling per-service features", "package", packageName, "count", len(servicesSubset))
	}

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
		Incomplete: slices.ContainsFunc(model.Services, func(s *api.Service) bool {
			return slices.ContainsFunc(s.Methods, func(m *api.Method) bool { return !codec.generateMethod(m) })
		}),
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
	seenLROTypes := make(map[string]bool)
	var lroTypes []*api.Message
	for _, m := range methods {
		if m.OperationInfo != nil {
			if _, ok := seenLROTypes[m.OperationInfo.MetadataTypeID]; !ok {
				seenLROTypes[m.OperationInfo.MetadataTypeID] = true
				lroTypes = append(lroTypes, model.State.MessageByID[m.OperationInfo.MetadataTypeID])
			}
			if _, ok := seenLROTypes[m.OperationInfo.ResponseTypeID]; !ok {
				seenLROTypes[m.OperationInfo.ResponseTypeID] = true
				lroTypes = append(lroTypes, model.State.MessageByID[m.OperationInfo.ResponseTypeID])
			}
		}
	}
	serviceName := c.ServiceName(s)
	moduleName := toSnake(serviceName)
	ann := &serviceAnnotations{
		Name:              toPascal(serviceName),
		PackageModuleName: packageToModuleName(s.Package),
		ModuleName:        moduleName,
		DocLines: c.formatDocComments(
			s.Documentation, s.ID, model.State, []string{s.ID, s.Package}),
		Methods:            methods,
		DefaultHost:        s.DefaultHost,
		LROTypes:           lroTypes,
		APITitle:           model.Title,
		PerServiceFeatures: c.perServiceFeatures,
		HasVeneer:          c.hasVeneer,
		Incomplete:         slices.ContainsFunc(s.Methods, func(m *api.Method) bool { return !c.generateMethod(m) }),
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
		HasNestedTypes:     language.HasNestedTypes(m),
		BasicFields:        basicFields,
		HasSyntheticFields: hasSyntheticFields,
	}
}

func (c *codec) annotateMethod(m *api.Method, s *api.Service, state *api.APIState, sourceSpecificationPackageName string, packageNamespace string) {
	// TODO(#2317) - move to pathBindingAnnotation
	if len(m.PathInfo.Bindings) != 0 {
		pathInfoAnnotation := &pathInfoAnnotation{
			Method:        m.PathInfo.Bindings[0].Verb,
			MethodToLower: strings.ToLower(m.PathInfo.Bindings[0].Verb),
			PathFmt:       httpPathFmt(m.PathInfo.Bindings[0].PathTemplate),
			PathArgs:      httpPathArgs(m.PathInfo, m, state),
			HasBody:       m.PathInfo.BodyFieldPath != "",
		}
		pathInfoAnnotation.HasPathArgs = len(pathInfoAnnotation.PathArgs) > 0
		m.PathInfo.Codec = pathInfoAnnotation
	} else {
		// Even when there are no bindings, we still want a concrete
		// annotation, which we use to determine the default idempotency
		// of the method. An empty annotation yields `false`.
		m.PathInfo.Codec = &pathInfoAnnotation{}
	}

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

	for _, b := range m.PathInfo.Bindings {
		annotatePathBinding(b, m, state)
	}
	returnType := c.methodInOutTypeName(m.OutputTypeID, state, sourceSpecificationPackageName)
	if m.ReturnsEmpty {
		returnType = "()"
	}
	serviceName := c.ServiceName(s)
	// TODO(#2317) - move query params into pathBindingAnnotation
	var query_params []*api.Field
	if len(m.PathInfo.Bindings) != 0 {
		query_params = language.QueryParams(m, m.PathInfo.Bindings[0])
	}
	annotation := &methodAnnotation{
		Name:                strcase.ToSnake(m.Name),
		BuilderName:         toPascal(m.Name),
		BodyAccessor:        bodyAccessor(m),
		DocLines:            c.formatDocComments(m.Documentation, m.ID, state, s.Scopes()),
		PathInfo:            m.PathInfo,
		QueryParams:         query_params,
		ServiceNameToPascal: toPascal(serviceName),
		ServiceNameToCamel:  toCamel(serviceName),
		ServiceNameToSnake:  toSnake(serviceName),
		SystemParameters:    c.systemParameters,
		ReturnType:          returnType,
		HasVeneer:           c.hasVeneer,
	}
	if annotation.Name == "clone" {
		// Some methods look too similar to standard Rust traits. Clippy makes
		// a recommendation that is not applicable to generated code.
		annotation.Attributes = []string{"#[allow(clippy::should_implement_trait)]"}
	}
	if m.OperationInfo != nil {
		metadataType := c.methodInOutTypeName(m.OperationInfo.MetadataTypeID, state, sourceSpecificationPackageName)
		responseType := c.methodInOutTypeName(m.OperationInfo.ResponseTypeID, state, sourceSpecificationPackageName)
		m.OperationInfo.Codec = &operationInfo{
			MetadataType:     metadataType,
			ResponseType:     responseType,
			PackageNamespace: packageNamespace,
		}
	}
	m.Codec = annotation
}

func (c *codec) annotateRoutingAccessors(variant *api.RoutingInfoVariant, m *api.Method, state *api.APIState) []string {
	return makeAccessors(variant.FieldPath, m, state)
}

func makeAccessors(fields []string, m *api.Method, state *api.APIState) []string {
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
	for _, name := range fields {
		field := findField(name, message)
		if field == nil {
			slog.Error("invalid routing/path field for request message", "field", name, "message ID", message.ID)
			continue
		}
		if field.Optional {
			accessors = append(accessors, fmt.Sprintf(".and_then(|m| m.%s.as_ref())", name))
		} else {
			accessors = append(accessors, fmt.Sprintf(".map(|m| &m.%s)", name))
		}
		if field.Typez == api.STRING_TYPE {
			accessors = append(accessors, ".map(|s| s.as_str())")
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
	// The model may have multiple consecutive literal segments. We use this
	// buffer to consolidate them into a single literal segment.
	literalBuffer := ""
	flushBuffer := func() {
		if literalBuffer != "" {
			ann = append(ann, fmt.Sprintf(`Segment::Literal("%s")`, literalBuffer))
		}
		literalBuffer = ""
	}
	for index, segment := range segments {
		switch {
		case segment == api.RoutingMultiSegmentWildcard:
			flushBuffer()
			if len(segments) == 1 {
				ann = append(ann, "Segment::MultiWildcard")
			} else if len(segments) != index+1 {
				ann = append(ann, "Segment::MultiWildcard")
			} else {
				ann = append(ann, "Segment::TrailingMultiWildcard")
			}
		case segment == api.RoutingSingleSegmentWildcard:
			if index != 0 {
				literalBuffer += "/"
			}
			flushBuffer()
			ann = append(ann, "Segment::SingleWildcard")
		default:
			if index != 0 {
				literalBuffer += "/"
			}
			literalBuffer += segment
		}
	}
	flushBuffer()
	return ann
}

func makeBindingSubstitution(v *api.PathVariable, m *api.Method, state *api.APIState) bindingSubstitution {
	fieldAccessor := "Some(&req)"
	for _, a := range makeAccessors(v.FieldPath, m, state) {
		fieldAccessor += a
	}
	var segments []string
	for _, s := range v.Segments {
		if s.Literal != nil {
			segments = append(segments, *s.Literal)
		} else if s.Match != nil {
			segments = append(segments, "*")
		} else if s.MatchRecursive != nil {
			segments = append(segments, "**")
		}
	}

	return bindingSubstitution{
		FieldAccessor: fieldAccessor,
		FieldName:     strings.Join(v.FieldPath, "."),
		Template:      segments,
	}
}

func annotatePathBinding(b *api.PathBinding, m *api.Method, state *api.APIState) {
	var subs []*bindingSubstitution
	for _, s := range b.PathTemplate.Segments {
		if s.Variable != nil {
			sub := makeBindingSubstitution(s.Variable, m, state)
			subs = append(subs, &sub)
		}
	}
	b.Codec = &pathBindingAnnotation{
		PathFmt:       httpPathFmt(b.PathTemplate),
		QueryParams:   language.QueryParams(m, b),
		Substitutions: subs,
	}
}

func (c *codec) annotateOneOf(oneof *api.OneOf, message *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	scope := messageScopeName(message, "", c.modulePath, sourceSpecificationPackageName, c.packageMapping)
	enumName := c.OneOfEnumName(oneof)
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
		FieldType:           fmt.Sprintf("%s::%s", scope, enumName),
		DocLines:            c.formatDocComments(oneof.Documentation, oneof.ID, state, message.Scopes()),
	}
}

func (c *codec) primitiveSerdeAs(field *api.Field) string {
	switch field.Typez {
	case api.INT32_TYPE, api.SFIXED32_TYPE, api.SINT32_TYPE:
		return "wkt::internal::I32"
	case api.INT64_TYPE, api.SFIXED64_TYPE, api.SINT64_TYPE:
		return "wkt::internal::I64"
	case api.UINT32_TYPE, api.FIXED32_TYPE:
		return "wkt::internal::U32"
	case api.UINT64_TYPE, api.FIXED64_TYPE:
		return "wkt::internal::U64"
	case api.FLOAT_TYPE:
		return "wkt::internal::F32"
	case api.DOUBLE_TYPE:
		return "wkt::internal::F64"
	case api.BYTES_TYPE:
		return "serde_with::base64::Base64"
	default:
		return ""
	}
}

func (c *codec) mapKeySerdeAs(field *api.Field) string {
	if field.Typez == api.BOOL_TYPE {
		return "serde_with::DisplayFromStr"
	}
	return c.primitiveSerdeAs(field)
}

func (c *codec) mapValueSerdeAs(field *api.Field) string {
	if field.Typez == api.MESSAGE_TYPE {
		return c.messageFieldSerdeAs(field)
	}
	return c.primitiveSerdeAs(field)
}

func (c *codec) messageFieldSerdeAs(field *api.Field) string {
	switch field.TypezID {
	case ".google.protobuf.BytesValue":
		return "serde_with::base64::Base64"
	case ".google.protobuf.UInt64Value":
		return "wkt::internal::U64"
	case ".google.protobuf.Int64Value":
		return "wkt::internal::I64"
	case ".google.protobuf.UInt32Value":
		return "wkt::internal::U32"
	case ".google.protobuf.Int32Value":
		return "wkt::internal::I32"
	case ".google.protobuf.FloatValue":
		return "wkt::internal::F32"
	case ".google.protobuf.DoubleValue":
		return "wkt::internal::F64"
	case ".google.protobuf.BoolValue":
		return ""
	default:
		return ""
	}
}

func (c *codec) annotateField(field *api.Field, message *api.Message, state *api.APIState, sourceSpecificationPackageName string) {
	ann := &fieldAnnotations{
		FieldName:          toSnake(field.Name),
		SetterName:         toSnakeNoMangling(field.Name),
		FQMessageName:      fullyQualifiedMessageName(message, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		BranchName:         toPascal(field.Name),
		DocLines:           c.formatDocComments(field.Documentation, field.ID, state, message.Scopes()),
		FieldType:          fieldType(field, state, false, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		PrimitiveFieldType: fieldType(field, state, true, c.modulePath, sourceSpecificationPackageName, c.packageMapping),
		AddQueryParameter:  addQueryParameter(field),
		SerdeAs:            c.primitiveSerdeAs(field),
		SkipIfIsDefault:    field.Typez != api.STRING_TYPE && field.Typez != api.BYTES_TYPE,
		IsWktValue:         field.Typez == api.MESSAGE_TYPE && field.TypezID == ".google.protobuf.Value",
		IsWktNullValue:     field.Typez == api.ENUM_TYPE && field.TypezID == ".google.protobuf.NullValue",
	}
	if field.Recursive || (field.Typez == api.MESSAGE_TYPE && field.IsOneOf) {
		ann.IsBoxed = true
	}
	field.Codec = ann
	if field.Typez == api.MESSAGE_TYPE {
		if msg, ok := state.MessageByID[field.TypezID]; ok && msg.IsMap {
			if len(msg.Fields) != 2 {
				slog.Error("expected exactly two fields for map message", "field ID", field.ID, "map ID", field.TypezID)
			}
			ann.KeyField = msg.Fields[0]
			ann.KeyType = mapType(msg.Fields[0], state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
			ann.ValueField = msg.Fields[1]
			ann.ValueType = mapType(msg.Fields[1], state, c.modulePath, sourceSpecificationPackageName, c.packageMapping)
			key := c.mapKeySerdeAs(msg.Fields[0])
			value := c.mapValueSerdeAs(msg.Fields[1])
			if key != "" || value != "" {
				if key == "" {
					key = "serde_with::Same"
				}
				if value == "" {
					value = "serde_with::Same"
				}
				ann.SerdeAs = fmt.Sprintf("std::collections::HashMap<%s, %s>", key, value)
			}
		} else {
			ann.SerdeAs = c.messageFieldSerdeAs(field)
		}
	}
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
