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

// Package openapi reads OpenAPI v3 specifications and converts them into
// the `genclient.API` model.
package openapi

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/pb33f/libopenapi"
	"github.com/pb33f/libopenapi/datamodel/high/base"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
	"github.com/pb33f/libopenapi/orderedmap"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
)

type Parser struct {
}

func NewParser() *Parser {
	return &Parser{}
}

func (t *Parser) Help() string {
	return "Parse a single OpenAPI v3 JSON file into an API specification."
}

func (t *Parser) OptionDescriptions() map[string]string {
	return map[string]string{
		// At the moment, this parser has no options, but we may consider
		//   - package-name: inject a name for all APIs
		//   - lro-mapping: treat the "Foo" message as "google.longrunning.Operation"
	}
}

func (t *Parser) Parse(opts genclient.ParserOptions) (*genclient.API, error) {
	contents, err := os.ReadFile(opts.Source)
	if err != nil {
		return nil, err
	}
	model, err := createDocModel(contents)
	if err != nil {
		return nil, err
	}
	var serviceConfig *serviceconfig.Service
	if opts.ServiceConfig != "" {
		cfg, err := genclient.ReadServiceConfig(opts.ServiceConfig)
		if err != nil {
			return nil, err
		}
		serviceConfig = cfg
	}
	// Translates OpenAPI specification into a [genclient.GenerateRequest].
	return makeAPI(serviceConfig, model)
}

func createDocModel(contents []byte) (*libopenapi.DocumentModel[v3.Document], error) {
	document, err := libopenapi.NewDocument(contents)
	if err != nil {
		return nil, err
	}
	docModel, errs := document.BuildV3Model()
	if len(errs) > 0 {
		return nil, fmt.Errorf("cannot convert document to OpenAPI V3 model: %w", errors.Join(errs...))
	}
	return docModel, nil
}

func makeAPI(serviceConfig *serviceconfig.Service, model *libopenapi.DocumentModel[v3.Document]) (*genclient.API, error) {
	api := &genclient.API{
		Name:        "",
		Title:       model.Model.Info.Title,
		Description: model.Model.Info.Description,
		Messages:    make([]*genclient.Message, 0),
		State: &genclient.APIState{
			ServiceByID: make(map[string]*genclient.Service),
			MessageByID: make(map[string]*genclient.Message),
			EnumByID:    make(map[string]*genclient.Enum),
		},
	}

	if serviceConfig != nil {
		api.Name = strings.TrimSuffix(serviceConfig.Name, ".googleapis.com")
		api.Title = serviceConfig.Title
		api.Description = serviceConfig.Documentation.Summary
	}

	// OpenAPI does not define a service name. The service config may provide
	// one. In tests, the service config is typically `nil`.
	serviceName := "Service"
	packageName := ""
	if serviceConfig != nil {
		for _, api := range serviceConfig.Apis {
			packageName, serviceName = splitApiName(api.Name)
			// Keep searching after well-known mixin services.
			if !wellKnownMixin(api.Name) {
				break
			}
		}
	}

	for name, msg := range model.Model.Components.Schemas.FromOldest() {
		id := fmt.Sprintf(".%s.%s", packageName, name)
		schema, err := msg.BuildSchema()
		if err != nil {
			return nil, err
		}
		fields, err := makeMessageFields(api.State, packageName, name, schema)
		if err != nil {
			return nil, err
		}
		message := &genclient.Message{
			Name:          name,
			ID:            id,
			Package:       packageName,
			Documentation: msg.Schema().Description,
			Fields:        fields,
		}

		api.Messages = append(api.Messages, message)
		api.State.MessageByID[id] = message
	}

	err := makeServices(api, model, packageName, serviceName)
	if err != nil {
		return nil, err
	}
	return api, nil
}

func wellKnownMixin(apiName string) bool {
	return strings.HasPrefix(apiName, "google.cloud.location.Location") ||
		strings.HasPrefix(apiName, "google.longrunning.Operations") ||
		strings.HasPrefix(apiName, "google.iam.v1.IAMPolicy")
}

func splitApiName(name string) (string, string) {
	li := strings.LastIndex(name, ".")
	if li == -1 {
		return "", name
	}
	return name[:li], name[li+1:]
}

func makeServices(api *genclient.API, model *libopenapi.DocumentModel[v3.Document], packageName, serviceName string) error {
	// It is hard to imagine an OpenAPI specification without at least some
	// RPCs, but we can simplify the tests if we support specifications without
	// paths or without any useful methods in the paths.
	if model.Model.Paths == nil {
		return nil
	}
	methods, err := makeMethods(api, model, packageName)
	if err != nil {
		return err
	}
	if len(methods) == 0 {
		return nil
	}
	service := &genclient.Service{
		Name:          serviceName,
		ID:            fmt.Sprintf(".%s.%s", packageName, serviceName),
		Package:       packageName,
		Documentation: api.Description,
		DefaultHost:   defaultHost(model),
		Methods:       methods,
	}
	api.Services = append(api.Services, service)
	api.State.ServiceByID[service.ID] = service
	return nil
}

func defaultHost(model *libopenapi.DocumentModel[v3.Document]) string {
	defaultHost := ""
	for _, server := range model.Model.Servers {
		if defaultHost == "" {
			defaultHost = server.URL
		} else if len(defaultHost) > len(server.URL) {
			defaultHost = server.URL
		}
	}
	// The mustache template adds https:// because Protobuf does not include
	// the scheme.
	return strings.TrimPrefix(defaultHost, "https://")
}

func makeMethods(api *genclient.API, model *libopenapi.DocumentModel[v3.Document], packageName string) ([]*genclient.Method, error) {
	methods := []*genclient.Method{}
	if model.Model.Paths == nil {
		return methods, nil
	}
	for pattern, item := range model.Model.Paths.PathItems.FromOldest() {
		pathTemplate := makePathTemplate(pattern)

		type NamedOperation struct {
			Verb      string
			Operation *v3.Operation
		}
		operations := []NamedOperation{
			{Verb: "GET", Operation: item.Get},
			{Verb: "PUT", Operation: item.Put},
			{Verb: "POST", Operation: item.Post},
			{Verb: "DELETE", Operation: item.Delete},
			{Verb: "OPTIONS", Operation: item.Options},
			{Verb: "HEAD", Operation: item.Head},
			{Verb: "PATCH", Operation: item.Patch},
			{Verb: "TRACE", Operation: item.Trace},
		}
		for _, op := range operations {
			if op.Operation == nil {
				continue
			}
			requestMessage, bodyFieldPath, err := makeRequestMessage(api, op.Operation, packageName, pattern)
			if err != nil {
				return nil, err
			}
			responseMessage, err := makeResponseMessage(api, op.Operation, packageName)
			if err != nil {
				return nil, err
			}
			pathInfo := &genclient.PathInfo{
				Verb:            op.Verb,
				PathTemplate:    pathTemplate,
				QueryParameters: makeQueryParameters(op.Operation),
				BodyFieldPath:   bodyFieldPath,
			}
			m := &genclient.Method{
				Name:          op.Operation.OperationId,
				ID:            op.Operation.OperationId,
				Documentation: op.Operation.Description,
				InputTypeID:   requestMessage.ID,
				OutputTypeID:  responseMessage.ID,
				PathInfo:      pathInfo,
			}
			methods = append(methods, m)
		}
	}
	return methods, nil
}

func makePathTemplate(template string) []genclient.PathSegment {
	segments := []genclient.PathSegment{}
	for idx, component := range strings.Split(template, ":") {
		if idx != 0 {
			segments = append(segments, genclient.PathSegment{Verb: &component})
			continue
		}
		for _, element := range strings.Split(component, "/") {
			if element == "" {
				continue
			}
			if strings.HasPrefix(element, "{") && strings.HasSuffix(element, "}") {
				element = element[1 : len(element)-1]
				segments = append(segments, genclient.PathSegment{FieldPath: &element})
				continue
			}
			segments = append(segments, genclient.PathSegment{Literal: &element})
		}
	}
	return segments
}

// Creates (if needed) the request message for `operation`. Returns the message
// and the body field path (if any) for the request.
func makeRequestMessage(api *genclient.API, operation *v3.Operation, packageName, template string) (*genclient.Message, string, error) {
	messageName := fmt.Sprintf("%sRequest", operation.OperationId)
	id := fmt.Sprintf(".%s.%s", packageName, messageName)
	message := &genclient.Message{
		Name:          messageName,
		ID:            id,
		Package:       packageName,
		Documentation: fmt.Sprintf("The request message for %s.", operation.OperationId),
	}

	bodyFieldPath := ""
	if operation.RequestBody != nil {
		reference, err := findReferenceInContentMap(operation.RequestBody.Content)
		if err != nil {
			return nil, "", err
		}
		bid := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(reference, "#/components/schemas/"))
		msg, ok := api.State.MessageByID[bid]
		if !ok {
			return nil, "", fmt.Errorf("cannot find referenced type (%s) in API messages", reference)
		}
		// Our OpenAPI specs do this weird thing: sometimes the `*Request`
		// message appears in the list of known messages. But sometimes only
		// the payload appears. I have not found any attribute to tell apart
		// between the two. Only the name suffix.
		if strings.HasSuffix(reference, "Request") {
			// If the message ends in `Request` then we can assume it is fine
			// adding more fields to it.
			message = msg
			bodyFieldPath = "*"
		} else {
			// The OpenAPI discovery docs do not preserve the original field
			// name for the request body. We need to create a synthetic name,
			// which may clash with other fields in the message. Let's try a
			// couple of different names.
			inserted := false
			for _, name := range []string{"requestBody", "openapiRequestBody"} {
				field := &genclient.Field{
					Name:          name,
					JSONName:      name,
					Documentation: "The request body.",
					Typez:         genclient.MESSAGE_TYPE,
					TypezID:       bid,
					Optional:      true,
				}
				inserted = addFieldIfNew(message, field)
				if inserted {
					// The the request body field path accordingly.
					bodyFieldPath = name
					break
				}
			}
			if !inserted {
				return nil, "", fmt.Errorf("cannot insert the request body to message %s", message.Name)
			}
			// We need to create the message.
			api.Messages = append(api.Messages, message)
			api.State.MessageByID[message.ID] = message
		}
	} else {
		// The message is new
		api.Messages = append(api.Messages, message)
		api.State.MessageByID[message.ID] = message
	}

	for _, p := range operation.Parameters {
		schema, err := p.Schema.BuildSchema()
		if err != nil {
			return nil, "", fmt.Errorf("error building schema for parameter %s: %w", p.Name, err)
		}
		typez, typezID, err := scalarType(messageName, p.Name, schema)
		if err != nil {
			return nil, "", err
		}
		documentation := p.Description
		if len(documentation) == 0 {
			// In Google's OpenAPI v3 specifications the parameters often lack
			// any documentation. Create a synthetic document in this case.
			documentation = fmt.Sprintf(
				"The `{%s}` component of the target path.\n"+
					"\n"+
					"The full target path will be in the form `%s`.", p.Name, template)
		}
		field := &genclient.Field{
			Name:          p.Name,
			JSONName:      p.Name, // OpenAPI fields are already camelCase
			Documentation: documentation,
			Optional:      p.Required == nil || !*p.Required,
			Typez:         typez,
			TypezID:       typezID,
			Synthetic:     true,
		}
		addFieldIfNew(message, field)
	}
	return message, bodyFieldPath, nil
}

func addFieldIfNew(message *genclient.Message, field *genclient.Field) bool {
	for _, f := range message.Fields {
		if f.Name == field.Name {
			// If the exact same field exists, treat that as a success.
			return *f == *field
		}
	}
	message.Fields = append(message.Fields, field)
	return true
}

func makeResponseMessage(api *genclient.API, operation *v3.Operation, packageName string) (*genclient.Message, error) {
	if operation.Responses == nil {
		return nil, fmt.Errorf("missing Responses in specification for operation %s", operation.OperationId)
	}
	if operation.Responses.Default == nil {
		// Google's OpenAPI v3 specifications only include the "default" response. In the future we may want to support
		return nil, fmt.Errorf("expected Default response for operation %s", operation.OperationId)
	}
	reference, err := findReferenceInContentMap(operation.Responses.Default.Content)
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(reference, "#/components/schemas/"))
	if message, ok := api.State.MessageByID[id]; ok {
		return message, nil
	}
	return nil, fmt.Errorf("cannot find response message ref=%s", reference)
}

func findReferenceInContentMap(content *orderedmap.Map[string, *v3.MediaType]) (string, error) {
	for pair := content.Oldest(); pair != nil; pair = pair.Next() {
		if pair.Key != "application/json" {
			continue
		}
		return pair.Value.Schema.GetReference(), nil
	}
	return "", fmt.Errorf("cannot find an application/json content type")
}

func makeQueryParameters(operation *v3.Operation) map[string]bool {
	queryParameters := map[string]bool{}
	for _, p := range operation.Parameters {
		if p.In != "query" {
			continue
		}
		queryParameters[p.Name] = true
	}
	return queryParameters
}

func makeMessageFields(state *genclient.APIState, packageName, messageName string, message *base.Schema) ([]*genclient.Field, error) {
	var fields []*genclient.Field
	for name, f := range message.Properties.FromOldest() {
		schema, err := f.BuildSchema()
		if err != nil {
			return nil, err
		}
		optional := true
		for _, r := range message.Required {
			if name == r {
				optional = false
				break
			}
		}
		field, err := makeField(state, packageName, messageName, name, optional, schema)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func makeField(state *genclient.APIState, packageName, messageName, name string, optional bool, field *base.Schema) (*genclient.Field, error) {
	if len(field.AllOf) != 0 {
		// Simple object fields name an AllOf attribute, but no `Type` attribute.
		return makeObjectField(state, packageName, messageName, name, field)
	}
	if len(field.Type) == 0 {
		return nil, fmt.Errorf("missing field type for field %s.%s", messageName, name)
	}
	switch field.Type[0] {
	case "boolean", "integer", "number", "string":
		return makeScalarField(messageName, name, field, optional, field)
	case "object":
		return makeObjectField(state, packageName, messageName, name, field)
	case "array":
		return makeArrayField(state, packageName, messageName, name, field)
	default:
		return nil, fmt.Errorf("unknown type for field %q", name)
	}
}

func makeScalarField(messageName, name string, schema *base.Schema, optional bool, field *base.Schema) (*genclient.Field, error) {
	typez, typezID, err := scalarType(messageName, name, schema)
	if err != nil {
		return nil, err
	}
	return &genclient.Field{
		Name:          name,
		JSONName:      name, // OpenAPI field names are always camelCase
		Documentation: field.Description,
		Typez:         typez,
		TypezID:       typezID,
		Optional:      optional || (typez == genclient.MESSAGE_TYPE),
	}, nil
}

func makeObjectField(state *genclient.APIState, packageName, messageName, name string, field *base.Schema) (*genclient.Field, error) {
	if len(field.AllOf) != 0 {
		return makeObjectFieldAllOf(packageName, messageName, name, field)
	}
	if field.AdditionalProperties != nil && field.AdditionalProperties.IsA() {
		// This indicates we have a map<K, T> field. In OpenAPI, these are
		// simply JSON objects, maybe with a restrictive value type.
		schema, err := field.AdditionalProperties.A.BuildSchema()
		if err != nil {
			return nil, fmt.Errorf("cannot build schema for field %s.%s: %w", messageName, name, err)
		}

		if len(schema.Type) == 0 {
			// Untyped message fields are .google.protobuf.Any
			return &genclient.Field{
				Name:          name,
				JSONName:      name, // OpenAPI field names are always camelCase
				Documentation: field.Description,
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       ".google.protobuf.Any",
				Optional:      true,
			}, nil
		}
		message, err := makeMapMessage(state, messageName, name, schema)
		if err != nil {
			return nil, err
		}
		return &genclient.Field{
			Name:          name,
			JSONName:      name, // OpenAPI field names are always camelCase
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       message.ID,
			Optional:      false,
		}, nil
	}
	if field.Items != nil && field.Items.IsA() {
		proxy := field.Items.A
		typezID := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(proxy.GetReference(), "#/components/schemas/"))
		return &genclient.Field{
			Name:          name,
			JSONName:      name, // OpenAPI field names are always camelCase
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       typezID,
			Optional:      true,
		}, nil
	}
	return nil, fmt.Errorf("unknown object field type for field %s.%s", messageName, name)
}

func makeArrayField(state *genclient.APIState, packageName, messageName, name string, field *base.Schema) (*genclient.Field, error) {
	if !field.Items.IsA() {
		return nil, fmt.Errorf("cannot handle arrays without an `Items` field for %s.%s", messageName, name)
	}
	reference := field.Items.A.GetReference()
	schema, err := field.Items.A.BuildSchema()
	if err != nil {
		return nil, fmt.Errorf("cannot build items schema for %s.%s error=%q", messageName, name, err)
	}
	if len(schema.Type) != 1 {
		return nil, fmt.Errorf("the items for field  %s.%s should have a single type", messageName, name)
	}
	var result *genclient.Field
	switch schema.Type[0] {
	case "boolean", "integer", "number", "string":
		result, err = makeScalarField(messageName, name, schema, false, field)
	case "object":
		typezID := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(reference, "#/components/schemas/"))
		if len(typezID) > 0 {
			new := &genclient.Field{
				Name:          name,
				JSONName:      name, // OpenAPI field names are always camelCase
				Documentation: field.Description,
				Typez:         genclient.MESSAGE_TYPE,
				TypezID:       typezID,
			}
			result = new
		} else {
			result, err = makeObjectField(state, packageName, messageName, name, schema)
		}
	default:
		return nil, fmt.Errorf("unknown array field type for %s.%s %q", messageName, name, schema.Type[0])
	}
	if err != nil {
		return nil, err
	}
	result.Repeated = true
	result.Optional = false
	return result, nil
}

func makeObjectFieldAllOf(packageName, messageName, name string, field *base.Schema) (*genclient.Field, error) {
	for _, proxy := range field.AllOf {
		typezID := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(proxy.GetReference(), "#/components/schemas/"))
		return &genclient.Field{
			Name:          name,
			JSONName:      name, // OpenAPI field names are always camelCase
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       typezID,
			Optional:      true,
		}, nil
	}
	return nil, fmt.Errorf("cannot build any AllOf schema for field %s.%s", messageName, name)
}

func makeMapMessage(state *genclient.APIState, messageName, name string, schema *base.Schema) (*genclient.Message, error) {
	value_typez, value_id, err := scalarType(messageName, name, schema)
	if err != nil {
		return nil, err
	}
	value := &genclient.Field{
		Name:    "value",
		ID:      value_id,
		Typez:   value_typez,
		TypezID: value_id,
	}

	id := fmt.Sprintf("$map<string, %s>", value.TypezID)
	message := state.MessageByID[id]
	if message == nil {
		// The map was not found, insert the type.
		key := &genclient.Field{
			Name:    "key",
			ID:      id + ".key",
			Typez:   genclient.STRING_TYPE,
			TypezID: "string",
		}
		placeholder := &genclient.Message{
			Name:             id,
			Documentation:    id,
			ID:               id,
			IsLocalToPackage: false,
			IsMap:            true,
			Fields:           []*genclient.Field{key, value},
			Parent:           nil,
			Package:          "$",
		}
		state.MessageByID[id] = placeholder
		message = placeholder
	}
	return message, nil
}

func scalarType(messageName, name string, schema *base.Schema) (genclient.Typez, string, error) {
	for _, type_name := range schema.Type {
		switch type_name {
		case "boolean":
			return genclient.BOOL_TYPE, "bool", nil
		case "integer":
			return scalarTypeForIntegerFormats(messageName, name, schema)
		case "number":
			return scalarTypeForNumberFormats(messageName, name, schema)
		case "string":
			return scalarTypeForStringFormats(messageName, name, schema)
		}
	}
	return 0, "", fmt.Errorf("expected a scalar type for field %s.%s", messageName, name)
}

func scalarTypeForIntegerFormats(messageName, name string, schema *base.Schema) (genclient.Typez, string, error) {
	switch schema.Format {
	case "int32":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return genclient.UINT32_TYPE, "uint32", nil
		}
		return genclient.INT32_TYPE, "int32", nil
	case "int64":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return genclient.UINT64_TYPE, "uint64", nil
		}
		return genclient.INT64_TYPE, "int64", nil
	}
	return 0, "", fmt.Errorf("unknown integer format (%s) for field %s.%s", schema.Format, messageName, name)
}

func scalarTypeForNumberFormats(messageName, name string, schema *base.Schema) (genclient.Typez, string, error) {
	switch schema.Format {
	case "float":
		return genclient.FLOAT_TYPE, "float", nil
	case "double":
		return genclient.DOUBLE_TYPE, "double", nil
	}
	return 0, "", fmt.Errorf("unknown number format (%s) for field %s.%s", schema.Format, messageName, name)
}

func scalarTypeForStringFormats(messageName, name string, schema *base.Schema) (genclient.Typez, string, error) {
	switch schema.Format {
	case "":
		return genclient.STRING_TYPE, "string", nil
	case "byte":
		return genclient.BYTES_TYPE, "bytes", nil
	case "int32":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return genclient.UINT32_TYPE, "uint32", nil
		}
		return genclient.INT32_TYPE, "int32", nil
	case "int64":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return genclient.UINT64_TYPE, "uint64", nil
		}
		return genclient.INT64_TYPE, "int64", nil
	case "google-duration":
		return genclient.MESSAGE_TYPE, ".google.protobuf.Duration", nil
	case "date-time":
		return genclient.MESSAGE_TYPE, ".google.protobuf.Timestamp", nil
	case "google-fieldmask":
		return genclient.MESSAGE_TYPE, ".google.protobuf.FieldMask", nil
	}
	return 0, "", fmt.Errorf("unknown string format (%s) for field %s.%s", schema.Format, messageName, name)
}
