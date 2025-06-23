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

// Package parser reads specifications and converts them into
// the `genclient.API` model.
package parser

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/parser/httprule"
	"github.com/pb33f/libopenapi"
	"github.com/pb33f/libopenapi/datamodel/high/base"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
	"github.com/pb33f/libopenapi/orderedmap"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
)

func ParseOpenAPI(source, serviceConfigFile string, options map[string]string) (*api.API, error) {
	contents, err := os.ReadFile(source)
	if err != nil {
		return nil, err
	}
	model, err := createDocModel(contents)
	if err != nil {
		return nil, err
	}
	var serviceConfig *serviceconfig.Service
	if serviceConfigFile != "" {
		cfg, err := readServiceConfig(findServiceConfigPath(serviceConfigFile, options))
		if err != nil {
			return nil, err
		}
		serviceConfig = cfg
	}
	return makeAPIForOpenAPI(serviceConfig, model)
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

func makeAPIForOpenAPI(serviceConfig *serviceconfig.Service, model *libopenapi.DocumentModel[v3.Document]) (*api.API, error) {
	result := &api.API{
		Name:        "",
		Title:       model.Model.Info.Title,
		Description: model.Model.Info.Description,
		Messages:    make([]*api.Message, 0),
		State: &api.APIState{
			ServiceByID: make(map[string]*api.Service),
			MethodByID:  make(map[string]*api.Method),
			MessageByID: make(map[string]*api.Message),
			EnumByID:    make(map[string]*api.Enum),
		},
	}

	if serviceConfig != nil {
		result.Name = strings.TrimSuffix(serviceConfig.Name, ".googleapis.com")
		result.Title = serviceConfig.Title
		if serviceConfig.Documentation != nil {
			result.Description = serviceConfig.Documentation.Summary
		}
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
		result.PackageName = packageName
	}

	for name, msg := range model.Model.Components.Schemas.FromOldest() {
		id := fmt.Sprintf(".%s.%s", packageName, name)
		schema, err := msg.BuildSchema()
		if err != nil {
			return nil, err
		}
		fields, err := makeMessageFields(result.State, packageName, name, schema)
		if err != nil {
			return nil, err
		}
		message := &api.Message{
			Name:          name,
			ID:            id,
			Package:       packageName,
			Deprecated:    msg.Schema().Deprecated != nil && *msg.Schema().Deprecated,
			Documentation: msg.Schema().Description,
			Fields:        fields,
		}

		result.Messages = append(result.Messages, message)
		result.State.MessageByID[id] = message
	}

	err := makeServices(result, model, packageName, serviceName)
	if err != nil {
		return nil, err
	}
	updateMethodPagination(result)
	updateAutoPopulatedFields(serviceConfig, result)
	return result, nil
}

func makeServices(a *api.API, model *libopenapi.DocumentModel[v3.Document], packageName, serviceName string) error {
	// It is hard to imagine an OpenAPI specification without at least some
	// RPCs, but we can simplify the tests if we support specifications without
	// paths or without any useful methods in the paths.
	if model.Model.Paths == nil {
		return nil
	}
	sID := fmt.Sprintf(".%s.%s", packageName, serviceName)
	methods, err := makeMethods(a, model, packageName, sID)
	if err != nil {
		return err
	}
	if len(methods) == 0 {
		return nil
	}
	service := &api.Service{
		Name:          serviceName,
		ID:            fmt.Sprintf(".%s.%s", packageName, serviceName),
		Package:       packageName,
		Documentation: a.Description,
		DefaultHost:   defaultHost(model),
		Methods:       methods,
	}
	a.Services = append(a.Services, service)
	a.State.ServiceByID[service.ID] = service
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

func makeMethods(a *api.API, model *libopenapi.DocumentModel[v3.Document], packageName, serviceID string) ([]*api.Method, error) {
	var methods []*api.Method
	if model.Model.Paths == nil {
		return methods, nil
	}
	for pattern, item := range model.Model.Paths.PathItems.FromOldest() {
		pathTemplate, err := httprule.LegacyParseSegments(pattern)
		if err != nil {
			return nil, err
		}

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
			requestMessage, bodyFieldPath, err := makeRequestMessage(a, op.Operation, packageName, pattern)
			if err != nil {
				return nil, err
			}
			responseMessage, err := makeResponseMessage(a, op.Operation, packageName)
			if err != nil {
				return nil, err
			}
			queryParameters := makeQueryParameters(op.Operation)
			pathInfo := &api.PathInfo{
				Bindings: []*api.PathBinding{
					{
						Verb:               op.Verb,
						LegacyPathTemplate: pathTemplate,
						QueryParameters:    queryParameters,
					},
				},
				BodyFieldPath: bodyFieldPath,
			}
			mID := fmt.Sprintf("%s.%s", serviceID, op.Operation.OperationId)
			m := &api.Method{
				Name:          op.Operation.OperationId,
				ID:            mID,
				Deprecated:    op.Operation.Deprecated != nil && *op.Operation.Deprecated,
				Documentation: op.Operation.Description,
				InputTypeID:   requestMessage.ID,
				OutputTypeID:  responseMessage.ID,
				PathInfo:      pathInfo,
			}
			a.State.MethodByID[m.ID] = m
			methods = append(methods, m)
		}
	}
	return methods, nil
}

// Creates (if needed) the request message for `operation`. Returns the message
// and the body field path (if any) for the request.
func makeRequestMessage(a *api.API, operation *v3.Operation, packageName, template string) (*api.Message, string, error) {
	messageName := fmt.Sprintf("%sRequest", operation.OperationId)
	id := fmt.Sprintf(".%s.%s", packageName, messageName)
	message := &api.Message{
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
		msg, ok := a.State.MessageByID[bid]
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
				field := &api.Field{
					Name:          name,
					JSONName:      name,
					Documentation: "The request body.",
					Typez:         api.MESSAGE_TYPE,
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
			a.Messages = append(a.Messages, message)
			a.State.MessageByID[message.ID] = message
		}
	} else {
		// The message is new
		a.Messages = append(a.Messages, message)
		a.State.MessageByID[message.ID] = message
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
		field := &api.Field{
			Name:          p.Name,
			JSONName:      p.Name, // OpenAPI fields are already camelCase
			Documentation: documentation,
			Deprecated:    p.Deprecated,
			Optional:      openapiFieldIsOptional(p),
			Typez:         typez,
			TypezID:       typezID,
			Synthetic:     true,
			AutoPopulated: openapiIsAutoPopulated(typez, schema, p),
			Behavior:      openapiParameterBehavior(p),
		}
		addFieldIfNew(message, field)
	}
	return message, bodyFieldPath, nil
}

func openapiFieldIsOptional(p *v3.Parameter) bool {
	return p.Required == nil || !*p.Required
}

func openapiIsAutoPopulated(typez api.Typez, schema *base.Schema, p *v3.Parameter) bool {
	return typez == api.STRING_TYPE && schema.Format == "uuid" && openapiFieldIsOptional(p)
}

func addFieldIfNew(message *api.Message, field *api.Field) bool {
	for _, f := range message.Fields {
		if f.Name == field.Name {
			// If the exact same field exists, treat that as a success.
			return cmp.Equal(f, field)
		}
	}
	message.Fields = append(message.Fields, field)
	return true
}

func makeResponseMessage(api *api.API, operation *v3.Operation, packageName string) (*api.Message, error) {
	if operation.Responses == nil {
		return nil, fmt.Errorf("missing Responses in specification for operation %s", operation.OperationId)
	}
	if operation.Responses.Default == nil {
		// Google's OpenAPI v3 specifications only include the "default"
		// response. In the future we may want to support more than this.
		return nil, fmt.Errorf("expected Default response for operation %s", operation.OperationId)
	}
	// TODO(#1590) - support a missing `Content` as an indication of `void`.
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

func makeMessageFields(state *api.APIState, packageName, messageName string, message *base.Schema) ([]*api.Field, error) {
	var fields []*api.Field
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

func makeField(state *api.APIState, packageName, messageName, name string, optional bool, field *base.Schema) (*api.Field, error) {
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

func makeScalarField(messageName, name string, schema *base.Schema, optional bool, field *base.Schema) (*api.Field, error) {
	typez, typezID, err := scalarType(messageName, name, schema)
	if err != nil {
		return nil, err
	}
	return &api.Field{
		Name:          name,
		JSONName:      name, // OpenAPI field names are always camelCase
		Documentation: field.Description,
		Typez:         typez,
		TypezID:       typezID,
		Deprecated:    field.Deprecated != nil && *field.Deprecated,
		Optional:      optional || (typez == api.MESSAGE_TYPE),
	}, nil
}

func makeObjectField(state *api.APIState, packageName, messageName, name string, field *base.Schema) (*api.Field, error) {
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
			return &api.Field{
				Name:          name,
				JSONName:      name, // OpenAPI field names are always camelCase
				Documentation: field.Description,
				Deprecated:    field.Deprecated != nil && *field.Deprecated,
				Typez:         api.MESSAGE_TYPE,
				TypezID:       ".google.protobuf.Any",
				Optional:      true,
			}, nil
		}
		message, err := makeMapMessage(state, messageName, name, schema)
		if err != nil {
			return nil, err
		}
		return &api.Field{
			Name:          name,
			JSONName:      name, // OpenAPI field names are always camelCase
			Documentation: field.Description,
			Deprecated:    field.Deprecated != nil && *field.Deprecated,
			Typez:         api.MESSAGE_TYPE,
			TypezID:       message.ID,
			Optional:      false,
			Repeated:      false,
			Map:           true,
		}, nil
	}
	if field.Items != nil && field.Items.IsA() {
		proxy := field.Items.A
		typezID := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(proxy.GetReference(), "#/components/schemas/"))
		return &api.Field{
			Name:          name,
			JSONName:      name, // OpenAPI field names are always camelCase
			Documentation: field.Description,
			Deprecated:    field.Deprecated != nil && *field.Deprecated,
			Typez:         api.MESSAGE_TYPE,
			TypezID:       typezID,
			Optional:      true,
		}, nil
	}
	return nil, fmt.Errorf("unknown object field type for field %s.%s", messageName, name)
}

func makeArrayField(state *api.APIState, packageName, messageName, name string, field *base.Schema) (*api.Field, error) {
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
	var result *api.Field
	switch schema.Type[0] {
	case "boolean", "integer", "number", "string":
		result, err = makeScalarField(messageName, name, schema, false, field)
	case "object":
		typezID := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(reference, "#/components/schemas/"))
		if len(typezID) > 0 {
			new := &api.Field{
				Name:          name,
				JSONName:      name, // OpenAPI field names are always camelCase
				Documentation: field.Description,
				Deprecated:    field.Deprecated != nil && *field.Deprecated,
				Typez:         api.MESSAGE_TYPE,
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
	result.Map = false
	result.Optional = false
	return result, nil
}

func makeObjectFieldAllOf(packageName, messageName, name string, field *base.Schema) (*api.Field, error) {
	for _, proxy := range field.AllOf {
		typezID := fmt.Sprintf(".%s.%s", packageName, strings.TrimPrefix(proxy.GetReference(), "#/components/schemas/"))
		return &api.Field{
			Name:          name,
			JSONName:      name, // OpenAPI field names are always camelCase
			Documentation: field.Description,
			Deprecated:    field.Deprecated != nil && *field.Deprecated,
			Typez:         api.MESSAGE_TYPE,
			TypezID:       typezID,
			Optional:      true,
		}, nil
	}
	return nil, fmt.Errorf("cannot build any AllOf schema for field %s.%s", messageName, name)
}

func makeMapMessage(state *api.APIState, messageName, name string, schema *base.Schema) (*api.Message, error) {
	value_typez, value_id, err := scalarType(messageName, name, schema)
	if err != nil {
		return nil, err
	}
	value := &api.Field{
		Name:    "value",
		ID:      value_id,
		Typez:   value_typez,
		TypezID: value_id,
	}

	id := fmt.Sprintf("$map<string, %s>", value.TypezID)
	message := state.MessageByID[id]
	if message == nil {
		// The map was not found, insert the type.
		key := &api.Field{
			Name:    "key",
			ID:      id + ".key",
			Typez:   api.STRING_TYPE,
			TypezID: "string",
		}
		placeholder := &api.Message{
			Name:             id,
			Documentation:    id,
			ID:               id,
			IsLocalToPackage: false,
			IsMap:            true,
			Fields:           []*api.Field{key, value},
			Parent:           nil,
			Package:          "$",
		}
		state.MessageByID[id] = placeholder
		message = placeholder
	}
	return message, nil
}

func scalarType(messageName, name string, schema *base.Schema) (api.Typez, string, error) {
	for _, type_name := range schema.Type {
		switch type_name {
		case "boolean":
			return api.BOOL_TYPE, "bool", nil
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

func scalarTypeForIntegerFormats(messageName, name string, schema *base.Schema) (api.Typez, string, error) {
	switch schema.Format {
	case "int32":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return api.UINT32_TYPE, "uint32", nil
		}
		return api.INT32_TYPE, "int32", nil
	case "int64":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return api.UINT64_TYPE, "uint64", nil
		}
		return api.INT64_TYPE, "int64", nil
	}
	return 0, "", fmt.Errorf("unknown integer format (%s) for field %s.%s", schema.Format, messageName, name)
}

func scalarTypeForNumberFormats(messageName, name string, schema *base.Schema) (api.Typez, string, error) {
	switch schema.Format {
	case "float":
		return api.FLOAT_TYPE, "float", nil
	case "double":
		return api.DOUBLE_TYPE, "double", nil
	}
	return 0, "", fmt.Errorf("unknown number format (%s) for field %s.%s", schema.Format, messageName, name)
}

func scalarTypeForStringFormats(messageName, name string, schema *base.Schema) (api.Typez, string, error) {
	switch schema.Format {
	case "":
		return api.STRING_TYPE, "string", nil
	case "uuid":
		return api.STRING_TYPE, "string", nil
	case "byte":
		return api.BYTES_TYPE, "bytes", nil
	case "int32":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return api.UINT32_TYPE, "uint32", nil
		}
		return api.INT32_TYPE, "int32", nil
	case "int64":
		if schema.Minimum != nil && *schema.Minimum == 0 {
			return api.UINT64_TYPE, "uint64", nil
		}
		return api.INT64_TYPE, "int64", nil
	case "google-duration":
		return api.MESSAGE_TYPE, ".google.protobuf.Duration", nil
	case "date-time":
		return api.MESSAGE_TYPE, ".google.protobuf.Timestamp", nil
	case "google-fieldmask":
		return api.MESSAGE_TYPE, ".google.protobuf.FieldMask", nil
	}
	return 0, "", fmt.Errorf("unknown string format (%s) for field %s.%s", schema.Format, messageName, name)
}
