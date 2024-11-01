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
// the `genclient` model.
package openapi

import (
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	"github.com/pb33f/libopenapi"
	base "github.com/pb33f/libopenapi/datamodel/high/base"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

type Translator struct {
	model    *libopenapi.DocumentModel[v3.Document]
	language string

	// State by FQN
	state *genclient.APIState

	// Only used for local testing
	outDir      string
	templateDir string
}

type Options struct {
	Language string
	// Only used for local testing
	OutDir      string
	TemplateDir string
}

func NewTranslator(contents []byte, opts *Options) (*Translator, error) {
	document, err := libopenapi.NewDocument(contents)
	if err != nil {
		return nil, err
	}
	docModel, errors := document.BuildV3Model()
	if len(errors) > 0 {
		for i := range errors {
			fmt.Printf("error: %e\n", errors[i])
		}
		return nil, fmt.Errorf("cannot convert document to OpenAPI V3 model: %e", errors[0])
	}

	return &Translator{
		model:       docModel,
		outDir:      opts.OutDir,
		language:    opts.Language,
		templateDir: opts.TemplateDir,
		state: &genclient.APIState{
			ServiceByID: make(map[string]*genclient.Service),
			MessageByID: make(map[string]*genclient.Message),
			EnumByID:    make(map[string]*genclient.Enum),
		},
	}, nil
}

func (t *Translator) makeAPI() (*genclient.API, error) {
	api := &genclient.API{
		Name:     t.model.Model.Info.Title,
		Messages: make([]*genclient.Message, 0),
	}
	for name, msg := range t.model.Model.Components.Schemas.FromOldest() {
		schema, err := msg.BuildSchema()
		if err != nil {
			return nil, err
		}
		fields, err := makeMessageFields(name, schema)
		if err != nil {
			return nil, err
		}
		message := genclient.Message{
			Name:          name,
			Documentation: msg.Schema().Description,
			Fields:        fields,
		}

		api.Messages = append(api.Messages, &message)
	}
	return api, nil
}

// Translates OpenAPI specification into a [genclient.GenerateRequest].
func (t *Translator) Translate() (*genclient.GenerateRequest, error) {
	api, err := t.makeAPI()
	if err != nil {
		return nil, err
	}

	codec, err := language.NewCodec(t.language)
	if err != nil {
		return nil, err
	}
	api.State = t.state
	return &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      t.outDir,
		TemplateDir: t.templateDir,
	}, nil
}

func makeMessageFields(messageName string, message *base.Schema) ([]*genclient.Field, error) {
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
		field, err := makeField(messageName, name, optional, schema)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func makeField(messageName, name string, optional bool, field *base.Schema) (*genclient.Field, error) {
	if len(field.AllOf) != 0 {
		return makeAllOfField(messageName, name, field)
	}
	if len(field.Type) == 0 {
		return nil, fmt.Errorf("missing field type for field %s.%s", messageName, name)
	}
	type_name := field.Type[0]
	switch type_name {
	case "boolean":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.BOOL_TYPE,
			Optional:      optional,
		}, nil
	case "string":
		return makeStringType(messageName, name, field.Format, optional, field)
	case "integer":
		return makeIntegerField(messageName, name, field.Format, optional, field)
	case "object":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			Optional:      true,
		}, nil
	case "array":
		return makeArrayField(messageName, name, field)
	default:
		return nil, fmt.Errorf("unknown type for field %q", name)
	}
}

func makeAllOfField(messageName, name string, field *base.Schema) (*genclient.Field, error) {
	for _, proxy := range field.AllOf {
		typezID := strings.TrimPrefix(proxy.GetReference(), "#/components/schemas/")
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       typezID,
			Optional:      true,
		}, nil
	}
	return nil, fmt.Errorf("cannot build any AllOf schema for field %s.%s", messageName, name)
}

func makeStringType(messageName, name, format string, optional bool, field *base.Schema) (*genclient.Field, error) {
	switch format {
	case "":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.STRING_TYPE,
			Optional:      optional,
		}, nil
	case "int64":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.INT64_TYPE,
			Optional:      optional,
		}, nil
	case "uint64":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.UINT64_TYPE,
			Optional:      optional,
		}, nil
	case "byte":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.BYTES_TYPE,
			Optional:      optional,
		}, nil
	case "google-duration":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       ".google.protobuf.Duration",
			Optional:      true,
		}, nil
	case "date-time":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       ".google.protobuf.Timestamp",
			Optional:      true,
		}, nil
	case "google-fieldmask":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			TypezID:       ".google.protobuf.FieldMask",
			Optional:      true,
		}, nil
	default:
		return nil, fmt.Errorf("unknown string format (%q) for field %s.%s", field.Format, messageName, name)
	}
}

func makeIntegerField(messageName, name, format string, optional bool, field *base.Schema) (*genclient.Field, error) {
	switch format {
	case "int32":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.INT32_TYPE,
			Optional:      optional,
		}, nil
	case "int64":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.INT64_TYPE,
			Optional:      optional,
		}, nil
	case "uint32":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.UINT32_TYPE,
			Optional:      optional,
		}, nil
	case "uint64":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.UINT64_TYPE,
			Optional:      optional,
		}, nil
	default:
		return nil, fmt.Errorf("unknown integer format (%q) for field %s.%s", format, messageName, name)
	}
}

func makeArrayField(messageName, name string, field *base.Schema) (*genclient.Field, error) {
	if !field.Items.IsA() {
		return nil, fmt.Errorf("cannot handle arrays without an `Items` field for %s.%s", messageName, name)
	}
	schema, err := field.Items.A.BuildSchema()
	if err != nil {
		return nil, fmt.Errorf("cannot build schema for %s.%s error=%q", messageName, name, err)
	}
	switch schema.Type[0] {
	case "string":
		field, err := makeStringType(messageName, name, schema.Format, false, field)
		if err != nil {
			return nil, err
		}
		field.Repeated = true
		field.Optional = false
		return field, nil
	case "integer":
		field, err := makeIntegerField(messageName, name, schema.Format, false, field)
		if err != nil {
			return nil, err
		}
		field.Repeated = true
		field.Optional = false
		return field, nil
	case "boolean":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.BOOL_TYPE,
			Optional:      false,
			Repeated:      true,
		}, nil
	case "object":
		return &genclient.Field{
			Name:          name,
			Documentation: field.Description,
			Typez:         genclient.MESSAGE_TYPE,
			Optional:      false,
			Repeated:      true,
		}, nil
	default:
		return nil, fmt.Errorf("unknown array field type for %s.%s %q", messageName, name, schema.Type[0])
	}
}
