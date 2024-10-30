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

package protobuf

import (
	"log/slog"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/src/genclient"
	"github.com/googleapis/google-cloud-rust/generator/src/genclient/language"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

type Translator struct {
	request  *pluginpb.CodeGeneratorRequest
	language string

	// Only used for local testing
	outDir      string
	templateDir string

	// State by FQN
	state *genclient.APIState
}

type Options struct {
	Request  *pluginpb.CodeGeneratorRequest
	Language string
	// Only used for local testing
	OutDir      string
	TemplateDir string
}

func NewTranslator(opts *Options) *Translator {
	return &Translator{
		outDir:      opts.OutDir,
		request:     opts.Request,
		language:    opts.Language,
		templateDir: opts.TemplateDir,
		state: &genclient.APIState{
			ServiceByID: make(map[string]*genclient.Service),
			MessageByID: make(map[string]*genclient.Message),
			EnumByID:    make(map[string]*genclient.Enum),
		},
	}
}

// Translates proto representation into a [genclient.GenerateRequest].
func (t *Translator) Translate() (*genclient.GenerateRequest, error) {
	api := &genclient.API{
		//TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/38
		Name: "secretmanager",
	}
	files := t.request.GetSourceFileDescriptors()
	for _, f := range files {
		var fileServices []*genclient.Service
		fFQN := "." + f.GetPackage()

		// Messages
		for _, m := range f.MessageType {
			mFQN := fFQN + "." + m.GetName()
			msg := t.processMessage(m, mFQN, nil)
			api.Messages = append(api.Messages, msg)
		}

		// Services
		for _, s := range f.Service {
			service := &genclient.Service{}
			sFQN := fFQN + "." + s.GetName()
			t.state.ServiceByID[sFQN] = service
			service.Name = s.GetName()
			service.ID = sFQN
			service.DefaultHost = parseDefaultHost(s.GetOptions())
			for _, m := range s.Method {
				method := &genclient.Method{}
				method.HTTPInfo = parseHTTPInfo(m.GetOptions())
				method.Name = m.GetName()
				method.InputTypeID = m.GetInputType()
				method.OutputTypeID = m.GetOutputType()
				service.Methods = append(service.Methods, method)
			}
			fileServices = append(fileServices, service)
		}

		// Add docs
		for _, loc := range f.GetSourceCodeInfo().GetLocation() {
			p := loc.GetPath()
			if loc.GetLeadingComments() == "" || len(p) == 0 {
				continue
			}

			// These magic numbers come from reading the proto docs. They come
			// from field numbers of the different descriptor types. See struct
			// tags on https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#FileDescriptorProto.
			switch p[0] {
			case 4:
				// Because of message nesting we need to call recursively and
				// strip out parts of the path.
				m := f.MessageType[p[1]]
				t.addMessageDocumentation(m, p[2:], strings.TrimSpace(loc.GetLeadingComments()), fFQN+"."+m.GetName())
			case 6:
				sFQN := fFQN + "." + f.GetService()[p[1]].GetName()
				t.addServiceDocumentation(p[2:], strings.TrimSpace(loc.GetLeadingComments()), sFQN)
			default:
				slog.Warn("file dropped documentation", "loc", p, "docs", loc.GetLeadingComments())
			}
		}
		api.Services = append(api.Services, fileServices...)
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

func NewCodeGeneratorResponse(_ *genclient.Output, err error) *pluginpb.CodeGeneratorResponse {
	resp := &pluginpb.CodeGeneratorResponse{}
	if err != nil {
		resp.Error = proto.String(err.Error())
	}
	return resp
}

func normalizeTypes(in *descriptorpb.FieldDescriptorProto, field *genclient.Field) {
	switch in.GetType() {
	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
		field.Typez = genclient.DOUBLE_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_FLOAT:
		field.Typez = genclient.FLOAT_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_INT64:
		field.Typez = genclient.INT64_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
		field.Typez = genclient.UINT64_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_INT32:
		field.Typez = genclient.INT32_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED64:
		field.Typez = genclient.FIXED64_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED32:
		field.Typez = genclient.FIXED32_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
		field.Typez = genclient.BOOL_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
		field.Typez = genclient.STRING_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_BYTES:
		field.Typez = genclient.BYTES_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
		field.Typez = genclient.UINT32_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED32:
		field.Typez = genclient.SFIXED32_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED64:
		field.Typez = genclient.SFIXED64_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_SINT32:
		field.Typez = genclient.SINT32_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_SINT64:
		field.Typez = genclient.SINT64_TYPE
	case descriptorpb.FieldDescriptorProto_TYPE_GROUP:
		field.Typez = genclient.GROUP_TYPE
		field.TypezID = in.GetTypeName()
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
		field.Typez = genclient.MESSAGE_TYPE
		field.TypezID = in.GetTypeName()
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
		field.Typez = genclient.ENUM_TYPE
		field.TypezID = in.GetTypeName()
	default:
		slog.Warn("found undefined field", "field", in.GetName())
		field.Typez = genclient.UNDEFINED_TYPE
	}
}

func (t *Translator) processMessage(m *descriptorpb.DescriptorProto, mFQN string, parent *genclient.Message) *genclient.Message {
	message := &genclient.Message{}
	t.state.MessageByID[mFQN] = message
	message.Name = m.GetName()
	message.ID = mFQN
	message.Parent = parent
	if opts := m.GetOptions(); opts != nil && opts.GetMapEntry() {
		message.IsMap = true
	}
	if len(m.GetNestedType()) > 0 {
		for _, nm := range m.GetNestedType() {
			nmFQN := mFQN + "." + nm.GetName()
			nmsg := t.processMessage(nm, nmFQN, message)
			message.Messages = append(message.Messages, nmsg)
		}
	}
	for _, e := range m.GetEnumType() {
		e := t.processEnum(e, mFQN, message)
		message.Enums = append(message.Enums, e)
	}
	// TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/39
	for _, mf := range m.Field {
		field := &genclient.Field{}
		field.Name = mf.GetName()
		field.ID = mFQN + "." + mf.GetName()
		field.JSONName = mf.GetJsonName()
		normalizeTypes(mf, field)
		message.Fields = append(message.Fields, field)
	}
	return message
}

func (t *Translator) processEnum(e *descriptorpb.EnumDescriptorProto, baseFQN string, parent *genclient.Message) *genclient.Enum {
	enum := &genclient.Enum{}
	t.state.EnumByID[baseFQN+"."+e.GetName()] = enum
	enum.Name = e.GetName()
	enum.Parent = parent
	for _, ev := range e.Value {
		enumValue := &genclient.EnumValue{}
		enumValue.Name = ev.GetName()
		enumValue.Number = ev.GetNumber()
		enumValue.Parent = enum
		enum.Values = append(enum.Values, enumValue)
	}
	return enum
}

func (t *Translator) addServiceDocumentation(p []int32, doc string, sFQN string) {
	// These magic numbers come from reading the proto docs. They come
	// from field numbers of the different descriptor types. See struct
	// tags on https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#ServiceDescriptorProto.
	if len(p) == 0 {
		// This is a comment for a service
		t.state.ServiceByID[sFQN].Documentation = doc
	} else if len(p) == 2 && p[0] == 2 {
		// This is a comment for a method
		t.state.ServiceByID[sFQN].Methods[p[1]].Documentation = doc
	} else {
		slog.Warn("service dropped documentation", "loc", p, "docs", doc)
	}
}

func (t *Translator) addMessageDocumentation(m *descriptorpb.DescriptorProto, p []int32, doc string, mFQN string) {
	// These magic numbers come from reading the proto docs. They come
	// from field numbers of the different descriptor types. See struct
	// tags on https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#DescriptorProto.
	if len(p) == 0 {
		// This is a comment for a top level message
		t.state.MessageByID[mFQN].Documentation = doc
	} else if p[0] == 3 {
		// This indicates a nested message, recurse.
		nmsg := m.GetNestedType()[p[1]]
		nmFQN := mFQN + "." + nmsg.GetName()
		t.addMessageDocumentation(nmsg, p[2:], doc, nmFQN)
	} else if len(p) == 2 && p[0] == 2 {
		// This is a comment for a field of a message
		t.state.MessageByID[mFQN].Fields[p[1]].Documentation = doc
	} else if p[0] == 4 {
		// This is a comment for a enum of a message
		eFQN := mFQN + "." + m.GetEnumType()[p[1]].GetName()
		t.addEnumDocumentation(p[2:], doc, eFQN)
	} else if len(p) == 2 && p[0] == 8 {
		// This is a comment for a field of a message one-of, skipping
	} else {
		slog.Warn("message dropped documentation", "loc", p, "docs", doc)
	}
}

// addEnumDocumentation adds documentation to an enum.
func (t *Translator) addEnumDocumentation(p []int32, doc string, eFQN string) {
	if len(p) == 0 {
		// This is a comment for an enum
		t.state.EnumByID[eFQN].Documentation = doc
	} else if len(p) == 2 {
		// This is a comment for an enum value
		t.state.EnumByID[eFQN].Values[p[1]].Documentation = doc
	} else {
		slog.Warn("enum dropped documentation", "loc", p, "docs", doc)
	}
}
