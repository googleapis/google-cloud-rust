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
	"fmt"
	"log/slog"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

const (
	// From https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#FileDescriptorProto
	fileDescriptorMessageType = 4
	fileDescriptorService     = 6

	// From https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#ServiceDescriptorProto
	serviceDescriptorProtoMethod = 2

	// From https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#DescriptorProto
	messageDescriptorField      = 2
	messageDescriptorNestedType = 3
	messageDescriptorEnum       = 4
	messageDescriptorOneOf      = 8

	// From https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#EnumDescriptorProto
	enumDescriptorValue = 2
)

type Options struct {
	Language string
	// Only used for local testing
	OutDir        string
	TemplateDir   string
	ServiceConfig *serviceconfig.Service
}

// Translate translates proto representation into a [genclienGenerateRequest].
func Translate(req *pluginpb.CodeGeneratorRequest, opts *Options) (*genclient.GenerateRequest, error) {
	api := makeAPI(req)

	codec, err := language.NewCodec(opts.Language)
	if err != nil {
		return nil, err
	}
	return &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      opts.OutDir,
		TemplateDir: opts.TemplateDir,
	}, nil
}

func makeAPI(req *pluginpb.CodeGeneratorRequest) *genclient.API {
	state := &genclient.APIState{
		ServiceByID: make(map[string]*genclient.Service),
		MessageByID: make(map[string]*genclient.Message),
		EnumByID:    make(map[string]*genclient.Enum),
	}
	api := &genclient.API{
		//TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/38
		Name:  "secretmanager",
		State: state,
	}

	files := req.GetSourceFileDescriptors()
	for _, f := range files {
		var fileServices []*genclient.Service
		fFQN := "." + f.GetPackage()

		// Messages
		for _, m := range f.MessageType {
			mFQN := fFQN + "." + m.GetName()
			msg := processMessage(state, m, mFQN, nil)
			api.Messages = append(api.Messages, msg)
		}

		// Services
		for _, s := range f.Service {
			service := &genclient.Service{
				Name:        s.GetName(),
				ID:          fmt.Sprintf("%s.%s", fFQN, s.GetName()),
				DefaultHost: parseDefaultHost(s.GetOptions()),
			}
			state.ServiceByID[service.ID] = service
			for _, m := range s.Method {
				method := &genclient.Method{
					HTTPInfo:     parseHTTPInfo(m.GetOptions()),
					Name:         m.GetName(),
					InputTypeID:  m.GetInputType(),
					OutputTypeID: m.GetOutputType(),
				}
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

			switch p[0] {
			case fileDescriptorMessageType:
				// Because of message nesting we need to call recursively and
				// strip out parts of the path.
				m := f.MessageType[p[1]]
				addMessageDocumentation(state, m, p[2:], loc.GetLeadingComments(), fFQN+"."+m.GetName())
			case fileDescriptorService:
				sFQN := fFQN + "." + f.GetService()[p[1]].GetName()
				addServiceDocumentation(state, p[2:], loc.GetLeadingComments(), sFQN)
			default:
				slog.Warn("file dropped documentation", "loc", p, "docs", loc.GetLeadingComments())
			}
		}
		api.Services = append(api.Services, fileServices...)
	}
	return api
}

func NewCodeGeneratorResponse(_ *genclient.Output, err error) *pluginpb.CodeGeneratorResponse {
	resp := &pluginpb.CodeGeneratorResponse{}
	if err != nil {
		resp.Error = proto.String(err.Error())
	}
	return resp
}

var descriptorpbToTypez = map[descriptorpb.FieldDescriptorProto_Type]genclient.Typez{
	descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:   genclient.DOUBLE_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_FLOAT:    genclient.FLOAT_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_INT64:    genclient.INT64_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_UINT64:   genclient.UINT64_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_INT32:    genclient.INT32_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_FIXED64:  genclient.FIXED64_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_FIXED32:  genclient.FIXED32_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_BOOL:     genclient.BOOL_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_STRING:   genclient.STRING_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_BYTES:    genclient.BYTES_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_UINT32:   genclient.UINT32_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_SFIXED32: genclient.SFIXED32_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_SFIXED64: genclient.SFIXED64_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_SINT32:   genclient.SINT32_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_SINT64:   genclient.SINT64_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_GROUP:    genclient.GROUP_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:  genclient.MESSAGE_TYPE,
	descriptorpb.FieldDescriptorProto_TYPE_ENUM:     genclient.ENUM_TYPE,
}

func normalizeTypes(state *genclient.APIState, in *descriptorpb.FieldDescriptorProto, field *genclient.Field) {
	typ := in.GetType()
	field.Typez = genclient.UNDEFINED_TYPE
	if tz, ok := descriptorpbToTypez[typ]; ok {
		field.Typez = tz
	}

	switch typ {
	case descriptorpb.FieldDescriptorProto_TYPE_GROUP:
		field.TypezID = in.GetTypeName()
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
		field.TypezID = in.GetTypeName()
		// Repeated fields are not optional, they can be empty, but always have
		// presence.
		field.Optional = !field.Repeated
		if message, ok := state.MessageByID[field.TypezID]; ok {
			// Map fields appear as repeated in Protobuf. This is confusing,
			// as they typically are represented by a single `map<k, v>`-like
			// datatype. Protobuf leaks the wire-representation of maps, i.e.,
			// repeated pairs.
			field.Repeated = !message.IsMap
		}
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
		field.TypezID = in.GetTypeName()
	default:
		slog.Warn("found undefined field", "field", in.GetName())
	}

}

func processMessage(state *genclient.APIState, m *descriptorpb.DescriptorProto, mFQN string, parent *genclient.Message) *genclient.Message {
	message := &genclient.Message{
		Name:   m.GetName(),
		ID:     mFQN,
		Parent: parent,
	}
	state.MessageByID[mFQN] = message
	if opts := m.GetOptions(); opts != nil && opts.GetMapEntry() {
		message.IsMap = true
	}
	if len(m.GetNestedType()) > 0 {
		for _, nm := range m.GetNestedType() {
			nmFQN := mFQN + "." + nm.GetName()
			nmsg := processMessage(state, nm, nmFQN, message)
			message.Messages = append(message.Messages, nmsg)
		}
	}
	for _, e := range m.GetEnumType() {
		e := processEnum(state, e, mFQN, message)
		message.Enums = append(message.Enums, e)
	}
	for _, oneof := range m.OneofDecl {
		oneOfs := &genclient.OneOf{
			Name:   oneof.GetName(),
			ID:     mFQN + "." + oneof.GetName(),
			Parent: message,
		}
		message.OneOfs = append(message.OneOfs, oneOfs)
	}
	for _, mf := range m.Field {
		isProtoOptional := mf.Proto3Optional != nil && *mf.Proto3Optional
		field := &genclient.Field{
			Name:     mf.GetName(),
			ID:       mFQN + "." + mf.GetName(),
			JSONName: mf.GetJsonName(),
			Optional: isProtoOptional,
			Repeated: mf.Label != nil && *mf.Label == descriptorpb.FieldDescriptorProto_LABEL_REPEATED,
			IsOneOf:  mf.OneofIndex != nil && !isProtoOptional,
		}
		normalizeTypes(state, mf, field)
		message.Fields = append(message.Fields, field)
		if field.IsOneOf {
			message.OneOfs[*mf.OneofIndex].Fields = append(message.OneOfs[*mf.OneofIndex].Fields, field)
		}
	}

	// Remove proto3 optionals from one-of
	var oneOfIdx int
	for _, oneof := range message.OneOfs {
		if len(oneof.Fields) > 0 {
			message.OneOfs[oneOfIdx] = oneof
			oneOfIdx++
		}
	}
	if oneOfIdx == 0 {
		message.OneOfs = nil
	} else {
		message.OneOfs = message.OneOfs[:oneOfIdx]
	}

	return message
}

func processEnum(state *genclient.APIState, e *descriptorpb.EnumDescriptorProto, baseFQN string, parent *genclient.Message) *genclient.Enum {
	enum := &genclient.Enum{
		Name:   e.GetName(),
		Parent: parent,
	}
	state.EnumByID[baseFQN+"."+e.GetName()] = enum
	for _, ev := range e.Value {
		enumValue := &genclient.EnumValue{
			Name:   ev.GetName(),
			Number: ev.GetNumber(),
			Parent: enum,
		}
		enum.Values = append(enum.Values, enumValue)
	}
	return enum
}

func addServiceDocumentation(state *genclient.APIState, p []int32, doc string, sFQN string) {
	doc = trimLeadingSpacesInDocumentation(doc)
	if len(p) == 0 {
		// This is a comment for a service
		state.ServiceByID[sFQN].Documentation = doc
	} else if len(p) == 2 && p[0] == serviceDescriptorProtoMethod {
		// This is a comment for a method
		state.ServiceByID[sFQN].Methods[p[1]].Documentation = doc
	} else {
		slog.Warn("service dropped documentation", "loc", p, "docs", doc)
	}
}

func addMessageDocumentation(state *genclient.APIState, m *descriptorpb.DescriptorProto, p []int32, doc string, mFQN string) {
	doc = trimLeadingSpacesInDocumentation(doc)
	if len(p) == 0 {
		// This is a comment for a top level message
		state.MessageByID[mFQN].Documentation = doc
	} else if p[0] == messageDescriptorNestedType {
		nmsg := m.GetNestedType()[p[1]]
		nmFQN := mFQN + "." + nmsg.GetName()
		addMessageDocumentation(state, nmsg, p[2:], doc, nmFQN)
	} else if len(p) == 2 && p[0] == messageDescriptorField {
		state.MessageByID[mFQN].Fields[p[1]].Documentation = doc
	} else if p[0] == messageDescriptorEnum {
		eFQN := mFQN + "." + m.GetEnumType()[p[1]].GetName()
		addEnumDocumentation(state, p[2:], doc, eFQN)
	} else if len(p) == 2 && p[0] == messageDescriptorOneOf {
		state.MessageByID[mFQN].OneOfs[p[1]].Documentation = doc
	} else {
		slog.Warn("message dropped documentation", "loc", p, "docs", doc)
	}
}

// addEnumDocumentation adds documentation to an enum.
func addEnumDocumentation(state *genclient.APIState, p []int32, doc string, eFQN string) {
	doc = trimLeadingSpacesInDocumentation(doc)
	if len(p) == 0 {
		// This is a comment for an enum
		state.EnumByID[eFQN].Documentation = doc
	} else if len(p) == 2 && p[0] == enumDescriptorValue {
		state.EnumByID[eFQN].Values[p[1]].Documentation = doc
	} else {
		slog.Warn("enum dropped documentation", "loc", p, "docs", doc)
	}
}

// Protobuf introduces an extra space after each newline and on the first line.
func trimLeadingSpacesInDocumentation(doc string) string {
	doc = strings.TrimPrefix(doc, " ")
	doc = strings.ReplaceAll(doc, "\n ", "\n")
	return strings.TrimSuffix(doc, "\n")
}
