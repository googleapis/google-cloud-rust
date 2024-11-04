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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

type Options struct {
	Language string
	// Only used for local testing
	OutDir      string
	TemplateDir string
}

// Translate translates proto representation into a [genclienGenerateRequest].
func Translate(req *pluginpb.CodeGeneratorRequest, opts *Options) (*genclient.GenerateRequest, error) {
	state := &genclient.APIState{
		ServiceByID: make(map[string]*genclient.Service),
		MessageByID: make(map[string]*genclient.Message),
		EnumByID:    make(map[string]*genclient.Enum),
	}

	api := &genclient.API{
		//TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/38
		Name: "secretmanager",
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
				addMessageDocumentation(state, m, p[2:], strings.TrimSpace(loc.GetLeadingComments()), fFQN+"."+m.GetName())
			case 6:
				sFQN := fFQN + "." + f.GetService()[p[1]].GetName()
				addServiceDocumentation(state, p[2:],
					strings.TrimSpace(loc.GetLeadingComments()), sFQN)
			default:
				slog.Warn("file dropped documentation", "loc", p, "docs", loc.GetLeadingComments())
			}
		}
		api.Services = append(api.Services, fileServices...)
	}

	codec, err := language.NewCodec(opts.Language)
	if err != nil {
		return nil, err
	}
	api.State = state
	return &genclient.GenerateRequest{
		API:         api,
		Codec:       codec,
		OutDir:      opts.OutDir,
		TemplateDir: opts.TemplateDir,
	}, nil
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

func normalizeTypes(in *descriptorpb.FieldDescriptorProto, field *genclient.Field) {
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
	// These magic numbers come from reading the proto docs. They come
	// from field numbers of the different descriptor types. See struct
	// tags on https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#ServiceDescriptorProto.
	if len(p) == 0 {
		// This is a comment for a service
		state.ServiceByID[sFQN].Documentation = doc
	} else if len(p) == 2 && p[0] == 2 {
		// This is a comment for a method
		state.ServiceByID[sFQN].Methods[p[1]].Documentation = doc
	} else {
		slog.Warn("service dropped documentation", "loc", p, "docs", doc)
	}
}

func addMessageDocumentation(state *genclient.APIState, m *descriptorpb.DescriptorProto, p []int32, doc string, mFQN string) {
	// These magic numbers come from reading the proto docs. They come
	// from field numbers of the different descriptor types. See struct
	// tags on https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb#DescriptorProto.
	if len(p) == 0 {
		// This is a comment for a top level message
		state.MessageByID[mFQN].Documentation = doc
	} else if p[0] == 3 {
		// This indicates a nested message, recurse.
		nmsg := m.GetNestedType()[p[1]]
		nmFQN := mFQN + "." + nmsg.GetName()
		addMessageDocumentation(state, nmsg, p[2:], doc, nmFQN)
	} else if len(p) == 2 && p[0] == 2 {
		// This is a comment for a field of a message
		state.MessageByID[mFQN].Fields[p[1]].Documentation = doc
	} else if p[0] == 4 {
		// This is a comment for a enum of a message
		eFQN := mFQN + "." + m.GetEnumType()[p[1]].GetName()
		addEnumDocumentation(state, p[2:], doc, eFQN)
	} else if len(p) == 2 && p[0] == 8 {
		// This is a comment for a field of a message one-of, skipping
	} else {
		slog.Warn("message dropped documentation", "loc", p, "docs", doc)
	}
}

// addEnumDocumentation adds documentation to an enum.
func addEnumDocumentation(state *genclient.APIState, p []int32, doc string, eFQN string) {
	if len(p) == 0 {
		// This is a comment for an enum
		state.EnumByID[eFQN].Documentation = doc
	} else if len(p) == 2 {
		// This is a comment for an enum value
		state.EnumByID[eFQN].Values[p[1]].Documentation = doc
	} else {
		slog.Warn("enum dropped documentation", "loc", p, "docs", doc)
	}
}
