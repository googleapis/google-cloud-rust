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

package api

import (
	"fmt"
	"strings"
)

// Typez represent different field types that may be found in messages.
type Typez int

const (
	// These are the different field types as defined in
	// descriptorpb.FieldDescriptorProto_Type
	UNDEFINED_TYPE Typez = iota // 0
	DOUBLE_TYPE                 // 1
	FLOAT_TYPE                  // 2
	INT64_TYPE                  // 3
	UINT64_TYPE                 // 4
	INT32_TYPE                  // 5
	FIXED64_TYPE                // 6
	FIXED32_TYPE                // 7
	BOOL_TYPE                   // 8
	STRING_TYPE                 // 9
	GROUP_TYPE                  // 10
	MESSAGE_TYPE                // 11
	BYTES_TYPE                  // 12
	UINT32_TYPE                 // 13
	ENUM_TYPE                   // 14
	SFIXED32_TYPE               // 15
	SFIXED64_TYPE               // 16
	SINT32_TYPE                 // 17
	SINT64_TYPE                 // 18
)

// API represents an API surface to be generated. Aside from the general API information and its optional list of
// services, it also contains the types required for code generation.
// The type hierarchy is defined by Non-nested vs Nested and Local vs Mixin types, explained as following:
//
//   - Non-nested types are Enum and Message types, defined at the top level of an API.
//     Non-nested types have their respective Enum.Parent and Message.Parent fields set to nil.
//   - Nested types are Enum and Message types defined inline within an enclosing Message.
//     Nested types have their respective Enum.Parent and Message.Parent fields set to the enclosing Message.
//   - Local types are Enum and Message types defined within the API being evaluated.
//     Local types have their respective Enum.API and Message.API fields set to the API instance.
//   - Mixin types are Enum and Message types defined outside the API, but referenced by it.
//     Mixin types have their respective Enum.API and Message.API fields set to nil.
//
// The structure of the API represents the hierarchy of its types as follows:
//
//   - API.Messages contains only local, non-nested messages. These are reusable message types defined in the API.
//   - API.Enums contains only local, non-nested enums. These are reusable enum types defined in the API.
//   - Message.Messages contains only nested messages declared inline within the enclosing message.
//   - Message.Enums contains only nested enums declared inline within the enclosing message.
//   - All Message and Enum instances, regardless of whether they are non-nested or nested, local or mixin are available
//     in the respective APIState.MessageByID and APIState.EnumByID.
type API struct {
	// Name of the API (e.g. secretmanager).
	Name string
	// Name of the package name in the source specification format. For Protobuf
	// this may be `google.cloud.secretmanager.v1`.
	PackageName string
	// The API Title (e.g. "Secret Manager API" or "Cloud Spanner API").
	Title string
	// The API Description
	Description string
	// Services are a collection of services that make up the API.
	Services []*Service
	// Messages are a collection of messages used to process request and
	// responses in the API.
	Messages []*Message
	// Enums
	Enums []*Enum
	// State contains helpful information that can be used when generating
	// clients.
	State *APIState
}

// APIState contains helpful information that can be used when generating
// clients.
type APIState struct {
	// ServiceByID returns a service that is associated with the API.
	ServiceByID map[string]*Service
	// MethodByID returns a method that is associated with the API.
	MethodByID map[string]*Method
	// MessageByID returns a message that is associated with the API.
	MessageByID map[string]*Message
	// EnumByID returns a message that is associated with the API.
	EnumByID map[string]*Enum
}

// Service represents a service in an API.
type Service struct {
	// Documentation for the service.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Methods associated with the Service.
	Methods []*Method
	// DefaultHost fragment of a URL.
	DefaultHost string
	// The Protobuf package this service belongs to.
	Package string
	// The API that this service belongs to.
	API *API
}

// Method defines a RPC belonging to a Service.
type Method struct {
	// Documentation for the method.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// InputTypeID is the ID of the input to the Method, to be used with the API state to retrieve the message.
	InputTypeID string
	// InputType is the input to the Method, it is only present after the Method struct has been
	// visited by the CrossReferencingVisitor.
	InputType *Message
	// OutputTypeID is the output of the Method, to be used with the API state to retrieve the message.
	OutputTypeID string
	// OutputType is the output to the Method, it is only present after the Method struct has been
	// visited by the CrossReferencingVisitor.
	OutputType *Message
	// PathInfo information about the HTTP request
	PathInfo *PathInfo
	// IsPageable is true if the method conforms to standard defined by
	// [AIP-4233](https://google.aip.dev/client-libraries/4233).
	IsPageable bool
	// The service that contains this method.
	Parent *Service
	// The streaming attributes of the method. Bidi streaming methods have both
	// set to true.
	ClientSideStreaming bool
	ServerSideStreaming bool
	// For methods returning long-running operations
	OperationInfo *OperationInfo
}

// PathInfo is a normalized request path information.
type PathInfo struct {
	// HTTP Verb.
	//
	// This is one of:
	// - GET
	// - POST
	// - PUT
	// - DELETE
	// - PATCH
	Verb string
	// The path broken by components.
	PathTemplate []PathSegment
	// Query parameter fields.
	QueryParameters map[string]bool
	// Body is the name of the field that should be used as the body of the
	// request.
	//
	// This is a string that may be "*" which indicates that the entire request
	// should be used as the body.
	//
	// If this is empty then the body is not used.
	BodyFieldPath string
	// The method that this path info is associated with.
	Method *Method
	// Codec is an optional language-specific struct that helps convert PathInfo values into language-specific code.
	Codec any
}

// OperationInfo is a normalized long-running operation info
type OperationInfo struct {
	// The metadata type. If there is no metadata, this is set to
	// `.google.protobuf.Empty`.
	MetadataTypeID string
	// The result type. This is the expected type when the long-running
	// operation completes successfully.
	ResponseTypeID string
}

// PathSegment is either a string literal (such as "projects") or a field
// path (such as "options.version").
//
// For OpenAPI these are formed by breaking the path string. Something like
//
//	`/v1/projects/{project}/secrets/{secret}:getIamPolicy`
//
// should produce:
// ```
//
//	[]PathSegment{
//	  {Literal:   &"v1"},
//	  {Literal:   &"projects"},
//	  {FieldPath: &"project"},
//	  {Literal:   &"secrets"},
//	  {FieldPath: &"secret"},
//	  {Verb:      &"getIamPolicy"},
//	}
//
// ```
//
// The Codec interpret these elements as needed.
type PathSegment struct {
	Literal   *Literal
	FieldPath *FieldPath
	Verb      *PathTemplateVerb

	// Parent is the PathInfo that this segment is associated with.
	Parent *PathInfo
}

type Literal struct {
	Value string
}

type PathTemplateVerb struct {
	Value string
}

type FieldPath struct {
	Components []*FieldPathComponent
}

func (f *FieldPath) String() string {
	components := make([]string, len(f.Components))
	for i, c := range f.Components {
		components[i] = c.Identifier
	}
	return strings.Join(components, ".")
}

type FieldPathComponent struct {
	Identifier string
	Reference  *MessageElement
}

func NewLiteralPathSegment(s string) PathSegment {
	return PathSegment{Literal: &Literal{s}}
}

func NewFieldPathPathSegment(c ...*FieldPathComponent) PathSegment {
	return PathSegment{FieldPath: &FieldPath{Components: c}}
}

func NewFieldPathPathSegmentComponent(identifier string, reference *MessageElement) *FieldPathComponent {
	return &FieldPathComponent{Identifier: identifier, Reference: reference}
}

func NewVerbPathSegment(s string) PathSegment {
	return PathSegment{Verb: &PathTemplateVerb{s}}
}

// Message defines a message used in request/response handling.
type Message struct {
	// Documentation for the message.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Fields associated with the Message.
	Fields []*Field
	// IsLocalToPackage is true if the message is defined in the current
	// namespace.
	IsLocalToPackage bool
	// Enums associated with the Message.
	Enums []*Enum
	// Messages associated with the Message. In protobuf these are referred to as
	// nested messages.
	Messages []*Message
	// OneOfs associated with the Message.
	OneOfs []*OneOf
	// Parent returns the ancestor of this message, if any.
	Parent *Message
	// The Protobuf package this message belongs to.
	Package string
	IsMap   bool
	// IsPageableResponse indicated that this Message is returned by a standard
	// List RPC and conforms to [AIP-4233](https://google.aip.dev/client-libraries/4233).
	IsPageableResponse bool
	// PageableItem is the field to be paginated over.
	PageableItem *Field
	// The API that this message belongs to.
	API *API
	// ElementsByName is a map of all the elements in the message, keyed by their name.
	// This field is only available after the message has been visited by the CrossReferencingVisitor.
	ElementsByName map[string]*MessageElement
}

// MessageElement wraps around the possible element types that may be contained within a message
type MessageElement struct {
	Message *Message
	Field   *Field
	Enum    *Enum
	OneOf   *OneOf
	// Parent is the message containing this element.
	Parent *Message
	Codec  any
}

func (m *MessageElement) Name() string {
	if m.Message != nil {
		return m.Message.Name
	}
	if m.Field != nil {
		return m.Field.Name
	}
	if m.Enum != nil {
		return m.Enum.Name
	}
	if m.OneOf != nil {
		return m.OneOf.Name
	}
	return "Unknown"
}

func (m *MessageElement) String() string {
	if m.Message != nil {
		return fmt.Sprintf("Message(%s)", m.Message.ID)
	}
	if m.Field != nil {
		return fmt.Sprintf("Field(%s)", m.Field.ID)
	}
	if m.Enum != nil {
		return fmt.Sprintf("Enum(%s)", m.Enum.ID)
	}
	if m.OneOf != nil {
		return fmt.Sprintf("OneOf(%s)", m.OneOf.ID)
	}
	return "Unknown"
}

func (m *MessageElement) Optional() bool {
	if m.Field != nil {
		return m.Field.Optional
	}
	return true
}

// Enum defines a message used in request/response handling.
type Enum struct {
	// Documentation for the message.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Values associated with the Enum.
	Values []*EnumValue
	// Parent returns the ancestor of this node, if any.
	Parent *Message
	// API references the API where this enum is defined
	API *API
	// The Protobuf package this enum belongs to.
	Package string
}

// EnumValue defines a value in an Enum.
type EnumValue struct {
	// Documentation for the message.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Number of the attribute.
	Number int32
	// Parent returns the ancestor of this node, if any.
	Parent *Enum
}

// Field defines a field in a Message.
type Field struct {
	// Documentation for the field.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Typez is the datatype of the field.
	Typez Typez
	// TypezID is the ID is the ID of the type the field refers to. This value
	// is populated for message-like types only.
	TypezID string
	// JSONName is the name of the field as it appears in JSON. Useful for
	// serializing to JSON.
	JSONName string
	// Optional indicates that the field is marked as optional in proto3.
	Optional bool
	// Repeated is true if the field is a repeated field.
	Repeated bool
	// IsOneOf is true if the field is related to a one-of and not
	// a proto3 optional field.
	IsOneOf bool
	// The OpenAPI specifications have incomplete `*Request` messages. We inject
	// some helper fields. These need to be marked so they can be excluded
	// from serialized messages and in other places.
	Synthetic bool
	// Parent returns the message that this field is associated with.
	Parent *Message
}

// Pair is a key-value pair.
type Pair struct {
	// Key of the pair.
	Key string
	// Value of the pair.
	Value string
}

// OneOf is a group of fields that are mutually exclusive. Notably, proto3 optional
// fields are all their own one-of.
type OneOf struct {
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Documentation for the field.
	Documentation string
	// Fields associated with the one-of.
	Fields []*Field
	// Parent returns the ancestor of this node, if any.
	Parent *Message
}
