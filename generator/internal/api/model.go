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

import "slices"

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

// FieldBehavior represents annotations for how the code generator handles a
// field.
//
// Regardless of the underlying data type and whether it is required or optional
// on the wire, some fields must be present for requests to succeed. Or may not
// be included in a request.
type FieldBehavior int

const (
	// No special behavior.
	FIELD_BEHAVIOR_UNSPECIFIED FieldBehavior = iota

	// Specifically denotes a field as optional.
	//
	// While Google Cloud uses proto3, where fields are either optional or have
	// a default value, this may be specified for emphasis.
	FIELD_BEHAVIOR_OPTIONAL

	// Denotes a field as required.
	//
	// This indicates that the field **must** be provided as part of the request,
	// and failure to do so will cause an error (usually `INVALID_ARGUMENT`).
	//
	// Code generators may change the generated types to include this field as a
	// parameter necessary to construct the request.
	FIELD_BEHAVIOR_REQUIRED

	// Denotes a field as output only.
	//
	// Some messages (and their fields) are used in both requests and responses.
	// This indicates that the field is provided in responses, but including the
	// field in a request does nothing (the server *must* ignore it and
	// *must not* throw an error as a result of the field's presence).
	//
	// Code generators that use different builders for "the message as part of a
	// request" vs. "the standalone message" may omit this field in the former.
	FIELD_BEHAVIOR_OUTPUT_ONLY

	// Denotes a field as input only.
	//
	// This indicates that the field is provided in requests, and the
	// corresponding field is not included in output.
	FIELD_BEHAVIOR_INPUT_ONLY

	// Denotes a field as immutable.
	//
	// This indicates that the field may be set once in a request to create a
	// resource, but may not be changed thereafter.
	FIELD_BEHAVIOR_IMMUTABLE

	// Denotes that a (repeated) field is an unordered list.
	//
	// This indicates that the service may provide the elements of the list
	// in any arbitrary  order, rather than the order the user originally
	// provided. Additionally, the list's order may or may not be stable.
	FIELD_BEHAVIOR_UNORDERED_LIST

	// Denotes that this field returns a non-empty default value if not set.
	//
	// This indicates that if the user provides the empty value in a request,
	// a non-empty value will be returned. The user will not be aware of what
	// non-empty value to expect.
	FIELD_BEHAVIOR_UNORDERED_NON_EMPTY_DEFAULT

	// Denotes that the field in a resource (a message annotated with
	// google.api.resource) is used in the resource name to uniquely identify the
	// resource.
	//
	// For AIP-compliant APIs, this should only be applied to the
	// `name` field on the resource.
	//
	// This behavior should not be applied to references to other resources within
	// the message.
	//
	// The identifier field of resources often have different field behavior
	// depending on the request it is embedded in (e.g. for Create methods name
	// is optional and unused, while for Update methods it is required). Instead
	// of method-specific annotations, only `IDENTIFIER` is required.
	FIELD_BEHAVIOR_IDENTIFIER
)

// API represents and API surface.
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
	// Language specific annotations
	Codec any

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
	// Some source specifications allow marking services as deprecated.
	Deprecated bool
	// Methods associated with the Service.
	Methods []*Method
	// DefaultHost fragment of a URL.
	DefaultHost string
	// The Protobuf package this service belongs to.
	Package string
	// The model this service belongs to, mustache templates use this field to
	// navigate the data structure.
	Model *API
	// Language specific annotations
	Codec any
}

// Method defines a RPC belonging to a Service.
type Method struct {
	// Documentation for the method.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Some source specifications allow marking methods as deprecated.
	Deprecated bool
	// InputType is the input to the Method
	InputTypeID string
	InputType   *Message
	// OutputType is the output of the Method
	OutputTypeID string
	OutputType   *Message
	// Some methods return nothing and the language mapping represents such
	// method with a special type such as `void`, or `()`.
	//
	// Protobuf uses the well-known type `google.protobuf.Empty` message to
	// represent this.
	//
	// OpenAPIv3 uses a missing content field:
	//   https://swagger.io/docs/specification/v3_0/describing-responses/#empty-response-body
	ReturnsEmpty bool
	// PathInfo information about the HTTP request
	PathInfo *PathInfo
	// Pagination holds the `page_token` field if the method conforms to the
	// standard defined by [AIP-4233](https://google.aip.dev/client-libraries/4233).
	Pagination *Field
	// The streaming attributes of the method. Bidi streaming methods have both
	// set to true.
	ClientSideStreaming bool
	ServerSideStreaming bool
	// For methods returning long-running operations
	OperationInfo *OperationInfo
	// The routing annotations, if any
	Routing []*RoutingInfo
	// The model this method belongs to, mustache templates use this field to
	// navigate the data structure.
	Model *API
	// The service this method belongs to, mustache templates use this field to
	// navigate the data structure.
	Service *Service
	// Language specific annotations
	Codec any
}

func (m *Method) HasRouting() bool {
	return len(m.Routing) != 0
}

// Normalized request path information.
type PathInfo struct {
	// The list of bindings, including the top-level binding.
	Bindings []*PathBinding
	// Body is the name of the field that should be used as the body of the
	// request.
	//
	// This is a string that may be "*" which indicates that the entire request
	// should be used as the body.
	//
	// If this is empty then the body is not used.
	BodyFieldPath string
	// Language specific annotations
	Codec any
}

type PathBinding struct {
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
	PathTemplate *PathTemplate
	// Query parameter fields.
	QueryParameters map[string]bool
}

// Normalized long running operation info
type OperationInfo struct {
	// The metadata type. If there is no metadata, this is set to
	// `.google.protobuf.Empty`.
	MetadataTypeID string
	// The result type. This is the expected type when the long running
	// operation completes successfully.
	ResponseTypeID string
	// The method.
	Method *Method
	// Language specific annotations
	Codec any
}

// Normalize routing info.
//
// The routing information format is documented in:
//
// https://google.aip.dev/client-libraries/4222
//
// At a high level, it consists of a field name (from the request) that is used
// to match a certain path template. If the value of the field matches the
// template, the matching portion is added to `x-goog-request-params`.
//
// An empty `Name` field is used as the special marker to cover this case in
// AIP-4222:
//
//	An empty google.api.routing annotation is acceptable. It means that no
//	routing headers should be generated for the RPC, when they otherwise
//	would be e.g. implicitly from the google.api.http annotation.
type RoutingInfo struct {
	// The name in `x-goog-request-params`.
	Name string
	// Group the possible variants for the given name.
	//
	// The variants are parsed into the reverse order of definition. AIP-4222
	// declares:
	//
	//   In cases when multiple routing parameters have the same resource ID
	//   path segment name, thus referencing the same header key, the
	//   "last one wins" rule is used to determine which value to send.
	//
	// Reversing the order allows us to implement "the first match wins". That
	// is easier and more efficient in most languages.
	Variants []*RoutingInfoVariant
}

// The routing information stripped of its name.
type RoutingInfoVariant struct {
	// The sequence of field names accessed to get the routing information.
	FieldPath []string
	// A path template that must match the beginning of the field value.
	Prefix RoutingPathSpec
	// A path template that, if matching, is used in the `x-goog-request-params`.
	Matching RoutingPathSpec
	// A path template that must match the end of the field value.
	Suffix RoutingPathSpec
	// Language specific information
	Codec any
}

type RoutingPathSpec struct {
	// A sequence of matching segments.
	//
	// A template like `projects/*/location/*/**` maps to
	// `["projects", "*", "locations", "*", "**"]`.
	Segments []string
}

const (
	// A special routing path segment which indicates "match anything that does not include a `/`"
	RoutingSingleSegmentWildcard = "*"
	// A special routing path segment which indicates "match anything including `/`"
	RoutingMultiSegmentWildcard = "**"
)

type PathTemplate struct {
	Segments []PathSegment
	Verb     *string
}

type PathSegment struct {
	Literal  *string
	Variable *PathVariable
}

type PathVariable struct {
	FieldPath []string
	Segments  []PathVariableSegment
}

type PathVariableSegment struct {
	Literal        *string
	Match          *PathMatch
	MatchRecursive *PathMatchRecursive
}

// PathMatch represents a single '*' match.
type PathMatch struct{}

// MatchRecursive represents a '**' match.
type PathMatchRecursive struct{}

func NewPathTemplate() *PathTemplate {
	return &PathTemplate{}
}

func NewPathVariable(fields ...string) *PathVariable {
	return &PathVariable{FieldPath: fields}
}

func (p *PathTemplate) WithLiteral(l string) *PathTemplate {
	p.Segments = append(p.Segments, PathSegment{Literal: &l})
	return p
}

func (p *PathTemplate) WithVariable(v *PathVariable) *PathTemplate {
	p.Segments = append(p.Segments, PathSegment{Variable: v})
	return p
}

func (p *PathTemplate) WithVariableNamed(fields ...string) *PathTemplate {
	v := PathVariable{FieldPath: fields}
	p.Segments = append(p.Segments, PathSegment{Variable: v.WithMatch()})
	return p
}

func (p *PathTemplate) WithVerb(v string) *PathTemplate {
	p.Verb = &v
	return p
}

func (v *PathVariable) WithLiteral(l string) *PathVariable {
	v.Segments = append(v.Segments, PathVariableSegment{Literal: &l})
	return v
}

func (v *PathVariable) WithMatchRecursive() *PathVariable {
	v.Segments = append(v.Segments, PathVariableSegment{MatchRecursive: &PathMatchRecursive{}})
	return v
}

func (v *PathVariable) WithMatch() *PathVariable {
	v.Segments = append(v.Segments, PathVariableSegment{Match: &PathMatch{}})
	return v
}

// Message defines a message used in request/response handling.
type Message struct {
	// Documentation for the message.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Some source specifications allow marking messages as deprecated.
	Deprecated bool
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
	// Indicates that this Message is returned by a standard
	// List RPC and conforms to [AIP-4233](https://google.aip.dev/client-libraries/4233).
	Pagination *PaginationInfo
	// Language specific annotations.
	Codec any
}

func (m *Message) HasFields() bool {
	return len(m.Fields) != 0
}

// Information related to pagination aka [AIP-4233](https://google.aip.dev/client-libraries/4233).
type PaginationInfo struct {
	// The field that gives us the next page token.
	NextPageToken *Field
	// PageableItem is the field to be paginated over.
	PageableItem *Field
}

// Enum defines a message used in request/response handling.
type Enum struct {
	// Documentation for the message.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Some source specifications allow marking enums as deprecated.
	Deprecated bool
	// Values associated with the Enum.
	Values []*EnumValue
	// The unique integer values, some enums have multiple aliases for the
	// same number (e.g. `enum X { a = 0, b = 0, c = 1 }`).
	UniqueNumberValues []*EnumValue
	// Parent returns the ancestor of this node, if any.
	Parent *Message
	// The Protobuf package this enum belongs to.
	Package string
	// Language specific annotations.
	Codec any
}

// EnumValue defines a value in an Enum.
type EnumValue struct {
	// Documentation for the message.
	Documentation string
	// Name of the attribute.
	Name string
	// ID is a unique identifier.
	ID string
	// Some source specifications allow marking enum values as deprecated.
	Deprecated bool
	// Number of the attribute.
	Number int32
	// Parent returns the ancestor of this node, if any.
	Parent *Enum
	// Language specific annotations.
	Codec any
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
	// TypezID is the ID of the type the field refers to. This value is populated
	// for message-like types only.
	TypezID string
	// JSONName is the name of the field as it appears in JSON. Useful for
	// serializing to JSON.
	JSONName string
	// Optional indicates that the field is marked as optional in proto3.
	Optional bool

	// For a given field, at most one of `Repeated` or `Map` is true.
	//
	// Using booleans (as opposed to an enum) makes it easier to write mustache
	// templates.
	//
	// Repeated is true if the field is a repeated field.
	Repeated bool
	// Map is true if the field is a map.
	Map bool
	// Some source specifications allow marking fields as deprecated.
	Deprecated bool
	// IsOneOf is true if the field is related to a one-of and not
	// a proto3 optional field.
	IsOneOf bool
	// The OpenAPI specifications have incomplete `*Request` messages. We inject
	// some helper fields. These need to be marked so they can be excluded
	// from serialized messages and in other places.
	Synthetic bool
	// Some fields have a type that refers (sometimes indirectly) to the
	// containing message. That triggers slightly different code generation for
	// some languages.
	Recursive bool
	// AutoPopulated is true if the field meets the requirements in AIP-4235.
	// That is:
	// - It has Typez == STRING_TYPE
	// - For Protobuf, has the `google.api.field_behavior = REQUIRED` annotation
	// - For Protobuf, has the `google.api.field_info.format = UUID4` annotation
	// - For OpenAPI, it is a required field
	// - For OpenAPI, it has format == "uuid"
	// - In the service config file, it is listed in the
	//   `google.api.MethodSettings.auto_populated_fields` entry in
	//   `google.api.Publishing.method_settings`
	AutoPopulated bool
	// FieldBehavior indicates how the field behaves in requests and responses.
	//
	// For example, that a field is required in requests, or given as output
	// but ignored as input.
	Behavior []FieldBehavior
	// For fields that are part of a OneOf, the group of fields that makes the
	// OneOf.
	Group *OneOf
	// A placeholder to put language specific annotations.
	Codec any
}

func (field *Field) DocumentAsRequired() bool {
	return slices.Contains(field.Behavior, FIELD_BEHAVIOR_REQUIRED)
}

func (f *Field) Singular() bool {
	return !f.Map && !f.Repeated
}

func (f *Field) NameEqualJSONName() bool {
	return f.JSONName == f.Name
}

// Pair is a key-value pair.
type Pair struct {
	// Key of the pair.
	Key string
	// Value of the pair.
	Value string
}

// A group of fields that are mutually exclusive. Notably, proto3 optional
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
	// A placeholder to put language specific annotations.
	Codec any
}
