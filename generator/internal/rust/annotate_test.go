// Copyright 2025 Google LLC
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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestPackageNames(t *testing.T) {
	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{},
		[]*api.Service{{Name: "Workflows", Package: "google.cloud.workflows.v1"}})
	// Override the default name for test APIs ("Test").
	model.Name = "workflows-v1"
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	got := annotateModel(model, codec, "")
	want := "google_cloud_workflows_v1"
	if got.PackageNamespace != want {
		t.Errorf("mismatched package namespace, want=%s, got=%s", want, got.PackageNamespace)
	}
}

func TestServiceAnnotations(t *testing.T) {
	request := &api.Message{
		Name:    "Request",
		Package: "test",
		ID:      ".test.Request",
	}
	response := &api.Message{
		Name:    "Response",
		Package: "test",
		ID:      ".test.Response",
	}
	method := &api.Method{
		Name:         "GetResource",
		ID:           ".test.Service.GetResource",
		InputTypeID:  ".test.Request",
		OutputTypeID: ".test.Response",
		PathInfo: &api.PathInfo{
			Verb: "GET",
			PathTemplate: []api.PathSegment{
				api.NewLiteralPathSegment("/v1/resource"),
			},
		},
	}
	noHttpMethod := &api.Method{
		Name:         "DoAThing",
		ID:           ".test.Service.DoAThing",
		InputTypeID:  ".test.Request",
		OutputTypeID: ".test.Response",
	}
	service := &api.Service{
		Name:    "ResourceService",
		ID:      ".test.ResourceService",
		Package: "test",
		Methods: []*api.Method{method, noHttpMethod},
	}

	model := api.NewTestAPI(
		[]*api.Message{request, response},
		[]*api.Enum{},
		[]*api.Service{service})
	api.CrossReference(model)
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")
	wantService := &serviceAnnotations{
		Name:       "ResourceService",
		ModuleName: "resource_service",
		HasLROs:    false,
	}
	if diff := cmp.Diff(wantService, service.Codec, cmpopts.IgnoreFields(serviceAnnotations{}, "Methods")); diff != "" {
		t.Errorf("mismatch in service annotations (-want, +got)\n:%s", diff)
	}

	// The `noHttpMethod` should be excluded from the list of methods in the
	// Codec.
	serviceAnn := service.Codec.(*serviceAnnotations)
	wantMethodList := []*api.Method{method}
	if diff := cmp.Diff(wantMethodList, serviceAnn.Methods, cmpopts.IgnoreFields(api.Method{}, "Model", "Service")); diff != "" {
		t.Errorf("mismatch in method list (-want, +got)\n:%s", diff)
	}

	wantMethod := &methodAnnotation{
		Name:         "get_resource",
		BuilderName:  "GetResource",
		BodyAccessor: ".",
		PathInfo:     method.PathInfo,
		SystemParameters: []systemParameter{
			{Name: "$alt", Value: "json;enum-encoding=int"},
		},
		ServiceNameToPascal: "ResourceService",
		ServiceNameToCamel:  "resourceService",
		ServiceNameToSnake:  "resource_service",
	}
	if diff := cmp.Diff(wantMethod, method.Codec); diff != "" {
		t.Errorf("mismatch in nested message annotations (-want, +got)\n:%s", diff)
	}
}

func TestOneOfAnnotations(t *testing.T) {
	singular := &api.Field{
		Name:     "oneof_field",
		JSONName: "oneofField",
		ID:       ".test.Message.oneof_field",
		Typez:    api.STRING_TYPE,
		IsOneOf:  true,
	}
	repeated := &api.Field{
		Name:     "oneof_field_repeated",
		JSONName: "oneofFieldRepeated",
		ID:       ".test.Message.oneof_field_repeated",
		Typez:    api.STRING_TYPE,
		Repeated: true,
		IsOneOf:  true,
	}
	map_field := &api.Field{
		Name:     "oneof_field_map",
		JSONName: "oneofFieldMap",
		ID:       ".test.Message.oneof_field_map",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".test.$Map",
		Repeated: false,
		IsOneOf:  true,
	}
	group := &api.OneOf{
		Name:          "type",
		ID:            ".test.Message.type",
		Documentation: "Say something clever about this oneof.",
		Fields:        []*api.Field{singular, repeated, map_field},
	}
	message := &api.Message{
		Name:    "Message",
		ID:      ".test.Message",
		Package: "test",
		Fields:  []*api.Field{singular, repeated, map_field},
		OneOfs:  []*api.OneOf{group},
	}
	map_message := &api.Message{
		Name:    "$Map",
		ID:      ".test.$Map",
		IsMap:   true,
		Package: "test",
		Fields: []*api.Field{
			{Name: "key", Typez: api.INT32_TYPE},
			{Name: "value", Typez: api.INT32_TYPE},
		},
	}
	model := api.NewTestAPI([]*api.Message{message, map_message}, []*api.Enum{}, []*api.Service{})
	api.CrossReference(model)
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")

	// Stops the recursion when comparing fields.
	ignore := cmpopts.IgnoreFields(api.Field{}, "Group")

	if diff := cmp.Diff(&oneOfAnnotation{
		FieldName:      "r#type",
		SetterName:     "type",
		EnumName:       "Type",
		QualifiedName:  "crate::model::message::Type",
		RelativeName:   "message::Type",
		FieldType:      "crate::model::message::Type",
		DocLines:       []string{"/// Say something clever about this oneof."},
		SingularFields: []*api.Field{singular},
		RepeatedFields: []*api.Field{repeated},
		MapFields:      []*api.Field{map_field},
	}, group.Codec, ignore); diff != "" {
		t.Errorf("mismatch in oneof annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&fieldAnnotations{
		FieldName:     "oneof_field",
		SetterName:    "oneof_field",
		BranchName:    "OneofField",
		FQMessageName: "crate::model::Message",
		DocLines:      nil,
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::string::String::is_empty")]`,
		},
		FieldType:          "std::string::String",
		PrimitiveFieldType: "std::string::String",
		AddQueryParameter:  `let builder = req.get_oneof_field().iter().fold(builder, |builder, p| builder.query(&[("oneofField", p)]));`,
		KeyType:            "",
		ValueType:          "",
		ToProto:            "cnv",
	}, singular.Codec, ignore); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&fieldAnnotations{
		FieldName:     "oneof_field_repeated",
		SetterName:    "oneof_field_repeated",
		BranchName:    "OneofFieldRepeated",
		FQMessageName: "crate::model::Message",
		DocLines:      nil,
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`,
		},
		FieldType:          "std::vec::Vec<std::string::String>",
		PrimitiveFieldType: "std::string::String",
		AddQueryParameter:  `let builder = req.get_oneof_field_repeated().iter().fold(builder, |builder, p| builder.query(&[("oneofFieldRepeated", p)]));`,
		KeyType:            "",
		ValueType:          "",
		ToProto:            "cnv",
	}, repeated.Codec, ignore); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&fieldAnnotations{
		FieldName:     "oneof_field_map",
		SetterName:    "oneof_field_map",
		BranchName:    "OneofFieldMap",
		FQMessageName: "crate::model::Message",
		DocLines:      nil,
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`,
		},
		FieldType:          "std::collections::HashMap<i32,i32>",
		PrimitiveFieldType: "std::collections::HashMap<i32,i32>",
		AddQueryParameter:  `let builder = req.get_oneof_field_map().map(|p| serde_json::to_value(p).map_err(Error::serde) ).transpose()?.into_iter().fold(builder, |builder, p| { use gclient::query_parameter::QueryParameter; p.add(builder, "oneofFieldMap") });`,
		KeyType:            "i32",
		ValueType:          "i32",
		IsBoxed:            true,
		ToProto:            "cnv",
		KeyToProto:         "cnv",
		ValueToProto:       "cnv",
	}, map_field.Codec, ignore); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}
}

func TestEnumAnnotations(t *testing.T) {
	// Verify we can handle values that are not in SCREAMING_SNAKE_CASE style.
	v0 := &api.EnumValue{
		Name:          "week5",
		ID:            ".test.v1.TestEnum.week5",
		Documentation: "week5 is also documented.",
		Number:        2,
	}
	v1 := &api.EnumValue{
		Name:          "MULTI_WORD_VALUE",
		ID:            ".test.v1.TestEnum.MULTI_WORD_VALUES",
		Documentation: "MULTI_WORD_VALUE is also documented.",
		Number:        1,
	}
	v2 := &api.EnumValue{
		Name:          "VALUE",
		ID:            ".test.v1.TestEnum.VALUE",
		Documentation: "VALUE is also documented.",
		Number:        0,
	}
	enum := &api.Enum{
		Name:          "TestEnum",
		ID:            ".test.v1.TestEnum",
		Documentation: "The enum is documented.",
		Values:        []*api.EnumValue{v0, v1, v2},
	}

	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{enum}, []*api.Service{})
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")

	want := &enumAnnotation{
		Name:          "TestEnum",
		ModuleName:    "test_enum",
		QualifiedName: "crate::model::TestEnum",
		RelativeName:  "TestEnum",
		DocLines:      []string{"/// The enum is documented."},
		UniqueNames:   []*api.EnumValue{v0, v1, v2},
	}
	if diff := cmp.Diff(want, enum.Codec, cmpopts.IgnoreFields(api.EnumValue{}, "Codec", "Parent")); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&enumValueAnnotation{
		Name:     "WEEK_5",
		EnumType: "TestEnum",
		DocLines: []string{"/// week5 is also documented."},
	}, v0.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&enumValueAnnotation{
		Name:     "MULTI_WORD_VALUE",
		EnumType: "TestEnum",
		DocLines: []string{"/// MULTI_WORD_VALUE is also documented."},
	}, v1.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}
	if diff := cmp.Diff(&enumValueAnnotation{
		Name:     "VALUE",
		EnumType: "TestEnum",
		DocLines: []string{"/// VALUE is also documented."},
	}, v2.Codec); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}
}

func TestDuplicateEnumValueAnnotations(t *testing.T) {
	// Verify we can handle values that are not in SCREAMING_SNAKE_CASE style.
	v0 := &api.EnumValue{
		Name:   "full",
		ID:     ".test.v1.TestEnum.full",
		Number: 1,
	}
	v1 := &api.EnumValue{
		Name:   "FULL",
		ID:     ".test.v1.TestEnum.FULL",
		Number: 1,
	}
	v2 := &api.EnumValue{
		Name:   "partial",
		ID:     ".test.v1.TestEnum.partial",
		Number: 2,
	}
	// This does not happen in practice, but we want to verify the code can
	// handle it if it ever does.
	v3 := &api.EnumValue{
		Name:   "PARTIAL",
		ID:     ".test.v1.TestEnum.PARTIAL",
		Number: 3,
	}
	enum := &api.Enum{
		Name:   "TestEnum",
		ID:     ".test.v1.TestEnum",
		Values: []*api.EnumValue{v0, v1, v2, v3},
	}

	model := api.NewTestAPI(
		[]*api.Message{}, []*api.Enum{enum}, []*api.Service{})
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")

	want := &enumAnnotation{
		Name:          "TestEnum",
		ModuleName:    "test_enum",
		QualifiedName: "crate::model::TestEnum",
		RelativeName:  "TestEnum",
		UniqueNames:   []*api.EnumValue{v0, v2},
	}

	if diff := cmp.Diff(want, enum.Codec, cmpopts.IgnoreFields(api.EnumValue{}, "Codec", "Parent")); diff != "" {
		t.Errorf("mismatch in enum annotations (-want, +got)\n:%s", diff)
	}
}

func TestJsonNameAnnotations(t *testing.T) {
	parent := &api.Field{
		Name:     "parent",
		JSONName: "parent",
		ID:       ".test.Request.parent",
		Typez:    api.STRING_TYPE,
	}
	publicKey := &api.Field{
		Name:     "public_key",
		JSONName: "public_key",
		ID:       ".test.Request.public_key",
		Typez:    api.STRING_TYPE,
	}
	readTime := &api.Field{
		Name:     "read_time",
		JSONName: "readTime",
		ID:       ".test.Request.read_time",
		Typez:    api.INT32_TYPE,
	}
	message := &api.Message{
		Name:          "Request",
		Package:       "test",
		ID:            ".test.Request",
		Documentation: "A test message.",
		Fields:        []*api.Field{parent, publicKey, readTime},
	}
	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
	api.CrossReference(model)
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")

	if diff := cmp.Diff(&fieldAnnotations{
		FieldName:     "parent",
		SetterName:    "parent",
		BranchName:    "Parent",
		FQMessageName: "crate::model::Request",
		DocLines:      nil,
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::string::String::is_empty")]`,
		},
		FieldType:          "std::string::String",
		PrimitiveFieldType: "std::string::String",
		AddQueryParameter:  `let builder = builder.query(&[("parent", &req.parent)]);`,
		KeyType:            "",
		ValueType:          "",
		ToProto:            "cnv",
	}, parent.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&fieldAnnotations{
		FieldName:     "public_key",
		SetterName:    "public_key",
		BranchName:    "PublicKey",
		FQMessageName: "crate::model::Request",
		DocLines:      nil,
		Attributes: []string{
			`#[serde(rename = "public_key")]`,
			`#[serde(skip_serializing_if = "std::string::String::is_empty")]`,
		},
		FieldType:          "std::string::String",
		PrimitiveFieldType: "std::string::String",
		AddQueryParameter:  `let builder = builder.query(&[("public_key", &req.public_key)]);`,
		KeyType:            "",
		ValueType:          "",
		ToProto:            "cnv",
	}, publicKey.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	if diff := cmp.Diff(&fieldAnnotations{
		FieldName:          "read_time",
		SetterName:         "read_time",
		BranchName:         "ReadTime",
		FQMessageName:      "crate::model::Request",
		DocLines:           nil,
		Attributes:         []string{},
		FieldType:          "i32",
		PrimitiveFieldType: "i32",
		AddQueryParameter:  `let builder = builder.query(&[("readTime", &req.read_time)]);`,
		KeyType:            "",
		ValueType:          "",
		ToProto:            "cnv",
	}, readTime.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}
}

func TestMessageAnnotations(t *testing.T) {
	message := &api.Message{
		Name:          "TestMessage",
		Package:       "test",
		ID:            ".test.TestMessage",
		Documentation: "A test message.",
	}
	nested := &api.Message{
		Name:          "NestedMessage",
		Package:       "test",
		ID:            ".test.TestMessage.NestedMessage",
		Documentation: "A nested message.",
		Parent:        message,
	}
	message.Messages = []*api.Message{nested}

	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
	api.CrossReference(model)
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")
	want := &messageAnnotation{
		Name:          "TestMessage",
		ModuleName:    "test_message",
		QualifiedName: "crate::model::TestMessage",
		RelativeName:  "TestMessage",
		SourceFQN:     "test.TestMessage",
		MessageAttributes: []string{
			`#[serde_with::serde_as]`,
			`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
			`#[serde(default, rename_all = "camelCase")]`,
			`#[non_exhaustive]`,
		},
		DocLines:       []string{"/// A test message."},
		HasNestedTypes: true,
		BasicFields:    []*api.Field{},
		SingularFields: []*api.Field{},
		RepeatedFields: []*api.Field{},
		MapFields:      []*api.Field{},
	}
	if diff := cmp.Diff(want, message.Codec); diff != "" {
		t.Errorf("mismatch in message annotations (-want, +got)\n:%s", diff)
	}

	want = &messageAnnotation{
		Name:          "NestedMessage",
		ModuleName:    "nested_message",
		QualifiedName: "crate::model::test_message::NestedMessage",
		RelativeName:  "test_message::NestedMessage",
		SourceFQN:     "test.TestMessage.NestedMessage",
		MessageAttributes: []string{
			`#[serde_with::serde_as]`,
			`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
			`#[serde(default, rename_all = "camelCase")]`,
			`#[non_exhaustive]`,
		},
		DocLines:       []string{"/// A nested message."},
		HasNestedTypes: false,
		BasicFields:    []*api.Field{},
		SingularFields: []*api.Field{},
		RepeatedFields: []*api.Field{},
		MapFields:      []*api.Field{},
	}
	if diff := cmp.Diff(want, nested.Codec); diff != "" {
		t.Errorf("mismatch in nested message annotations (-want, +got)\n:%s", diff)
	}
}

func TestFieldAnnotations(t *testing.T) {
	map_message := &api.Message{
		Name:    "$Map",
		ID:      ".test.$Map",
		IsMap:   true,
		Package: "test",
		Fields: []*api.Field{
			{Name: "key", Typez: api.INT32_TYPE},
			{Name: "value", Typez: api.INT64_TYPE},
		},
	}
	singular_field := &api.Field{
		Name:     "singular_field",
		JSONName: "singularField",
		ID:       ".test.Message.singular_field",
		Typez:    api.STRING_TYPE,
	}
	repeated_field := &api.Field{
		Name:     "repeated_field",
		JSONName: "repeatedField",
		ID:       ".test.Message.repeated_field",
		Typez:    api.STRING_TYPE,
		Repeated: true,
	}
	map_field := &api.Field{
		Name:     "map_field",
		JSONName: "mapField",
		ID:       ".test.Message.map_field",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".test.$Map",
		Repeated: false,
	}
	boxed_field := &api.Field{
		Name:     "boxed_field",
		JSONName: "boxedField",
		ID:       ".test.Message.boxed_field",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  ".test.TestMessage",
		Optional: true,
	}
	message := &api.Message{
		Name:          "TestMessage",
		Package:       "test",
		ID:            ".test.TestMessage",
		Documentation: "A test message.",
		Fields:        []*api.Field{singular_field, repeated_field, map_field, boxed_field},
	}

	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{}, []*api.Service{})
	model.State.MessageByID[map_message.ID] = map_message
	api.CrossReference(model)
	api.LabelRecursiveFields(model)
	codec, err := newCodec(true, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")
	wantMessage := &messageAnnotation{
		Name:          "TestMessage",
		ModuleName:    "test_message",
		QualifiedName: "crate::model::TestMessage",
		RelativeName:  "TestMessage",
		SourceFQN:     "test.TestMessage",
		MessageAttributes: []string{
			`#[serde_with::serde_as]`,
			`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
			`#[serde(default, rename_all = "camelCase")]`,
			`#[non_exhaustive]`,
		},
		DocLines:       []string{"/// A test message."},
		BasicFields:    []*api.Field{singular_field, repeated_field, map_field, boxed_field},
		SingularFields: []*api.Field{singular_field, boxed_field},
		RepeatedFields: []*api.Field{repeated_field},
		MapFields:      []*api.Field{map_field},
	}
	if diff := cmp.Diff(wantMessage, message.Codec); diff != "" {
		t.Errorf("mismatch in message annotations (-want, +got)\n:%s", diff)
	}

	wantField := &fieldAnnotations{
		FieldName:     "singular_field",
		SetterName:    "singular_field",
		BranchName:    "SingularField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::string::String::is_empty")]`,
		},
		FieldType:          "std::string::String",
		PrimitiveFieldType: "std::string::String",
		AddQueryParameter:  `let builder = builder.query(&[("singularField", &req.singular_field)]);`,
		ToProto:            "cnv",
	}
	if diff := cmp.Diff(wantField, singular_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	wantField = &fieldAnnotations{
		FieldName:     "repeated_field",
		SetterName:    "repeated_field",
		BranchName:    "RepeatedField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`,
		},
		FieldType:          "std::vec::Vec<std::string::String>",
		PrimitiveFieldType: "std::string::String",
		AddQueryParameter:  `let builder = req.repeated_field.iter().fold(builder, |builder, p| builder.query(&[("repeatedField", p)]));`,
		ToProto:            "cnv",
	}
	if diff := cmp.Diff(wantField, repeated_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	wantField = &fieldAnnotations{
		FieldName:     "map_field",
		SetterName:    "map_field",
		BranchName:    "MapField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`,
			`#[serde_as(as = "std::collections::HashMap<_, serde_with::DisplayFromStr>")]`,
		},
		FieldType:          "std::collections::HashMap<i32,i64>",
		PrimitiveFieldType: "std::collections::HashMap<i32,i64>",
		AddQueryParameter:  `let builder = { use gclient::query_parameter::QueryParameter; serde_json::to_value(&req.map_field).map_err(Error::serde)?.add(builder, "mapField") };`,
		KeyType:            "i32",
		ValueType:          "i64",
		ToProto:            "cnv",
		KeyToProto:         "cnv",
		ValueToProto:       "cnv",
	}
	if diff := cmp.Diff(wantField, map_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	wantField = &fieldAnnotations{
		FieldName:     "boxed_field",
		SetterName:    "boxed_field",
		BranchName:    "BoxedField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
		},
		FieldType:          "std::option::Option<std::boxed::Box<crate::model::TestMessage>>",
		PrimitiveFieldType: "crate::model::TestMessage",
		AddQueryParameter:  `let builder = req.boxed_field.as_ref().map(|p| serde_json::to_value(p).map_err(Error::serde) ).transpose()?.into_iter().fold(builder, |builder, v| { use gclient::query_parameter::QueryParameter; v.add(builder, "boxedField") });`,
		IsBoxed:            true,
		ToProto:            "cnv",
	}
	if diff := cmp.Diff(wantField, boxed_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}
}

func TestEnumFieldAnnotations(t *testing.T) {
	enumz := &api.Enum{
		Name:    "TestEnum",
		Package: "test",
		ID:      ".test.TestEnum",
	}
	singular_field := &api.Field{
		Name:     "singular_field",
		JSONName: "singularField",
		ID:       ".test.Message.singular_field",
		Typez:    api.ENUM_TYPE,
		TypezID:  ".test.TestEnum",
	}
	repeated_field := &api.Field{
		Name:     "repeated_field",
		JSONName: "repeatedField",
		ID:       ".test.Message.repeated_field",
		Typez:    api.ENUM_TYPE,
		TypezID:  ".test.TestEnum",
		Repeated: true,
	}
	optional_field := &api.Field{
		Name:     "optional_field",
		JSONName: "optionalField",
		ID:       ".test.Message.optional_field",
		Typez:    api.ENUM_TYPE,
		TypezID:  ".test.TestEnum",
		Optional: true,
	}
	null_value_field := &api.Field{
		Name:     "null_value_field",
		JSONName: "nullValueField",
		ID:       ".test.Message.null_value_field",
		Typez:    api.ENUM_TYPE,
		TypezID:  ".google.protobuf.NullValue",
	}
	map_field := &api.Field{
		Name:     "map_field",
		JSONName: "mapField",
		ID:       ".test.Message.map_field",
		Typez:    api.MESSAGE_TYPE,
		TypezID:  "$map<string, .test.TestEnum>",
	}
	// TODO(#1381) - this is closer to what map message should be called.
	key_field := &api.Field{
		Name:     "key",
		JSONName: "key",
		ID:       "$map<string, .test.TestEnum>.key",
		Typez:    api.STRING_TYPE,
	}
	value_field := &api.Field{
		Name:     "value",
		JSONName: "value",
		ID:       "$map<string, .test.TestEnum>.value",
		Typez:    api.ENUM_TYPE,
		TypezID:  ".test.TestEnum",
	}
	map_message := &api.Message{
		Name:   "$map<string, .test.TestEnum>",
		ID:     "$map<string, .test.TestEnum>",
		IsMap:  true,
		Fields: []*api.Field{key_field, value_field},
	}
	message := &api.Message{
		Name:          "TestMessage",
		Package:       "test",
		ID:            ".test.TestMessage",
		Documentation: "A test message.",
		Fields:        []*api.Field{singular_field, repeated_field, optional_field, null_value_field, map_field},
	}

	model := api.NewTestAPI([]*api.Message{message}, []*api.Enum{enumz}, []*api.Service{})
	model.State.MessageByID[map_message.ID] = map_message
	api.CrossReference(model)
	api.LabelRecursiveFields(model)
	codec, err := newCodec(true, map[string]string{
		"package:wkt": "force-used=true,package=google-cloud-wkt,path=src/wkt,source=google.protobuf,version=0.2",
	})
	if err != nil {
		t.Fatal(err)
	}
	annotateModel(model, codec, "")
	wantMessage := &messageAnnotation{
		Name:          "TestMessage",
		ModuleName:    "test_message",
		QualifiedName: "crate::model::TestMessage",
		RelativeName:  "TestMessage",
		SourceFQN:     "test.TestMessage",
		MessageAttributes: []string{
			`#[serde_with::serde_as]`,
			`#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]`,
			`#[serde(default, rename_all = "camelCase")]`,
			`#[non_exhaustive]`,
		},
		DocLines:       []string{"/// A test message."},
		BasicFields:    []*api.Field{singular_field, repeated_field, optional_field, null_value_field, map_field},
		SingularFields: []*api.Field{singular_field, optional_field, null_value_field},
		RepeatedFields: []*api.Field{repeated_field},
		MapFields:      []*api.Field{map_field},
	}
	if diff := cmp.Diff(wantMessage, message.Codec); diff != "" {
		t.Errorf("mismatch in message annotations (-want, +got)\n:%s", diff)
	}

	wantField := &fieldAnnotations{
		FieldName:          "singular_field",
		SetterName:         "singular_field",
		BranchName:         "SingularField",
		FQMessageName:      "crate::model::TestMessage",
		Attributes:         []string{},
		FieldType:          "crate::model::TestEnum",
		PrimitiveFieldType: "crate::model::TestEnum",
		AddQueryParameter:  `let builder = builder.query(&[("singularField", &req.singular_field.value())]);`,
		ToProto:            "value",
	}
	if diff := cmp.Diff(wantField, singular_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	wantField = &fieldAnnotations{
		FieldName:     "repeated_field",
		SetterName:    "repeated_field",
		BranchName:    "RepeatedField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::vec::Vec::is_empty")]`,
		},
		FieldType:          "std::vec::Vec<crate::model::TestEnum>",
		PrimitiveFieldType: "crate::model::TestEnum",
		AddQueryParameter:  `let builder = req.repeated_field.iter().fold(builder, |builder, p| builder.query(&[("repeatedField", p.value())]));`,
		ToProto:            "value",
	}
	if diff := cmp.Diff(wantField, repeated_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	wantField = &fieldAnnotations{
		FieldName:     "optional_field",
		SetterName:    "optional_field",
		BranchName:    "OptionalField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::option::Option::is_none")]`,
		},
		FieldType:          "std::option::Option<crate::model::TestEnum>",
		PrimitiveFieldType: "crate::model::TestEnum",
		AddQueryParameter:  `let builder = req.optional_field.iter().fold(builder, |builder, p| builder.query(&[("optionalField", p.value())]));`,
		ToProto:            "value",
	}
	if diff := cmp.Diff(wantField, optional_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	// In the .proto specification this is represented as an enum. Which we
	// map to a unit struct.
	wantField = &fieldAnnotations{
		FieldName:          "null_value_field",
		SetterName:         "null_value_field",
		BranchName:         "NullValueField",
		FQMessageName:      "crate::model::TestMessage",
		Attributes:         []string{},
		FieldType:          "wkt::NullValue",
		PrimitiveFieldType: "wkt::NullValue",
		AddQueryParameter:  `let builder = builder.query(&[("nullValueField", &req.null_value_field.value())]);`,
		ToProto:            "value",
	}
	if diff := cmp.Diff(wantField, null_value_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}

	wantField = &fieldAnnotations{
		FieldName:     "map_field",
		SetterName:    "map_field",
		BranchName:    "MapField",
		FQMessageName: "crate::model::TestMessage",
		Attributes: []string{
			`#[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]`,
		},
		FieldType:          "std::collections::HashMap<std::string::String,crate::model::TestEnum>",
		PrimitiveFieldType: "std::collections::HashMap<std::string::String,crate::model::TestEnum>",
		AddQueryParameter:  `let builder = { use gclient::query_parameter::QueryParameter; serde_json::to_value(&req.map_field).map_err(Error::serde)?.add(builder, "mapField") };`,
		KeyType:            "std::string::String",
		ValueType:          "crate::model::TestEnum",
		ToProto:            "cnv",
		KeyToProto:         "cnv",
		ValueToProto:       "value",
	}
	if diff := cmp.Diff(wantField, map_field.Codec); diff != "" {
		t.Errorf("mismatch in field annotations (-want, +got)\n:%s", diff)
	}
}
