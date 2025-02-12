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

package protojson

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAnyInAny(t *testing.T) {
	type Inner struct {
		Typez string `json:"@type"`
		Value string `json:"value"`
	}

	type Outer struct {
		Typez string `json:"@type"`
		Value Inner  `json:"value"`
	}

	input := durationpb.New(123 * time.Second)
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var gotInner Inner
	if err := json.Unmarshal(blob, &gotInner); err != nil {
		t.Fatal(err)
	}
	wantInner := Inner{
		Typez: "type.googleapis.com/google.protobuf.Duration",
		Value: "123s",
	}
	if diff := cmp.Diff(wantInner, gotInner); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}

	anyz, err := anypb.New(inner)
	if err != nil {
		t.Fatal(err)
	}
	blob, err = protojson.MarshalOptions{UseProtoNames: true}.Marshal(anyz)
	if err != nil {
		t.Fatal(err)
	}
	var gotOuter Outer
	if err := json.Unmarshal(blob, &gotOuter); err != nil {
		t.Fatal(err)
	}
	wantOuter := Outer{
		Typez: "type.googleapis.com/google.protobuf.Any",
		Value: Inner{
			Typez: "type.googleapis.com/google.protobuf.Duration",
			Value: "123s",
		},
	}
	if diff := cmp.Diff(wantOuter, gotOuter); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestNullInAny(t *testing.T) {
	type Null struct {
		Typez string `json:"@type"`
		Value any    `json:"value"`
	}
	input := structpb.NewNullValue()
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got Null
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := Null{
		Typez: "type.googleapis.com/google.protobuf.Value",
		Value: nil,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestBoolInAny(t *testing.T) {
	type Bool struct {
		Typez string `json:"@type"`
		Value bool   `json:"value"`
	}
	input := structpb.NewBoolValue(true)
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got Bool
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := Bool{
		Typez: "type.googleapis.com/google.protobuf.Value",
		Value: true,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestNumberInAny(t *testing.T) {
	type Number struct {
		Typez string  `json:"@type"`
		Value float64 `json:"value"`
	}
	input := structpb.NewNumberValue(1234.5)
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got Number
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := Number{
		Typez: "type.googleapis.com/google.protobuf.Value",
		Value: 1234.5,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestStringInAny(t *testing.T) {
	type String struct {
		Typez string `json:"@type"`
		Value string `json:"value"`
	}
	input := structpb.NewStringValue("1234.5")
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got String
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := String{
		Typez: "type.googleapis.com/google.protobuf.Value",
		Value: "1234.5",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestStructValue(t *testing.T) {
	type Struct struct {
		Typez string         `json:"@type"`
		Value map[string]any `json:"value"`
	}
	structz, err := structpb.NewStruct(map[string]any{
		"fieldA": "123",
		"fieldB": map[string]any{
			"fieldC": []any{"a", "b", "c"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := structpb.NewStructValue(structz)
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got Struct
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := Struct{
		Typez: "type.googleapis.com/google.protobuf.Value",
		Value: map[string]any{
			"fieldA": "123",
			"fieldB": map[string]any{
				"fieldC": []any{"a", "b", "c"},
			},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestListInAny(t *testing.T) {
	type List struct {
		Typez string `json:"@type"`
		Value []any  `json:"value"`
	}

	input, err := structpb.NewList([]any{1, 2, 3, 4, "abc"})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got List
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := List{
		Typez: "type.googleapis.com/google.protobuf.ListValue",
		Value: []any{float64(1), float64(2), float64(3), float64(4), "abc"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}

func TestStructInAny(t *testing.T) {
	type Struct struct {
		Typez string         `json:"@type"`
		Value map[string]any `json:"value"`
	}
	input, err := structpb.NewStruct(map[string]any{
		"fieldA": "a_value",
		"fieldB": map[string]any{
			"fieldC": []any{1, 2, 3, 4},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	var got Struct
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	want := Struct{
		Typez: "type.googleapis.com/google.protobuf.Struct",
		Value: map[string]any{
			"fieldA": "a_value",
			"fieldB": map[string]any{
				"fieldC": []any{float64(1), float64(2), float64(3), float64(4)},
			},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched inner, (-want, +got)\n%s", diff)
	}
}
