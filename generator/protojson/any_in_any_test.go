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
)

type Inner struct {
	Typez string `json:"@type"`
	Value string `json:"value"`
}

type Outer struct {
	Typez string `json:"@type"`
	Value Inner  `json:"value"`
}

func TestAnyInAny(t *testing.T) {
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
