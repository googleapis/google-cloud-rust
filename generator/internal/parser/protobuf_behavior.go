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

package parser

import (
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func protobufFieldBehavior(field *descriptorpb.FieldDescriptorProto) []api.FieldBehavior {
	extensionId := annotations.E_FieldBehavior
	if !proto.HasExtension(field.GetOptions(), extensionId) {
		return nil
	}
	var behavior []api.FieldBehavior
	for _, b := range proto.GetExtension(field.GetOptions(), extensionId).([]annotations.FieldBehavior) {
		behavior = append(behavior, api.FieldBehavior(b))
	}
	return behavior
}
