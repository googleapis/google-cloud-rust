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

package parser

import (
	"fmt"
	"log/slog"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/parser/httprule"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The types in LRO annotations sometimes (always?) are missing the leading `.`.
// We need to add them so they are useful when searching in
// `state.MessageByID[]`.
func normalizeTypeID(packagez, id string) string {
	if strings.HasPrefix(id, ".") {
		return id
	}
	if strings.Contains(id, ".") {
		// Already has a package, return the string.
		return "." + id
	}
	return fmt.Sprintf(".%s.%s", packagez, id)
}

func parseOperationInfo(packagez string, m *descriptorpb.MethodDescriptorProto) *api.OperationInfo {
	extensionId := longrunningpb.E_OperationInfo
	if !proto.HasExtension(m.GetOptions(), extensionId) {
		return nil
	}
	protobufInfo := proto.GetExtension(m.GetOptions(), extensionId).(*longrunningpb.OperationInfo)
	operationInfo := &api.OperationInfo{
		MetadataTypeID: normalizeTypeID(packagez, protobufInfo.GetMetadataType()),
		ResponseTypeID: normalizeTypeID(packagez, protobufInfo.GetResponseType()),
	}
	return operationInfo
}

func parsePathInfo(m *descriptorpb.MethodDescriptorProto, state *api.APIState) (*api.PathInfo, error) {
	eHTTP := proto.GetExtension(m.GetOptions(), annotations.E_Http)
	httpRule := eHTTP.(*annotations.HttpRule)
	return processRule(httpRule, state, m.GetInputType())
}

func processRule(httpRule *annotations.HttpRule, state *api.APIState, mID string) (*api.PathInfo, error) {
	binding, body, err := processRuleShallow(httpRule, state, mID)
	if err != nil {
		return nil, err
	}
	if binding == nil {
		return &api.PathInfo{}, nil
	}
	pathInfo := &api.PathInfo{
		BodyFieldPath: body,
		Bindings:      []*api.PathBinding{binding},
	}

	for _, binding := range httpRule.GetAdditionalBindings() {
		binding, body, err := processRuleShallow(binding, state, mID)
		if err != nil {
			return nil, err
		}
		if pathInfo.BodyFieldPath != "" && body != "" && body != pathInfo.BodyFieldPath {
			slog.Warn("mismatched body in additional binding (see AIP-127)", "message", mID, "topLevelBody", pathInfo.BodyFieldPath, "additionalBindingBody", body)
		}
		if binding != nil {
			pathInfo.Bindings = append(pathInfo.Bindings, binding)
		} else {
			slog.Warn("additional binding without a pattern", "message", mID)
		}
	}
	return pathInfo, nil
}

func processRuleShallow(httpRule *annotations.HttpRule, state *api.APIState, mID string) (*api.PathBinding, string, error) {
	var verb string
	var rawPath string
	switch httpRule.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		verb = "GET"
		rawPath = httpRule.GetGet()
	case *annotations.HttpRule_Post:
		verb = "POST"
		rawPath = httpRule.GetPost()
	case *annotations.HttpRule_Put:
		verb = "PUT"
		rawPath = httpRule.GetPut()
	case *annotations.HttpRule_Delete:
		verb = "DELETE"
		rawPath = httpRule.GetDelete()
	case *annotations.HttpRule_Patch:
		verb = "PATCH"
		rawPath = httpRule.GetPatch()
	default:
		// Most often this happens with streaming RPCs. Also some
		// services (e.g. `storagecontrol`) have RPCs without any HTTP
		// annotations.
		return nil, "", nil
	}
	pathTemplate, err := httprule.ParseSegments(rawPath)
	if err != nil {
		return nil, "", err
	}
	queryParameters, err := queryParameters(mID, pathTemplate, httpRule.GetBody(), state)
	if err != nil {
		return nil, "", err
	}

	return &api.PathBinding{
		Verb:            verb,
		PathTemplate:    pathTemplate,
		QueryParameters: queryParameters,
	}, httpRule.GetBody(), nil
}

func queryParameters(msgID string, pathTemplate *api.PathTemplate, body string, state *api.APIState) (map[string]bool, error) {
	msg, ok := state.MessageByID[msgID]
	if !ok {
		return nil, fmt.Errorf("unable to lookup type %s", msgID)
	}
	params := map[string]bool{}
	if body == "*" {
		// All parameters are body parameters.
		return params, nil
	}
	// Start with all the fields marked as query parameters.
	for _, field := range msg.Fields {
		params[field.Name] = true
	}
	for _, s := range pathTemplate.Segments {
		if s.Variable != nil {
			// TODO(#2508) - Note that nested fields are not excluded
			delete(params, strings.Join(s.Variable.FieldPath, "."))
		}
	}
	if body != "" {
		delete(params, body)
	}
	return params, nil
}

func parseDefaultHost(m proto.Message) string {
	eDefaultHost := proto.GetExtension(m, annotations.E_DefaultHost)
	defaultHost := eDefaultHost.(string)
	if defaultHost == "" {
		slog.Warn("missing default host for service", "service", m.ProtoReflect().Descriptor().FullName())
	}
	return defaultHost
}

func protobufIsAutoPopulated(field *descriptorpb.FieldDescriptorProto) bool {
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_STRING {
		return false
	}
	extensionId := annotations.E_FieldInfo
	if !proto.HasExtension(field.GetOptions(), extensionId) {
		return false
	}
	fieldInfo := proto.GetExtension(field.GetOptions(), extensionId).(*annotations.FieldInfo)
	if fieldInfo.GetFormat() != annotations.FieldInfo_UUID4 {
		return false
	}
	extensionId = annotations.E_FieldBehavior
	if !proto.HasExtension(field.GetOptions(), extensionId) {
		return true
	}
	fieldBehavior := proto.GetExtension(field.GetOptions(), extensionId).([]annotations.FieldBehavior)
	for _, b := range fieldBehavior {
		if b == annotations.FieldBehavior_REQUIRED {
			return false
		}
	}

	return true
}
