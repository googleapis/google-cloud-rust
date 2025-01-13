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
		// Most often this happens with streaming RPCs. We will handle any
		/// errors later in the code generation, maybe by ignoring the RPC.
		return &api.PathInfo{
			Verb:            "POST",
			PathTemplate:    []api.PathSegment{},
			QueryParameters: map[string]bool{},
			BodyFieldPath:   "*",
		}, nil
	}
	pathTemplate, err := httprule.ParseSegments(rawPath)
	if err != nil {
		return nil, err
	}
	queryParameters, err := queryParameters(mID, pathTemplate, httpRule.GetBody(), state)
	if err != nil {
		return nil, err
	}

	return &api.PathInfo{
		Verb:            verb,
		PathTemplate:    pathTemplate,
		QueryParameters: queryParameters,
		BodyFieldPath:   httpRule.GetBody(),
	}, nil
}

func queryParameters(msgID string, pathTemplate []api.PathSegment, body string, state *api.APIState) (map[string]bool, error) {
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
	for _, s := range pathTemplate {
		if s.FieldPath != nil {
			delete(params, *s.FieldPath)
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
