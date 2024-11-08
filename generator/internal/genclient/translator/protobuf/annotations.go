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
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func parseHTTPInfo(m proto.Message) *genclient.HTTPInfo {
	eHTTP := proto.GetExtension(m, annotations.E_Http)
	httpRule := eHTTP.(*annotations.HttpRule)
	var info *genclient.HTTPInfo
	switch httpRule.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		info = &genclient.HTTPInfo{
			Method:  "GET",
			RawPath: httpRule.GetGet(),
		}
	case *annotations.HttpRule_Post:
		info = &genclient.HTTPInfo{
			Method:  "POST",
			RawPath: httpRule.GetPost(),
		}
	case *annotations.HttpRule_Put:
		info = &genclient.HTTPInfo{
			Method:  "PUT",
			RawPath: httpRule.GetPut(),
		}
	case *annotations.HttpRule_Delete:
		info = &genclient.HTTPInfo{
			Method:  "DELETE",
			RawPath: httpRule.GetDelete(),
		}
	case *annotations.HttpRule_Patch:
		info = &genclient.HTTPInfo{
			Method:  "PATCH",
			RawPath: httpRule.GetPatch(),
		}
	default:
		slog.Warn("unsupported http method", "method", httpRule.GetPattern())
	}
	if info != nil {
		info.Body = httpRule.GetBody()
	}
	return info
}

func parsePathInfo(m *descriptorpb.MethodDescriptorProto, state *genclient.APIState) (*genclient.PathInfo, error) {
	eHTTP := proto.GetExtension(m.GetOptions(), annotations.E_Http)
	httpRule := eHTTP.(*annotations.HttpRule)
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
		return nil, fmt.Errorf("unsupported http method: %q", httpRule.GetPattern())
	}
	pathTemplate := parseRawPath(rawPath)
	queryParameters, err := queryParameters(m, pathTemplate, httpRule.GetBody(), state)
	if err != nil {
		return nil, err
	}

	return &genclient.PathInfo{
		Verb:            verb,
		PathTemplate:    pathTemplate,
		QueryParameters: queryParameters,
		BodyFieldPath:   httpRule.GetBody(),
	}, nil
}

func queryParameters(m *descriptorpb.MethodDescriptorProto, pathTemplate []genclient.PathSegment, body string, state *genclient.APIState) (map[string]bool, error) {
	msg, ok := state.MessageByID[m.GetInputType()]
	if !ok {
		return nil, fmt.Errorf("unable to lookup type %s", m.GetInputType())
	}
	params := map[string]bool{}
	// Start with all the fields marked as query parameters.
	for _, field := range msg.Fields {
		params[field.Name] = true
	}
	for _, s := range pathTemplate {
		if s.FieldPath != nil {
			delete(params, *s.FieldPath)
		}
	}
	if body != "" && body != "*" {
		delete(params, body)
	}
	return params, nil
}

func parseRawPath(rawPath string) []genclient.PathSegment {
	// TODO(#121) - use a proper parser for the template syntax
	template := genclient.HTTPPathVarRegex.ReplaceAllStringFunc(rawPath, func(s string) string {
		members := strings.Split(s, "=")
		if len(members) == 1 {
			return members[0]
		}
		return members[0] + "}"
	})
	segments := []genclient.PathSegment{}
	for idx, component := range strings.Split(template, ":") {
		if idx != 0 {
			segments = append(segments, genclient.PathSegment{Verb: &component})
			continue
		}
		for _, element := range strings.Split(component, "/") {
			if element == "" {
				continue
			}
			if strings.HasPrefix(element, "{") && strings.HasSuffix(element, "}") {
				element = element[1 : len(element)-1]
				segments = append(segments, genclient.PathSegment{FieldPath: &element})
				continue
			}
			segments = append(segments, genclient.PathSegment{Literal: &element})
		}
	}
	return segments
}

func parseDefaultHost(m proto.Message) string {
	eDefaultHost := proto.GetExtension(m, annotations.E_DefaultHost)
	defaultHost := eDefaultHost.(string)
	if defaultHost == "" {
		slog.Warn("missing default host for service", "service", m.ProtoReflect().Descriptor().FullName())
	}
	return defaultHost
}

// TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/27
