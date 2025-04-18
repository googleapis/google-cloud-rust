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
	"errors"
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func parseRoutingAnnotations(methodID string, m *descriptorpb.MethodDescriptorProto) ([]*api.RoutingInfo, error) {
	var info []*api.RoutingInfo
	extensionId := annotations.E_Routing
	if !proto.HasExtension(m.GetOptions(), extensionId) {
		return info, nil
	}

	rule := proto.GetExtension(m.GetOptions(), extensionId).(*annotations.RoutingRule)
	var errs []error
	for _, routing := range rule.GetRoutingParameters() {
		new, err := parseRoutingInfo(methodID, routing)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		info = append(info, new)
	}
	if len(errs) != 0 {
		return nil, errors.Join(errs...)
	}
	return info, nil
}

func parseRoutingInfo(methodID string, routing *annotations.RoutingParameter) (*api.RoutingInfo, error) {
	pathTemplate := routing.GetPathTemplate()
	fieldName := routing.GetField()
	info, err := parseRoutingPathTemplate(fieldName, pathTemplate)
	if err != nil {
		return nil, fmt.Errorf("%w, method=%s", err, methodID)
	}
	return info, nil
}

func parseRoutingPathTemplate(fieldName, pathTemplate string) (*api.RoutingInfo, error) {
	fieldPath := strings.Split(fieldName, ".")
	if pathTemplate == "" {
		info := &api.RoutingInfo{
			FieldPath: fieldPath,
			Name:      fieldName,
			Matching: api.RoutingPathSpec{
				Segments: []string{api.RoutingSegmentMulti},
			},
		}
		return info, nil
	}
	if strings.Count(pathTemplate, api.RoutingSegmentMulti) > 1 {
		return nil, fmt.Errorf("too many `**` matchers in pathTemplate=%q", pathTemplate)
	}

	pos := 0
	prefix, width := parseRoutingPrefix(pathTemplate[pos:])
	pos += width
	if !strings.HasPrefix(pathTemplate[pos:], "{") {
		return nil, fmt.Errorf("expected '{', found=%s", pathTemplate[pos:])
	}
	pos += 1
	name, match, width, err := parseRoutingVariable(fieldName, pathTemplate[pos:])
	if err != nil {
		return nil, err
	}
	pos += width
	if !strings.HasPrefix(pathTemplate[pos:], "}") {
		return nil, fmt.Errorf("expected '}', found=%s", pathTemplate[pos:])
	}
	pos += 1
	suffix := api.RoutingPathSpec{}
	if strings.HasPrefix(pathTemplate[pos:], "/") {
		pos += 1
		suffix, width = parseRoutingSuffix(pathTemplate[pos:])
		pos += width
	}
	if pathTemplate[pos:] != "" {
		return nil, fmt.Errorf("unexpected trailer in pathTemplate trailer=%s", pathTemplate[pos:])
	}
	info := &api.RoutingInfo{
		FieldPath: fieldPath,
		Name:      name,
		Prefix:    prefix,
		Matching:  match,
		Suffix:    suffix,
	}
	return info, nil
}

func parseRoutingPrefix(pathTemplate string) (api.RoutingPathSpec, int) {
	return parseRoutingPathSpec(pathTemplate)
}

func parseRoutingVariable(defaultName, pathTemplate string) (string, api.RoutingPathSpec, int, error) {
	spec, width := parseRoutingPathSpec(pathTemplate)
	if strings.HasPrefix(pathTemplate[width:], "=") {
		pos := width + 1
		// The initial spec must be a simple name.
		if len(spec.Segments) != 1 || spec.Segments[0] == api.RoutingSegmentMulti || spec.Segments[0] == api.RoutingSegmentSingle {
			return "", api.RoutingPathSpec{}, 0, fmt.Errorf("expected name=pathspec, but the name format is invalid name=%v", spec.Segments)
		}
		name := spec.Segments[0]
		spec, width := parseRoutingPathSpec(pathTemplate[pos:])
		return name, spec, pos + width, nil
	}
	return defaultName, spec, width, nil
}

func parseRoutingSuffix(pathTemplate string) (api.RoutingPathSpec, int) {
	return parseRoutingPathSpec(pathTemplate)
}

func parseRoutingPathSpec(pathTemplate string) (api.RoutingPathSpec, int) {
	segment, width := parseRoutingSegment(pathTemplate)
	if segment == "" {
		return api.RoutingPathSpec{}, width
	}
	if !strings.HasPrefix(pathTemplate[width:], "/") {
		return api.RoutingPathSpec{Segments: []string{segment}}, width
	}
	pos := width + 1
	spec, width := parseRoutingPathSpec(pathTemplate[pos:])
	spec.Segments = append([]string{segment}, spec.Segments...)
	return spec, width + pos
}

func parseRoutingSegment(pathTemplate string) (string, int) {
	if strings.HasPrefix(pathTemplate, api.RoutingSegmentMulti) {
		return api.RoutingSegmentMulti, len(api.RoutingSegmentMulti)
	}
	if strings.HasPrefix(pathTemplate, api.RoutingSegmentSingle) {
		return api.RoutingSegmentSingle, len(api.RoutingSegmentSingle)
	}
	index := strings.IndexAny(pathTemplate, "=/{}")
	if index == -1 {
		return pathTemplate, len(pathTemplate)
	}
	return pathTemplate[:index], index
}
