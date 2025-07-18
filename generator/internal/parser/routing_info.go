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
	"maps"
	"slices"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func parseRoutingAnnotations(methodID string, m *descriptorpb.MethodDescriptorProto) ([]*api.RoutingInfo, error) {
	extensionId := annotations.E_Routing
	if !proto.HasExtension(m.GetOptions(), extensionId) {
		return nil, nil
	}

	rule := proto.GetExtension(m.GetOptions(), extensionId).(*annotations.RoutingRule)
	var errs []error
	collect := map[string]*api.RoutingInfo{}
	for _, routing := range rule.GetRoutingParameters() {
		new, err := parseRoutingInfo(methodID, routing)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		current, ok := collect[new.Name]
		if !ok {
			collect[new.Name] = new
			continue
		}
		current.Variants = append(new.Variants, current.Variants...)
	}
	if len(errs) != 0 {
		return nil, errors.Join(errs...)
	}
	var info []*api.RoutingInfo
	for _, k := range slices.Sorted(maps.Keys(collect)) {
		info = append(info, collect[k])
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
	if fieldName == "" && pathTemplate == "" {
		// AIP-4222: empty routing infos mean something special.
		info := &api.RoutingInfo{
			Name: fieldName,
			Variants: []*api.RoutingInfoVariant{{
				FieldPath: []string{},
				Matching: api.RoutingPathSpec{
					Segments: []string{},
				},
			}},
		}
		return info, nil
	}
	fieldPath := strings.Split(fieldName, ".")
	if pathTemplate == "" {
		info := &api.RoutingInfo{
			Name: fieldName,
			Variants: []*api.RoutingInfoVariant{{
				FieldPath: fieldPath,
				Matching: api.RoutingPathSpec{
					Segments: []string{api.MultiSegmentWildcard},
				},
			}},
		}
		return info, nil
	}
	if strings.Count(pathTemplate, api.MultiSegmentWildcard) > 1 {
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
	index := slices.Index(prefix.Segments, api.MultiSegmentWildcard)
	if index != -1 {
		return nil, fmt.Errorf("multi segment wildcards may not appear in the prefix portion of a path template, template=%s", pathTemplate)
	}
	for _, spec := range []*api.RoutingPathSpec{&match, &suffix} {
		index := slices.Index(spec.Segments, api.MultiSegmentWildcard)
		if index == -1 || index == len(spec.Segments)-1 {
			continue
		}
		return nil, fmt.Errorf("multi segment wildcards may only appear at the end of a path template, template=%s", pathTemplate)
	}
	if pathTemplate[pos:] != "" {
		return nil, fmt.Errorf("unexpected trailer in pathTemplate trailer=%s", pathTemplate[pos:])
	}
	info := &api.RoutingInfo{
		Name: name,
		Variants: []*api.RoutingInfoVariant{{
			FieldPath: fieldPath,
			Prefix:    prefix,
			Matching:  match,
			Suffix:    suffix,
		}},
	}
	return info, nil
}

func parseRoutingPrefix(pathTemplate string) (api.RoutingPathSpec, int) {
	return parseRoutingPathSpec(pathTemplate)
}

func isRoutingWildcard(segment string) bool {
	return segment == api.SingleSegmentWildcard || segment == api.MultiSegmentWildcard
}

func parseRoutingVariable(defaultName, pathTemplate string) (string, api.RoutingPathSpec, int, error) {
	spec, width := parseRoutingPathSpec(pathTemplate)
	if strings.HasPrefix(pathTemplate[width:], "=") {
		pos := width + 1
		// The initial spec must be a simple name.
		if len(spec.Segments) != 1 || isRoutingWildcard(spec.Segments[0]) {
			return "", api.RoutingPathSpec{}, 0, fmt.Errorf("expected name=pathspec, but the name format is invalid name=%v", spec.Segments)
		}
		name := spec.Segments[0]
		spec, width := parseRoutingPathSpec(pathTemplate[pos:])
		return name, spec, pos + width, nil
	}
	if len(spec.Segments) == 1 && !isRoutingWildcard(spec.Segments[0]) {
		// AIP-4222: It is acceptable to omit the pattern in the resource ID
		// segment, `{parent}` for example, is equivalent to `{parent=*}`.
		return spec.Segments[0], api.RoutingPathSpec{Segments: []string{"*"}}, width, nil
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
	if strings.HasPrefix(pathTemplate, api.MultiSegmentWildcard) {
		return api.MultiSegmentWildcard, len(api.MultiSegmentWildcard)
	}
	if strings.HasPrefix(pathTemplate, api.SingleSegmentWildcard) {
		return api.SingleSegmentWildcard, len(api.SingleSegmentWildcard)
	}
	index := strings.IndexAny(pathTemplate, "=/{}")
	if index == -1 {
		return pathTemplate, len(pathTemplate)
	}
	return pathTemplate[:index], index
}
