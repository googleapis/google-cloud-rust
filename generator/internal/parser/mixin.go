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

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	locationPackage    = "google.cloud.location"
	locationService    = locationPackage + ".Locations"
	iamPackage         = "google.iam.v1"
	iamService         = iamPackage + ".IAMPolicy"
	longrunningPackage = "google.longrunning"
	longrunningService = longrunningPackage + ".Operations"
)

type mixinMethods map[string]bool

// loadMixins loads file descriptors for configured mixins.
func loadMixins(serviceConfig *serviceconfig.Service) (mixinMethods, []*descriptorpb.FileDescriptorProto) {
	var files []*descriptorpb.FileDescriptorProto
	var enabledMixinMethods mixinMethods = make(map[string]bool)
	apis := serviceConfig.GetApis()
	if len(apis) < 2 {
		return enabledMixinMethods, files
	}
	for _, api := range apis {
		switch api.GetName() {
		case locationService:
			files = append(files, protodesc.ToFileDescriptorProto(location.File_google_cloud_location_locations_proto))
		case iamService:
			files = append(files, protodesc.ToFileDescriptorProto(iampb.File_google_iam_v1_iam_policy_proto),
				protodesc.ToFileDescriptorProto(iampb.File_google_iam_v1_policy_proto),
				protodesc.ToFileDescriptorProto(iampb.File_google_iam_v1_options_proto))
		case longrunningService:
			files = append(files, protodesc.ToFileDescriptorProto(longrunningpb.File_google_longrunning_operations_proto))
		}
	}
	enabledMixinMethods = loadMixinMethods(serviceConfig)
	return enabledMixinMethods, files
}

// loadMixinMethods determines which mixins methods should be generated.
func loadMixinMethods(serviceConfig *serviceconfig.Service) mixinMethods {
	var enabledMixinMethods mixinMethods = make(map[string]bool)
	for _, rule := range serviceConfig.GetHttp().GetRules() {
		selector := rule.GetSelector()
		if !strings.HasPrefix(selector, ".") {
			selector = "." + selector
		}
		enabledMixinMethods[selector] = true
	}
	return enabledMixinMethods
}

// Apply `serviceConfig` overrides to `targetMethod`.
//
// The service config file may include overrides to mixin method definitions.
// These overrides reference the original fully-qualified name of the method,
// but should be applied to each copy of the method.
func applyServiceConfigMethodOverrides(
	targetMethod *api.Method,
	originalID string,
	serviceConfig *serviceconfig.Service,
	api *api.API,
	mixin *api.Service) {
	for _, rule := range serviceConfig.GetHttp().GetRules() {
		selector := rule.GetSelector()
		if !strings.HasPrefix(selector, ".") {
			selector = "." + selector
		}
		if selector != originalID {
			continue
		}
		pathInfo, err := processRule(rule, api.State, targetMethod.InputTypeID)
		if err != nil {
			slog.Error("unsupported http rule", "method", targetMethod, "rule", rule)
			continue
		}
		targetMethod.PathInfo = pathInfo
	}

	for _, rule := range serviceConfig.GetDocumentation().GetRules() {
		selector := rule.GetSelector()
		if !strings.HasPrefix(selector, ".") {
			selector = "." + selector
		}
		if selector != originalID {
			continue
		}
		targetMethod.Documentation = rule.GetDescription()
	}
	if targetMethod.Documentation == "" {
		targetMethod.Documentation = fmt.Sprintf("Provides the [%s][%s] service functionality in this service.", mixin.Name, mixin.ID[1:])
	}
}
