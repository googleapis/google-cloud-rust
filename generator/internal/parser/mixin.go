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
	locationpb "google.golang.org/genproto/googleapis/cloud/location"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	exprpb "google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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
func loadMixins(serviceConfig *serviceconfig.Service, withLongrunning bool) (mixinMethods, []*descriptorpb.FileDescriptorProto) {
	var files []*descriptorpb.FileDescriptorProto
	var enabledMixinMethods mixinMethods = make(map[string]bool)
	var apiNames []string
	hasLongrunning := false
	for _, api := range serviceConfig.GetApis() {
		// Only insert the service if needed. We want to preserve the order
		// to make the generated code reproducible, so we cannot use a map.
		if api.GetName() == longrunningService {
			hasLongrunning = true
		}
		apiNames = append(apiNames, api.GetName())
	}
	if withLongrunning && !hasLongrunning {
		apiNames = append(apiNames, longrunningService)
	}
	if len(apiNames) < 2 {
		return enabledMixinMethods, files
	}
	known := map[string]bool{}
	appendIfNew := func(desc protoreflect.FileDescriptor) {
		file := protodesc.ToFileDescriptorProto(desc)
		if _, ok := known[file.GetName()]; ok {
			return
		}
		known[file.GetName()] = true
		files = append(files, file)
	}
	for _, apiName := range apiNames {
		switch apiName {
		case locationService:
			appendIfNew(anypb.File_google_protobuf_any_proto)
			appendIfNew(locationpb.File_google_cloud_location_locations_proto)
		case iamService:
			appendIfNew(fieldmaskpb.File_google_protobuf_field_mask_proto)
			appendIfNew(exprpb.File_google_type_expr_proto)
			appendIfNew(iampb.File_google_iam_v1_iam_policy_proto)
			appendIfNew(iampb.File_google_iam_v1_policy_proto)
			appendIfNew(iampb.File_google_iam_v1_options_proto)
		case longrunningService:
			appendIfNew(anypb.File_google_protobuf_any_proto)
			appendIfNew(durationpb.File_google_protobuf_duration_proto)
			appendIfNew(emptypb.File_google_protobuf_empty_proto)
			appendIfNew(statuspb.File_google_rpc_status_proto)
			appendIfNew(longrunningpb.File_google_longrunning_operations_proto)
		}
	}
	enabledMixinMethods = loadMixinMethods(serviceConfig)
	if withLongrunning {
		// We prefer using the `http.rules` section from the service config, but
		// if we must implement the longrunning mixin, we must also implement
		// the GetOperation method.
		enabledMixinMethods[".google.longrunning.Operations.GetOperation"] = true
	}
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
