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

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
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

// updateMixinState modifies mixin method definitions based on configuration in
// the service yaml.
func updateMixinState(serviceConfig *serviceconfig.Service, api *genclient.API) {
	// Overwrite the google.api.http annotations with bindings from the Service config.
	for _, rule := range serviceConfig.GetHttp().GetRules() {
		selector := rule.GetSelector()
		if !strings.HasPrefix(selector, ".") {
			selector = "." + selector
		}
		m, match := api.State.MethodByID[selector]
		if !match {
			continue
		}
		pathInfo, err := processRule(rule, api.State, m.InputTypeID)
		if err != nil {
			slog.Error("unsupported http rule", "method", m, "rule", rule)
			continue
		}
		m.PathInfo = pathInfo
	}

	// Include any documentation from the Service config.
	for _, rule := range serviceConfig.GetDocumentation().GetRules() {
		selector := rule.GetSelector()
		if !strings.HasPrefix(selector, ".") {
			selector = "." + selector
		}
		m, ok := api.State.MethodByID[selector]
		if !ok {
			continue
		}

		m.Documentation = rule.GetDescription()
	}

	// Add some default docs for mixins if not specified in service config
	for _, service := range api.Services {
		// only process mixin services
		if !(service.Package == locationPackage || service.Package == iamPackage || service.Package == longrunningPackage) {
			continue
		}
		if service.Package == locationPackage && service.Documentation == "" {
			service.Documentation = "Manages location-related information with an API service."
		}
		if service.Package == iamPackage && service.Documentation == "" {
			service.Documentation = "Manages Identity and Access Management (IAM) policies with an API service."
		}
		if service.Package == longrunningPackage && service.Documentation == "" {
			service.Documentation = "Manages long-running operations with an API service."
		}

		for _, method := range service.Methods {
			if method.Documentation == "" {
				method.Documentation = fmt.Sprintf("%s is an RPC method of %s.", method.Name, service.Name)
			}
		}
	}
}
