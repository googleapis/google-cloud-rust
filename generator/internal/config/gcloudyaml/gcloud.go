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

package gcloudyaml

// Config represents the top-level schema of a gcloud config YAML file.
type Config struct {
	// ServiceName is the name of a service. Each gcloud.yaml file should
	// correlate to a single service config with one or more APIs defined.
	ServiceName string `yaml:"service_name"`

	// APIs describes the APIs for which to generate a gcloud surface.
	APIs []API `yaml:"apis,omitempty"`

	// ResourcePatterns describes resource patterns not included in
	// descriptors, providing additional patterns that might be used for
	// resource identification or command generation.
	ResourcePatterns []ResourcePattern `yaml:"resource_patterns,omitempty"`
}

// API describes an API to generate a surface for. This structure holds
// configurations specific to a single API within the gcloud surface.
type API struct {
	// Name is the name of the API. This should be the API name as it appears
	// in the normalized service config (e.g., "compute.googleapis.com").
	Name string `yaml:"name"`

	// APIVersion is the API version of the API (e.g., "v1", "v2beta1").
	APIVersion string `yaml:"api_version,omitempty"`

	// SupportsStarUpdateMasks indicates that this API supports '*' updateMasks
	// in accordance with https://google.aip.dev/134#request-message. The
	// default is assumed to be true for AIP compliant APIs.
	SupportsStarUpdateMasks bool `yaml:"supports_star_update_masks,omitempty"`

	// RootIsHidden applies the gcloud 'hidden' flag to the root command group
	// of the generated surface.  When true, the top-level command group for
	// this API will not appear in `--help` output by default.  See
	// go/gcloud-advanced-topics#hiding-commands-and-command-groups for more
	// details.
	RootIsHidden bool `yaml:"root_is_hidden"`

	// ReleaseTracks are the gcloud release tracks this surface should appear
	// in. This determines the visibility and stability level of the generated
	// commands and resources.
	ReleaseTracks []ReleaseTrack `yaml:"release_tracks,omitempty"`

	// HelpText contains all help text configurations for the surfaces
	// including groups, commands, resources, and flags/arguments related to
	// this API.
	HelpText *HelpText `yaml:"help_text,omitempty"`

	// OutputFormatting contains all output formatting rules for commands
	// within this API. These rules dictate how the results of commands are
	// displayed to the user.
	OutputFormatting []*OutputFormatting `yaml:"output_formatting,omitempty"`

	// CommandOperationsConfig contains long running operations config for
	// methods within this API. This allows customization of how asynchronous
	// operations are handled and displayed.
	CommandOperationsConfig []*CommandOperationsConfig `yaml:"command_operations_config,omitempty"`
}

// HelpText contains rules for various types of help text within an API
// surface. It groups help text definitions by the type of CLI element they
// apply to.
type HelpText struct {
	// ServiceRules defines help text rules specifically for services.
	ServiceRules []*HelpTextRule `yaml:"service_rules,omitempty"`

	// MessageRules defines help text rules specifically for messages (resource
	// command groups).
	MessageRules []*HelpTextRule `yaml:"message_rules,omitempty"`

	// MethodRules defines help text rules specifically for API methods
	// (commands).
	MethodRules []*HelpTextRule `yaml:"method_rules,omitempty"`

	// FieldRules defines help text rules specifically for individual fields
	// (flags/arguments).
	FieldRules []*HelpTextRule `yaml:"field_rules,omitempty"`
}

// HelpTextRule maps an API selector to its corresponding HelpTextElement.
// This allows for targeted help text customization based on specific API
// elements.
type HelpTextRule struct {
	// Selector is a comma-separated list of patterns for any element such as a
	// method, a field, an enum value. Each pattern is a qualified name of the
	// element which may end in "*", indicating a wildcard. Wildcards are only
	// allowed at the end and for a whole component of the qualified name, i.e.
	// "foo.*" is ok, but not "foo.b*" or "foo.*.bar".
	//
	// Wildcard may not be applicable for some elements, in those cases an
	// 'InvalidSelectorWildcardError' error will be thrown.  Additionally, some
	// gcloud data elements expect a singular selector, if a comma separated
	// selector string is passed, a 'InvalidSelectorList' error will be thrown.
	//
	// See http://google3/google/api/documentation.proto;l=253;rcl=525006895
	// for API selector details.
	Selector string `yaml:"selector,omitempty"`

	// HelpText contains the detailed help text content for the selected
	// element.
	HelpText *HelpTextElement `yaml:"help_text,omitempty"`
}

// HelpTextElement describes the actual content of the help text for a CLI
// Element. This structure holds the brief, description, and examples for a
// given element. This can be linked to an individual API RPC/Command, a
// Resource Message, Enum, Service, Enum.value or Message.field.
type HelpTextElement struct {
	// Brief is a concise, single-line summary of the help text for the CLI
	// element.
	Brief string `yaml:"brief,omitempty"`

	// Description provides a detailed, multi-line description for the CLI
	// element.
	Description string `yaml:"description,omitempty"`

	// Examples provides a list of string examples illustrating how to use the
	// CLI element.
	Examples []string `yaml:"examples,omitempty"`
}

// OutputFormatting contains a collection of command output formatting rules.
// These rules are used to specify how the output of gcloud commands should be
// presented.
type OutputFormatting struct {
	// Selector is a comma-separated list of patterns for any element such as a
	// method, a field, an enum value. Each pattern is a qualified name of the
	// element which may end in "*", indicating a wildcard. Wildcards are only
	// allowed at the end and for a whole component of the qualified name, i.e.
	// "foo.*" is ok, but not "foo.b*" or "foo.*.bar".
	//
	// Wildcard may not be applicable for some elements, in those cases an
	// 'InvalidSelectorWildcardError' error will be thrown.  Additionally, some
	// gcloud data elements expect a singular selector, if a comma separated
	// selector string is passed, a 'InvalidSelectorList' error will be thrown.
	//
	// See http://google3/google/api/documentation.proto;l=253;rcl=525006895
	// for API selector details.  Must point to a single RPC/command. Wildcards
	// ('*') not allowed for output formatting.
	Selector string `yaml:"selector"`

	// Format is the output formatting string to apply. This string typically
	// follows the `gcloud topic formats` specification (e.g., "table(name,
	// createTime)", "json").
	Format string `yaml:"format"`
}

// CommandOperationsConfig contains a collection of command operations
// configuration rules.  These rules govern the behavior of long-running
// operations triggered by gcloud commands.
type CommandOperationsConfig struct {
	// Selector is a comma-separated list of patterns for any element such as a
	// method, a field, an enum value. Each pattern is a qualified name of the
	// element which may end in "*", indicating a wildcard. Wildcards are only
	// allowed at the end and for a whole component of the qualified name, i.e.
	// "foo.*" is ok, but not "foo.b*" or "foo.*.bar".
	//
	// Wildcard may not be applicable for some elements, in those cases an
	// 'InvalidSelectorWildcardError' error will be thrown.  Additionally, some
	// gcloud data elements expect a singular selector, if a comma separated
	// selector string is passed, a 'InvalidSelectorList' error will be thrown.
	//
	// See http://google3/google/api/documentation.proto;l=253;rcl=525006895
	// for API selector details.
	Selector string `yaml:"selector"`

	// DisplayOperationResult determines whether to display the resource result
	// in the output of the command by default.  Set to `true` to display the
	// operation result instead of the final resource.  See
	// http://go/gcloud-creating-commands#async for more details.
	DisplayOperationResult bool `yaml:"display_operation_result"`
}

// ReleaseTrack is an enumeration of the gcloud release tracks. These indicate
// the stability level and visibility of commands and features.
type ReleaseTrack string

const (
	// ReleaseTrackAlpha represents the ALPHA release track. Features in this
	// track are experimental and subject to change.
	ReleaseTrackAlpha ReleaseTrack = "ALPHA"

	// ReleaseTrackBeta represents the BETA release track. Features in this
	// track are more stable than ALPHA but may still undergo minor changes.
	ReleaseTrackBeta ReleaseTrack = "BETA"

	// ReleaseTrackGA represents the GA (Generally Available) release track.
	// Features in this track are stable and suitable for production use.
	ReleaseTrackGA ReleaseTrack = "GA"
)

// ResourcePattern describes resource patterns not explicitly included in API
// descriptors.  These patterns can be used to define additional resource
// identifiers or custom resource structures.
type ResourcePattern struct {
	// Type is the resource type (e.g., "example.googleapis.com/Service").
	Type string `yaml:"type"`

	// Patterns is a list of resource patterns (e.g.,
	// "projects/{project}/locations/{location}/services/{service}").  These
	// define the structure of resource names.
	Patterns []string `yaml:"patterns,omitempty"`

	// APIVersion is the API version associated with this resource pattern
	// (e.g., "v1").
	APIVersion string `yaml:"api_version,omitempty"`
}
