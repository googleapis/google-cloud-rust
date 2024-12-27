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

package sidekick

// CommandLine Represents the arguments received from the command line.
type CommandLine struct {
	Command             []string
	ProjectRoot         string
	SpecificationFormat string
	SpecificationSource string
	ServiceConfig       string
	Source              map[string]string
	Output              string
	Language            string
	Codec               map[string]string
	DryRun              bool
}

var (
	flagProjectRoot string
	format          string
	source          string
	serviceConfig   string
	sourceOpts      = map[string]string{}
	output          string
	flagLanguage    string
	codecOpts       = map[string]string{}
	dryrun          bool
)
