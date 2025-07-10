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

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func resetArgs() {
	flagProjectRoot, format, source, serviceConfig, output, flagLanguage = "", "", "", "", "", ""
	sourceOpts, codecOpts = map[string]string{}, map[string]string{}
	dryrun = false
}

func TestParseArgs(t *testing.T) {
	t.Cleanup(resetArgs)
	args := []string{
		"generate",
		"-project-root", "../..",
		"-specification-format", "openapi",
		"-specification-source", specificationSource,
		"-service-config", secretManagerServiceConfig,
		"-source-option", fmt.Sprintf("googleapis-root=%s", googleapisRoot),
		"-language", "not-rust",
		"-output", outputDir,
		"-codec-option", "copyright-year=2024",
		"-codec-option", "package-name-override=secretmanager-golden-openapi",
		"-codec-option", "package:wkt=package=google-cloud-wkt,source=google.protobuf",
		"-codec-option", "package:gax=package=gcp-sdk-gax,feature=unstable-sdk-client",
	}
	cmd, _, args := cmdSidekick.lookup(args)
	if cmd.name() != "generate" {
		t.Fatal("expected lookup to return 'generate' command")
	}

	got, err := cmd.parseCmdLine(args)
	if err != nil {
		t.Fatal(err)
	}
	want := &CommandLine{
		Command:             args,
		ProjectRoot:         "../..",
		SpecificationFormat: "openapi",
		SpecificationSource: specificationSource,
		ServiceConfig:       secretManagerServiceConfig,
		Source: map[string]string{
			"googleapis-root": googleapisRoot,
		},
		Language: "not-rust",
		Output:   outputDir,
		Codec: map[string]string{
			"copyright-year":        "2024",
			"package-name-override": "secretmanager-golden-openapi",
			"package:wkt":           "package=google-cloud-wkt,source=google.protobuf",
			"package:gax":           "package=gcp-sdk-gax,feature=unstable-sdk-client",
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}

func TestDefaults(t *testing.T) {
	t.Cleanup(resetArgs)
	root := t.TempDir()
	args := []string{
		"-project-root", root,
	}
	got, err := cmdSidekick.parseCmdLine(args)
	if err != nil {
		t.Fatal(err)
	}
	want := &CommandLine{
		Command:     args,
		ProjectRoot: root,
		Source:      map[string]string{},
		Codec:       map[string]string{},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatched merged config (-want, +got):\n%s", diff)
	}
}
