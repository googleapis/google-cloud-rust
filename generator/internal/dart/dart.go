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

package dart

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/iancoleman/strcase"
)

var typedDataImport = "dart:typed_data"
var httpImport = "package:http/http.dart"
var commonImport = "package:google_cloud_gax/common.dart"
var commonHelpersImport = "package:google_cloud_gax/src/json_helpers.dart"

var needsCtorValidation = map[string]string{
	".google.protobuf.Duration": ".google.protobuf.Duration",
}

var usesCustomEncoding = map[string]string{
	".google.protobuf.Duration":  ".google.protobuf.Duration",
	".google.protobuf.FieldMask": ".google.protobuf.FieldMask",
}

var reservedNames = map[string]string{
	"Function": "",
}

func messageName(m *api.Message) string {
	name := strcase.ToCamel(m.Name)

	if m.Parent == nil {
		// For top-most symbols, check for conflicts with reserved names.
		if _, hasConflict := reservedNames[name]; hasConflict {
			return name + "$"
		} else {
			return name
		}
	} else {
		return messageName(m.Parent) + "$" + name
	}
}

func qualifiedName(m *api.Message) string {
	// Convert '.google.protobuf.Duration' to 'google.protobuf.Duration'.
	return strings.TrimPrefix(m.ID, ".")
}

func enumName(e *api.Enum) string {
	if e.Parent != nil {
		return messageName(e.Parent) + "$" + strcase.ToCamel(e.Name)
	}
	return strcase.ToCamel(e.Name)
}

func enumValueName(e *api.EnumValue) string {
	return strcase.ToLowerCamel(e.Name)
}

func httpPathFmt(pathInfo *api.PathInfo) string {
	var builder strings.Builder

	for _, segment := range pathInfo.PathTemplate {
		switch {
		case segment.Literal != nil:
			builder.WriteString("/")
			builder.WriteString(*segment.Literal)
		case segment.FieldPath != nil:
			fieldPath := *segment.FieldPath
			paths := strings.Split(fieldPath, ".")
			for i, p := range paths {
				paths[i] = strcase.ToLowerCamel(p)
			}
			fieldPath = strings.Join(paths, ".")
			builder.WriteString("/${request.")
			builder.WriteString(fieldPath)
			builder.WriteString("}")
		case segment.Verb != nil:
			builder.WriteString(":")
			builder.WriteString(*segment.Verb)
		}
	}

	return builder.String()
}

func httpPathArgs(_ *api.PathInfo) []string {
	var args []string
	// TODO(#1577): Determine the correct format for Dart.
	return args
}

func formatDocComments(documentation string, _ *api.APIState) []string {
	lines := strings.Split(documentation, "\n")

	for i, line := range lines {
		lines[i] = strings.TrimRightFunc(line, unicode.IsSpace)
	}

	for len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}

	for i, line := range lines {
		if len(line) == 0 {
			lines[i] = "///"
		} else {
			lines[i] = "/// " + line
		}
	}

	return lines
}

func packageName(api *api.API, packageNameOverride string) string {
	if len(packageNameOverride) > 0 {
		return packageNameOverride
	}
	return "google_cloud_" + strcase.ToSnake(api.Name)
}

func shouldGenerateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations; we cannot generate working RPCs
	// for them.
	// TODO(#499) Switch to explicitly excluding such functions.
	return !m.ClientSideStreaming && !m.ServerSideStreaming && m.PathInfo != nil && len(m.PathInfo.PathTemplate) != 0
}

func formatDirectory(dir string) error {
	if err := runExternalCommand("dart", "format", dir); err != nil {
		return fmt.Errorf("got an error trying to run `dart format`; perhaps try https://dart.dev/get-dart (%w)", err)
	}
	return nil
}

func runExternalCommand(c string, arg ...string) error {
	cmd := exec.Command(c, arg...)
	cmd.Dir = "."
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return fmt.Errorf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		return fmt.Errorf("%v: %v\n%s", cmd, err, output)
	}
	return nil
}
