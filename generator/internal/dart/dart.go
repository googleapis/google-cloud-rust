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
	"regexp"
	"strings"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/iancoleman/strcase"
)

var typedDataImport = "dart:typed_data"
var httpImport = "package:http/http.dart as http"
var commonImport = "package:google_cloud_gax/gax.dart"
var commonHelpersImport = "package:google_cloud_gax/src/encoding.dart"

var needsCtorValidation = map[string]string{
	".google.protobuf.Duration":  "",
	".google.protobuf.Timestamp": "",
}

// This list needs to be kept in sync with
// generator/dart/generated/google_cloud_protobuf/lib/src/protobuf.p.dart.
var usesCustomEncoding = map[string]string{
	".google.protobuf.BoolValue":   "",
	".google.protobuf.BytesValue":  "",
	".google.protobuf.DoubleValue": "",
	".google.protobuf.Duration":    "",
	".google.protobuf.FieldMask":   "",
	".google.protobuf.FloatValue":  "",
	".google.protobuf.Int32Value":  "",
	".google.protobuf.Int64Value":  "",
	".google.protobuf.ListValue":   "",
	".google.protobuf.StringValue": "",
	".google.protobuf.Struct":      "",
	".google.protobuf.Timestamp":   "",
	".google.protobuf.UInt32Value": "",
	".google.protobuf.UInt64Value": "",
	".google.protobuf.Value":       "",
}

// Used to concatenate a message and a child message.
var nestedMessageChar = "_"

// Used to concatenate a message and a child enum.
var nestedEnumChar = "_"

// Appended to a name to avoid conflicting with a Dart identifier.
var deconflictChar = "$"

// Dart reserved words.
//
// This blocklist includes words that can never be used as an identifier as well
// as a few that could be used depending on context. We can add additional
// context keywords as we discover conflicts.
//
// See also https://dart.dev/language/keywords.
var reservedNames = map[string]string{
	"assert":   "",
	"await":    "",
	"break":    "",
	"case":     "",
	"catch":    "",
	"class":    "",
	"const":    "",
	"continue": "",
	"default":  "",
	"do":       "",
	"else":     "",
	"enum":     "",
	"extends":  "",
	"false":    "",
	"final":    "",
	"finally":  "",
	"for":      "",
	"Function": "",
	"if":       "",
	"in":       "",
	"is":       "",
	"new":      "",
	"null":     "",
	"rethrow":  "",
	"return":   "",
	"super":    "",
	"switch":   "",
	"this":     "",
	"throw":    "",
	"true":     "",
	"try":      "",
	"var":      "",
	"void":     "",
	"while":    "",
	"with":     "",
	"yield":    "",
}

func messageName(m *api.Message) string {
	name := strcase.ToCamel(m.Name)

	if m.Parent == nil {
		// For top-most symbols, check for conflicts with reserved names.
		if _, hasConflict := reservedNames[name]; hasConflict {
			return name + deconflictChar
		} else {
			return name
		}
	} else {
		return messageName(m.Parent) + nestedMessageChar + name
	}
}

func qualifiedName(m *api.Message) string {
	// Convert '.google.protobuf.Duration' to 'google.protobuf.Duration'.
	return strings.TrimPrefix(m.ID, ".")
}

func fieldName(field *api.Field) string {
	name := strcase.ToLowerCamel(field.Name)
	if _, hasConflict := reservedNames[name]; hasConflict {
		name = name + deconflictChar
	}
	return name
}

func enumName(e *api.Enum) string {
	name := strcase.ToCamel(e.Name)
	if e.Parent != nil {
		name = messageName(e.Parent) + nestedEnumChar + name
	}
	return name
}

func enumValueName(e *api.EnumValue) string {
	name := strcase.ToLowerCamel(e.Name)
	if _, hasConflict := reservedNames[name]; hasConflict {
		name = name + deconflictChar
	}
	return name
}

func httpPathFmt(pathInfo *api.PathInfo) string {
	var builder strings.Builder
	for _, segment := range pathInfo.Bindings[0].LegacyPathTemplate {
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

// This regex matches Google API documentation reference links; it supports
// both regular references as well as implit references.
//
// - `[Code][google.rpc.Code]`
// - `[google.rpc.Code][]`
var commentRefsRegex = regexp.MustCompile(`\[([\w\d\._]+)\]\[([\d\w\._]*)\]`)

func formatDocComments(documentation string, _ *api.APIState) []string {
	lines := strings.Split(documentation, "\n")

	// Remove trailing whitespace.
	for i, line := range lines {
		lines[i] = strings.TrimRightFunc(line, unicode.IsSpace)
	}

	// Re-write Google API doc references to code formatted text.
	// TODO(#1575): Instead, resolve and insert dartdoc style references.
	for i, line := range lines {
		lines[i] = commentRefsRegex.ReplaceAllString(line, "`$1`")
	}

	// Remove trailing blank lines.
	for len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}

	// Convert to dartdoc format.
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

	// Convert 'google.protobuf' to 'google_cloud_protobuf' and
	// 'google.cloud.language.v2' to 'google_cloud_language_v2.
	packageName := api.PackageName
	packageName = strings.TrimPrefix(packageName, "google.cloud.")
	packageName = strings.TrimPrefix(packageName, "google.")
	return "google_cloud_" + strings.ReplaceAll(packageName, ".", "_")
}

func shouldGenerateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations; we cannot generate working RPCs
	// for them.
	// TODO(#499) Switch to explicitly excluding such functions.
	if m.ClientSideStreaming || m.ServerSideStreaming || m.PathInfo == nil {
		return false
	}
	if len(m.PathInfo.Bindings) == 0 {
		return false
	}
	return len(m.PathInfo.Bindings[0].LegacyPathTemplate) != 0
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
