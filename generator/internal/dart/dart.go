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
	"embed"
	"log/slog"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/iancoleman/strcase"
)

//go:embed templates
var dartTemplates embed.FS

var typedDataImport = "dart:typed_data"
var httpImport = "package:http/http.dart"
var commonImport = "package:google_cloud_common/common.dart"
var commonHelpersImport = "package:google_cloud_common/src/json_helpers.dart"

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

func Generate(model *api.API, outdir string, cfg *config.Config) error {
	_, err := annotateModel(model, cfg.Codec)
	if err != nil {
		return err
	}
	provider := templatesProvider()
	// TODO(#1034): Walk the generated files; dartfmt Dart ones.
	return language.GenerateFromRoot(outdir, model, provider, generatedFiles(model))
}

func generatedFiles(model *api.API) []language.GeneratedFile {
	codec := model.Codec.(*modelAnnotations)
	mainFileName := codec.MainFileName

	files := language.WalkTemplatesDir(dartTemplates, "templates")

	// Look for and replace 'main.dart' with '{servicename}.dart'
	for index, fileInfo := range files {
		if filepath.Base(fileInfo.TemplatePath) == "main.dart.mustache" {
			outDir := filepath.Dir(fileInfo.OutputPath)
			fileInfo.OutputPath = filepath.Join(outDir, mainFileName+".dart")

			files[index] = fileInfo
		}
	}

	return files
}

func fieldType(f *api.Field, state *api.APIState, packageMapping map[string]string, imports map[string]string) string {
	var out string

	switch f.Typez {
	case api.BOOL_TYPE:
		out = "bool"
	case api.INT32_TYPE:
		out = "int"
	case api.INT64_TYPE:
		out = "int"
	case api.UINT32_TYPE:
		out = "int"
	case api.UINT64_TYPE:
		out = "int"
	case api.FLOAT_TYPE:
		out = "double"
	case api.DOUBLE_TYPE:
		out = "double"
	case api.STRING_TYPE:
		out = "String"
	case api.BYTES_TYPE:
		// TODO(#1034): We should instead reference a custom type (ProtoBuffer or
		// similar), encode/decode to it, and add Uint8List related utility methods.
		imports["typed_data"] = typedDataImport
		out = "Uint8List"
	case api.MESSAGE_TYPE:
		message, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if message.IsMap {
			key := fieldType(message.Fields[0], state, packageMapping, imports)
			val := fieldType(message.Fields[1], state, packageMapping, imports)
			out = "Map<" + key + ", " + val + ">"
		} else {
			out = resolveTypeName(message, packageMapping, imports)
		}
	case api.ENUM_TYPE:
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		out = enumName(e)
	default:
		slog.Error("unhandled fieldType", "type", f.Typez, "id", f.TypezID)
	}

	if f.Repeated {
		out = "List<" + out + ">"
	}

	return out
}

func templatesProvider() language.TemplateProvider {
	return func(name string) (string, error) {
		contents, err := dartTemplates.ReadFile(name)
		if err != nil {
			return "", err
		}
		return string(contents), nil
	}
}

func resolveTypeName(message *api.Message, packageMapping map[string]string, imports map[string]string) string {
	if message == nil {
		slog.Error("unable to lookup type")
		return ""
	}

	if message.ID == ".google.protobuf.Empty" {
		return "void"
	}

	// Use the packageMapping info to add any necessary import.
	dartImport, ok := packageMapping[message.Package]
	if ok {
		imports[message.Package] = dartImport
	}

	return messageName(message)
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
	// TODO(#1034): Determine the correct format for Dart.
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

func modelPackageName(api *api.API, packageNameOverride string) string {
	if len(packageNameOverride) > 0 {
		return packageNameOverride
	}
	return "google_cloud_" + strcase.ToSnake(api.Name)
}

func generateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations; we cannot generate working RPCs
	// for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	return !m.ClientSideStreaming && !m.ServerSideStreaming && m.PathInfo != nil && len(m.PathInfo.PathTemplate) != 0
}
