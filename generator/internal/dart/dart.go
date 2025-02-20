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
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/iancoleman/strcase"
)

//go:embed templates
var dartTemplates embed.FS

type dartImport struct {
	// The Protobuf package this message belongs to.
	Package string
	// The import url to use, i.e., 'package:foo/foo.dart' or 'dart:typed_data'.
	DartImport string
}

var typedDataImport = &dartImport{
	Package:    "typed_data",
	DartImport: "dart:typed_data",
}

var httpImport = &dartImport{
	Package:    "http",
	DartImport: "package:http/http.dart",
}

func Generate(model *api.API, outdir string, options map[string]string) error {
	_, err := annotateModel(model, options)
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

func loadWellKnownTypes(s *api.APIState) {
	// TODO(#1034): Create a WKT for google.protobuf.Timestamp.
	timestamp := &api.Message{
		ID:      ".google.protobuf.Timestamp",
		Name:    "DateTime",
		Package: "google.protobuf",
	}
	s.MessageByID[timestamp.ID] = timestamp

	// TODO(#1034): Create a WKT for google.protobuf.Duration.
	duration := &api.Message{
		ID:      ".google.protobuf.Duration",
		Name:    "Duration",
		Package: "google.protobuf",
	}
	s.MessageByID[duration.ID] = duration
}

func fieldType(f *api.Field, state *api.APIState, importMap map[string]*dartImport) string {
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
		importMap[typedDataImport.Package] = typedDataImport
		out = "Uint8List"
	case api.MESSAGE_TYPE:
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			key := fieldType(m.Fields[0], state, importMap)
			val := fieldType(m.Fields[1], state, importMap)
			out = "Map<" + key + ", " + val + ">"
		} else {
			out = messageName(m)
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

func methodInOutTypeName(id string, s *api.APIState) string {
	if id == "" {
		return ""
	}
	if id == ".google.protobuf.Empty" {
		return "void"
	}
	m, ok := s.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return strcase.ToCamel(m.Name)
}

func messageName(m *api.Message) string {
	if m.Parent != nil {
		return messageName(m.Parent) + "$" + strcase.ToCamel(m.Name)
	}
	return strcase.ToCamel(m.Name)
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

func bodyAccessor(m *api.Method) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + strcase.ToCamel(m.PathInfo.BodyFieldPath)
}

func httpPathFmt(_ *api.PathInfo) string {
	fmt := ""
	// TODO(#1034): Determine the correct format for Dart.
	return fmt
}

func httpPathArgs(_ *api.PathInfo) []string {
	var args []string
	// TODO(#1034): Determine the correct format for Dart.
	return args
}

func formatDocComments(documentation string, _ *api.APIState) []string {
	ss := strings.Split(documentation, "\n")
	for i := range ss {
		ss[i] = strings.TrimRightFunc(ss[i], unicode.IsSpace)
	}
	return ss
}

func modelPackageName(api *api.API, packageNameOverride string) string {
	if len(packageNameOverride) > 0 {
		return packageNameOverride
	}
	return "google_cloud_" + strcase.ToSnake(api.Name)
}

func generateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations, we cannot generate working
	// RPCs for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	return !m.ClientSideStreaming && !m.ServerSideStreaming && m.PathInfo != nil && len(m.PathInfo.PathTemplate) != 0
}
