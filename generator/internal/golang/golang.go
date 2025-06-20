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

package golang

import (
	"embed"
	"fmt"
	"log/slog"
	"strings"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/iancoleman/strcase"
)

//go:embed templates
var goTemplates embed.FS

type goImport struct {
	path string
	name string
}

func Generate(model *api.API, outdir string, cfg *config.Config) error {
	_, err := annotateModel(model, cfg.Codec)
	if err != nil {
		return err
	}
	provider := templatesProvider()
	return language.GenerateFromModel(outdir, model, provider, generatedFiles())
}

func generatedFiles() []language.GeneratedFile {
	return language.WalkTemplatesDir(goTemplates, "templates")
}

func loadWellKnownTypes(s *api.APIState) {
	timestamp := &api.Message{
		ID:      ".google.protobuf.Timestamp",
		Name:    "Time",
		Package: "time",
	}
	duration := &api.Message{
		ID:      ".google.protobuf.Duration",
		Name:    "Duration",
		Package: "time",
	}
	s.MessageByID[timestamp.ID] = timestamp
	s.MessageByID[duration.ID] = duration
}

func fieldType(f *api.Field, state *api.APIState, importMap map[string]*goImport) string {
	var out string
	switch f.Typez {
	case api.STRING_TYPE:
		out = "string"
	case api.INT64_TYPE:
		out = "int64"
	case api.INT32_TYPE:
		out = "int32"
	case api.BOOL_TYPE:
		out = "bool"
	case api.BYTES_TYPE:
		out = "[]byte"
	case api.MESSAGE_TYPE:
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			key := fieldType(m.Fields[0], state, importMap)
			val := fieldType(m.Fields[1], state, importMap)
			out = "map[" + key + "]" + val
			break
		}
		out = "*" + messageName(m, importMap)
	case api.ENUM_TYPE:
		e, ok := state.EnumByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		out = enumName(e, importMap)
	default:
		slog.Error("unhandled fieldType", "type", f.Typez, "id", f.TypezID)
	}
	return out
}

func asQueryParameter(f *api.Field) string {
	return fmt.Sprintf("req.%s.to_str()", strcase.ToLowerCamel(f.Name))
}

func templatesProvider() language.TemplateProvider {
	return func(name string) (string, error) {
		contents, err := goTemplates.ReadFile(name)
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
	m, ok := s.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return strcase.ToCamel(m.Name)
}

func messageName(m *api.Message, importMap map[string]*goImport) string {
	if m.Parent != nil {
		return messageName(m.Parent, importMap) + "_" + strcase.ToCamel(m.Name)
	}
	if imp, ok := importMap[m.Package]; ok {
		return imp.name + "." + toPascal(m.Name)
	}
	return toPascal(m.Name)
}

func enumName(e *api.Enum, importMap map[string]*goImport) string {
	if e.Parent != nil {
		return messageName(e.Parent, importMap) + "_" + strcase.ToCamel(e.Name)
	}
	return strcase.ToCamel(e.Name)
}

func enumValueName(e *api.EnumValue, importMap map[string]*goImport) string {
	var name string
	if strings.ToUpper(e.Name) == e.Name {
		name = e.Name
	} else {
		name = strcase.ToScreamingSnake(e.Name)
	}
	if e.Parent.Parent != nil {
		return messageName(e.Parent.Parent, importMap) + "_" + name
	}
	return name
}

func bodyAccessor(m *api.Method) string {
	if m.PathInfo.BodyFieldPath == "*" {
		// no accessor needed, use the whole request
		return ""
	}
	return "." + strcase.ToCamel(m.PathInfo.BodyFieldPath)
}

func httpPathFmt(m *api.PathInfo) string {
	fmt := ""
	for _, segment := range m.Bindings[0].LegacyPathTemplate {
		if segment.Literal != nil {
			fmt = fmt + "/" + *segment.Literal
		} else if segment.FieldPath != nil {
			fmt = fmt + "/%s"
		} else if segment.Verb != nil {
			fmt = fmt + ":" + *segment.Verb
		}
	}
	return fmt
}

func httpPathArgs(h *api.PathInfo) []string {
	var args []string
	// TODO(codyoss): https://github.com/googleapis/google-cloud-rust/issues/34
	for _, segment := range h.Bindings[0].LegacyPathTemplate {
		if segment.FieldPath != nil {
			// TODO(#34) - handle nested path params
			args = append(args, fmt.Sprintf(", req.%s", strcase.ToCamel(*segment.FieldPath)))
		}
	}
	return args
}

func toPascal(symbol string) string {
	return escapeKeyword(strcase.ToCamel(symbol))
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
	return api.Name
}

func imports(importMap map[string]*goImport) []string {
	var imports []string
	for _, imp := range importMap {
		imports = append(imports, fmt.Sprintf("%q", imp.path))
	}
	return imports
}

func generateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations, we cannot generate working
	// RPCs for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	if m.ClientSideStreaming || m.ServerSideStreaming || m.PathInfo == nil {
		return false
	}
	if len(m.PathInfo.Bindings) == 0 {
		return false
	}
	return len(m.PathInfo.Bindings[0].LegacyPathTemplate) != 0
}

// The list of Golang keywords and reserved words can be found at:
//
// https://go.dev/ref/spec#Keywords
func escapeKeyword(symbol string) string {
	keywords := map[string]bool{
		"break":       true,
		"default":     true,
		"func":        true,
		"interface":   true,
		"select":      true,
		"case":        true,
		"defer":       true,
		"go":          true,
		"map":         true,
		"struct":      true,
		"chan":        true,
		"else":        true,
		"goto":        true,
		"package":     true,
		"switch":      true,
		"const":       true,
		"fallthrough": true,
		"if":          true,
		"range":       true,
		"type":        true,
		"continue":    true,
		"for":         true,
		"import":      true,
		"return":      true,
		"var":         true,
	}
	_, ok := keywords[symbol]
	if !ok {
		return symbol
	}
	return symbol + "_"
}
