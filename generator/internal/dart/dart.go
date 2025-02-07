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
	"fmt"
	"log/slog"
	"strings"
	"unicode"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/language"
	"github.com/iancoleman/strcase"
)

//go:embed templates
var dartTemplates embed.FS

func Generate(model *api.API, outdir string, options map[string]string) error {
	_, err := annotateModel(model, options)
	if err != nil {
		return err
	}
	provider := templatesProvider()
	return language.GenerateFromRoot(outdir, model, provider, generatedFiles())
}

func generatedFiles() []language.GeneratedFile {
	return language.WalkTemplatesDir(dartTemplates, "templates")
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

func fieldType(f *api.Field, state *api.APIState) string {
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
		out = "Uint8List"
	case api.MESSAGE_TYPE:
		// TODO(#1034): Handle MESSAGE_TYPE conversion.
		m, ok := state.MessageByID[f.TypezID]
		if !ok {
			slog.Error("unable to lookup type", "id", f.TypezID)
			return ""
		}
		if m.IsMap {
			out = "Map"
			break
		}
		out = "*" + messageName(m)
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
	m, ok := s.MessageByID[id]
	if !ok {
		slog.Error("unable to lookup type", "id", id)
		return ""
	}
	return strcase.ToCamel(m.Name)
}

func messageName(m *api.Message) string {
	if m.Parent != nil {
		return messageName(m.Parent) + "_" + strcase.ToCamel(m.Name)
	}
	return toPascal(m.Name)
}

func enumName(e *api.Enum) string {
	if e.Parent != nil {
		return messageName(e.Parent) + "_" + strcase.ToCamel(e.Name)
	}
	return strcase.ToCamel(e.Name)
}

func enumValueName(e *api.EnumValue) string {
	var name string
	if strings.ToUpper(e.Name) == e.Name {
		name = e.Name
	} else {
		name = strcase.ToScreamingSnake(e.Name)
	}
	if e.Parent.Parent != nil {
		return messageName(e.Parent.Parent) + "_" + name
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
	return "google_cloud_" + strcase.ToSnake(api.Name)
}

func validatePackageName(newPackage, elementName, sourceSpecificationPackageName string) error {
	if sourceSpecificationPackageName == newPackage {
		return nil
	}
	// Special exceptions for mixin services
	if newPackage == "google.cloud.location" ||
		newPackage == "google.iam.v1" ||
		newPackage == "google.longrunning" {
		return nil
	}
	if sourceSpecificationPackageName == newPackage {
		return nil
	}
	return fmt.Errorf("dart codec requires all top-level elements to be in the same package want=%s, got=%s for %s",
		sourceSpecificationPackageName, newPackage, elementName)
}

func validateModel(api *api.API, sourceSpecificationPackageName string) error {
	// Set the source package. We should always take the first service registered
	// as the source package. Services with mixes will register those after the
	// source package.
	if len(api.Services) > 0 {
		sourceSpecificationPackageName = api.Services[0].Package
	} else if len(api.Messages) > 0 {
		sourceSpecificationPackageName = api.Messages[0].Package
	}
	for _, s := range api.Services {
		if err := validatePackageName(s.Package, s.ID, sourceSpecificationPackageName); err != nil {
			return err
		}
	}
	for _, s := range api.Messages {
		if err := validatePackageName(s.Package, s.ID, sourceSpecificationPackageName); err != nil {
			return err
		}
	}
	for _, s := range api.Enums {
		if err := validatePackageName(s.Package, s.ID, sourceSpecificationPackageName); err != nil {
			return err
		}
	}
	return nil
}

func generateMethod(m *api.Method) bool {
	// Ignore methods without HTTP annotations, we cannot generate working
	// RPCs for them.
	// TODO(#499) - switch to explicitly excluding such functions. Easier to
	//     find them and fix them that way.
	return !m.ClientSideStreaming && !m.ServerSideStreaming && m.PathInfo != nil && len(m.PathInfo.PathTemplate) != 0
}

// The list of Dart keywords and reserved words can be found at
// https://dart.dev/language/keywords.
func escapeKeyword(symbol string) string {
	// TODO(#1034): Populate these once we need this function.
	keywords := map[string]bool{
		//
	}
	_, ok := keywords[symbol]
	if !ok {
		return symbol
	}
	return symbol + "_"
}
