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

package parser

import (
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser/openapi"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser/protobuf"
)

type newParser func() genclient.Parser

func knownParsers() map[string]newParser {
	return map[string]newParser{
		"openapi":  func() genclient.Parser { return openapi.NewParser() },
		"protobuf": func() genclient.Parser { return protobuf.NewParser() },
	}
}

func NewParser(parserID string) (genclient.Parser, error) {
	create, ok := knownParsers()[parserID]
	if !ok {
		return nil, fmt.Errorf("unknown parser %q", parserID)
	}
	return create(), nil
}

func Help() string {
	var help []string
	for name, fact := range knownParsers() {
		parser := fact()
		help = append(help, fmt.Sprintf("%s: %s", name, parser.Help()))
		options := parser.OptionDescriptions()
		if len(options) > 0 {
			help = append(help, "  Options")
		}
		for opt, description := range parser.OptionDescriptions() {
			help = append(help, fmt.Sprintf("    %s: %s", opt, description))
		}
	}
	return strings.Join(help, "\n")
}
