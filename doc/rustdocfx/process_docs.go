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

package main

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

type State struct {
	Indent int
	Marker string
}

func processDocString(contents string) (string, error) {
	var results []string
	md := goldmark.New(
		goldmark.WithParserOptions(
			parser.WithAutoHeadingID(),
		),
		goldmark.WithExtensions(),
	)
	documentationBytes := []byte(contents)
	doc := md.Parser().Parse(text.NewReader(documentationBytes))

	// A flag for when we need an extra line break between blocks.
	print_previous_blank := false

	// Write a new line, given the current state.
	add_line := func(l string) {
		results = append(results, l)
		// We wrote something. Accept an extra line break between blocks.
		print_previous_blank = true
	}

	err := ast.Walk(doc, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		// First handle blank lines between blocks
		switch node.Kind() {
		case ast.KindCodeBlock,
			ast.KindFencedCodeBlock,
			ast.KindHeading,
			ast.KindList,
			ast.KindListItem,
			ast.KindParagraph,
			ast.KindTextBlock:
			if entering && node.HasBlankPreviousLines() && print_previous_blank {
				results = append(results, "")
				// Disallow consecutive empty lines.
				print_previous_blank = false
			}
		}

		switch node.Kind() {
		case ast.KindDocument:
			// The root block. There is nothing to render.
		case ast.KindTextBlock,
			ast.KindParagraph:
			// We will dump the contents from these blocks, skipping
			// any children. This saves us from having to parse all
			// inline blocks, e.g. an **emphasis** block.
			if entering {
				for i := 0; i < node.Lines().Len(); i++ {
					line := node.Lines().At(i)
					line_str := string(line.Value(documentationBytes))
                                        add_line(line_str)
				}
			}
			return ast.WalkSkipChildren, nil
		default:
			if entering {
				fmt.Printf("\n\nKind: %d", node.Kind())
				node.Dump(documentationBytes, 2)
				return ast.WalkStop, fmt.Errorf("Encountered unknown NodeKind: %s", node.Kind().String())
			}
		}
		return ast.WalkContinue, nil
	})
	if err != nil {
		return "", err
	}

	for i, line := range results {
		// Many lines end in a newline, but we are handling new lines
		// manually. So we trim any extra space on the right.
		results[i] = strings.TrimRightFunc(line, unicode.IsSpace)
	}
	return strings.Join(results, "\n"), nil
}
