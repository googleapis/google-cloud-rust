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
	"regexp"
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

	// We store the state in a stack. This allows for backtracking as we
	// walk the AST.
	//
	// We push states as we enter nodes, and pop states as we exit them.
	states := []State{{}}
	// Additionally, we have a global flag for when we should print the
	// marker for a list item.
	print_marker := false
	// And a flag for when we need an extra line break between blocks.
	print_previous_blank := false

	// Write a new line, given the current state.
	add_line := func(l string) {
		state := states[len(states)-1]
		if print_marker {
			spaces := strings.Repeat(" ", state.Indent-len(state.Marker))
			results = append(results, fmt.Sprintf("%s%s%s", spaces, state.Marker, l))
		} else {
			spaces := strings.Repeat(" ", state.Indent)
			results = append(results, fmt.Sprintf("%s%s", spaces, l))
		}
		// Avoid printing extra markers in the case of multi-line or
		// multi-paragraph list items.
		print_marker = false
		// We wrote something. Accept an extra line break between blocks.
		print_previous_blank = true
	}

	err := ast.Walk(doc, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		// A copy of the current state
		state := states[len(states)-1]

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
		case ast.KindList:
			if entering {
				list := node.(*ast.List)
				marker := string(list.Marker)
				if list.IsOrdered() {
					marker = "1."
				}
				state.Marker = marker + " "
				// It is simpler to set the indent now. Note that we never
				// count past 1, so the marker is not increasing in length.
				state.Indent += len(state.Marker)
				states = append(states, state)
			} else {
				states = states[:len(states)-1]
			}
		case ast.KindListItem:
			// Restore the marker, which might have been cleared if the
			// item has multi-line text blocks.
			print_marker = true
		default:
			if entering {
				fmt.Printf("\n\nKind: %d", node.Kind())
				node.Dump(documentationBytes, 2)
				return ast.WalkStop, fmt.Errorf("encountered unknown NodeKind: %s", node.Kind().String())
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

	// Append reference links. These are skipped by the AST.
	results = append(results, referenceLinks(contents)...)
	return strings.Join(results, "\n"), nil
}

var referenceLinkMatcher = regexp.MustCompile(`^\[([^\]]+)\]:\s*(.*)$`)

func referenceLinks(contents string) []string {
	var results []string
	lines := strings.Split(contents, "\n")
	for _, line := range lines {
		if len(referenceLinkMatcher.FindStringSubmatch(line)) > 0 {
			results = append(results, line)
		}
	}
	return results
}
