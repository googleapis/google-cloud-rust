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

package httprule

import (
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

// The following documentation was copied and adapted from the [C++ HTTP Annotation parser]
//
// This parser interprets the PathTemplate syntax, defined at the [google.api.http annotation].
//
// A `google.api.http` annotation describes how to convert gRPC RPCs to HTTP
// URLs. The description uses a "path template", showing what portions of the
// URL path are replaced with values from the gRPC request message.
//
// These path templates follow a specific grammar. The grammar is defined by:
//
//	Template = "/" Segments [ Verb ] ;
//	Segments = Segment { "/" Segment } ;
//	Segment  = "*" | "**" | LITERAL | Variable ;
//	Variable = "{" FieldPath [ "=" Segments ] "}" ;
//	FieldPath = IDENT { "." IDENT } ;
//	Verb     = ":" LITERAL ;
//
// The specific notation is not defined, but it seems inspired by
// [Backus-Naur Form]. In this notation, `{ ... }` allows repetition.
//
// The documentation goes on to say:
//
//	A variable template must not contain other variables.
//
// So the grammar is better defined by:
//
//	Template = "/" Segments [ Verb ] ;
//	Segments = Segment { "/" Segment } ;
//	Segment  = Variable | PlainSegment;
//	PlainSegment  = "*" | "**" | LITERAL ;
//	Variable = "{" FieldPath [ "=" PlainSegments ] "}" ;
//	PlainSegments = PlainSegment { "/" PlainSegment };
//	FieldPath = IDENT { "." IDENT } ;
//	Verb     = ":" LITERAL ;
//
// Neither "IDENT" nor "LITERAL" are defined. From context we can infer that
// IDENT must be a valid proto3 identifier, so matching the regular expression
// `[A-Za-z][A-Za-z0-9_]*`. Likewise, we can infer that LITERAL must be a path
// segment in a URL. [RFC 3986] provides a definition for these, which we
// summarize as:
//
// Segment     = pchar { pchar }
// pchar       = unreserved | pct-encoded | sub-delims | ":" | "@"
// unreserved  = ALPHA | DIGIT | "-" | "." | "_" | "~"
// pct-encoded = "%" HEXDIG HEXDIG
// sub-delims  = "!" | "$" | "&" | "'" | "(" | ")" | "*" | "+" | "," | ";" | "="
//
// ALPHA       = [A-Za-z]
// DIGIT       = [0-9]
// HEXDIG      = [0-9A-Fa-f]
//
// Because pchar includes special characters like ':' and '*', which are part of the
// HTTP Rule spec, we define LITERAL as the following subset of pchar:
//
// LITERAL     = unreserved | pct-encoded { unreserved | pct-encoded }
//
// [RFC 3986]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.3
// [Backus-Naur Form]: https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form
// [C++ HTTP Annotation parser]: https://github.com/googleapis/google-cloud-cpp/blob/4174d656136f4b849c8a3d327237f3a96be3e003/generator/internal/http_annotation_parser.h#L49-L58
// [google.api.http annotation]: https://github.com/googleapis/google-cloud-rust/blob/61b9d3bbac5530e4321ac19fe7d2760db82e31db/generator/testdata/googleapis/google/api/http.proto
func ParseSegments(pathTemplate string) (*api.PathTemplate, error) {
	return parsePathTemplate(pathTemplate)
}

type Literal string
type Identifier string

const (
	eof      = -1
	slash    = '/'
	star     = '*'
	varLeft  = '{'
	varRight = '}'
	varSep   = '='
	identSep = '.'
	verbSep  = ':'
)

func parsePathTemplate(pathTemplate string) (*api.PathTemplate, error) {
	var pos int
	var segments []api.PathSegment
	var verb *string
	if len(pathTemplate) < 2 {
		return nil, fmt.Errorf("invalid path template, expected at least two characters: %s", pathTemplate)
	} else if pathTemplate[0] != slash {
		return nil, fmt.Errorf("invalid path template, expected it to start with '/': %s", pathTemplate)
	}
	pos++ // Skip slash
	segments, width, err := parseSegments(pathTemplate[pos:])
	if err != nil {
		return nil, err
	}
	pos += width
	verb, width, err = parseVerb(pathTemplate[pos:])
	if err != nil {
		return nil, err
	}
	pos += width
	if pos != len(pathTemplate) {
		return nil, fmt.Errorf("invalid path template, expected it to end at position %d: %s", pos, pathTemplate)
	}
	return &api.PathTemplate{
		Segments: segments,
		Verb:     verb,
	}, nil

}

func parseVerb(verbString string) (*string, int, error) {
	if len(verbString) == 0 {
		return nil, 0, nil
	}
	var pos int
	if verbString[pos] != verbSep {
		return nil, 0, fmt.Errorf("invalid verb, must start with '%q': %s", verbSep, verbString)
	}
	pos++ // Skip verbSep
	verb, width, err := parseLiteral(verbString[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += width
	return (*string)(verb), pos, nil
}

// parseSegments parses a sequence of variable and/or plain segments starting at the beginning of the provided string.
func parseSegments(segmentsString string) ([]api.PathSegment, int, error) {
	var segments []api.PathSegment
	var pos int
	for {
		var err error
		var segment *api.PathSegment
		var width int

		if pos == len(segmentsString) {
			return nil, pos, fmt.Errorf("expected a segment, found eof: %s", segmentsString)
		}
		if segmentsString[pos] == varLeft {
			segment, width, err = parseVarSegment(segmentsString[pos:])
		} else {
			segment, width, err = parseLiteralSegment(segmentsString[pos:])
		}
		if err != nil {
			return nil, pos, err
		}
		segments = append(segments, *segment)
		pos += width
		if pos == len(segmentsString) || segmentsString[pos] != slash {
			break
		}
		pos++ // Skip slash
	}
	return segments, pos, nil
}

func parseLiteralSegment(literalSegment string) (*api.PathSegment, int, error) {
	literal, width, err := parseLiteral(literalSegment)
	if err != nil {
		return nil, 0, err
	}
	return &api.PathSegment{
		Literal: (*string)(literal),
	}, width, nil
}

func parseVarSegment(varString string) (*api.PathSegment, int, error) {
	if len(varString) < 3 {
		return nil, 0, fmt.Errorf("invalid variable, expected at least three characters: %s", varString)
	}
	var pos int
	if varString[pos] != varLeft {
		return nil, 0, fmt.Errorf("invalid variable, expected it to start with '%q': %s", varLeft, varString)
	}
	pos++ // Skip varLeft
	var width int
	var segments []string
	fieldPath, width, err := parseFieldPath(varString[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += width
	if pos < len(varString) && varString[pos] == varSep {
		pos++ // Skip varSep
		segments, width, err = parseVarSubsegments(varString[pos:])
		if err != nil {
			return nil, 0, err
		}
		pos += width
	}
	if pos == len(varString) || varString[pos] != varRight {
		return nil, 0, fmt.Errorf("invalid variable, expected it to end with '%q': %s", varRight, varString)
	}
	pos++ // Skip varRight
	if len(segments) == 0 {
		// When there are no segments, the single "*" matcher is implied.
		segments = append(segments, api.SingleSegmentWildcard)
	}
	return &api.PathSegment{
		Variable: &api.PathVariable{
			FieldPath: fieldPath,
			Segments:  segments,
		},
	}, pos, nil
}

func parseVarSubsegments(segmentsString string) ([]string, int, error) {
	var pos int
	var segments []string

	for {
		segment, width, err := parseVarSubsegment(segmentsString[pos:])
		if err != nil {
			return nil, pos, err
		}
		segments = append(segments, *segment)
		pos += width
		if pos == len(segmentsString) || segmentsString[pos] != slash {
			break
		}
		pos++ // Skip slash
	}
	return segments, pos, nil
}

func parseFieldPath(fieldPathString string) ([]string, int, error) {
	var pos int
	var identifiers []string
	for {
		identifier, width, err := parseIdentifier(fieldPathString[pos:])
		if err != nil {
			return nil, pos, err
		}

		identifiers = append(identifiers, string(*identifier))
		pos += width
		if pos == len(fieldPathString) || fieldPathString[pos] != identSep {
			break
		}
		pos++ // Skip identSep
	}
	return identifiers, pos, nil
}

func parseVarSubsegment(plainSegment string) (*string, int, error) {
	if len(plainSegment) < 1 {
		return nil, 0, fmt.Errorf("invalid plain segment, expected at least one character: %s", plainSegment)
	}
	if plainSegment[0] == slash {
		return nil, 0, fmt.Errorf("invalid plain segment, cannot start with : %q", slash)
	}
	if len(plainSegment) >= 2 && plainSegment[0:2] == string(star)+string(star) {
		wc := api.MultiSegmentWildcard
		return &wc, 2, nil
	}
	if plainSegment[0] == star {
		wc := api.SingleSegmentWildcard
		return &wc, 1, nil
	}
	literal, pos, err := parseLiteral(plainSegment)
	if err != nil {
		return nil, 0, err
	}
	return (*string)(literal), pos, nil
}

const (
	hexStart   = '%'
	hexdig     = "0123456789ABCDEFabcdef"
	digit      = "0123456789"
	alpha      = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	unreserved = alpha + digit + "-._~"
)

// parseLiteral validates that the provided string conforms to the LITERAL definition, and returns a Literal type if it does.
func parseLiteral(literal string) (*Literal, int, error) {
	var pos int
	for pos < len(literal) {
		if strings.ContainsRune(unreserved, rune(literal[pos])) {
			pos++
		} else if literal[pos] == hexStart {
			if pos+2 >= len(literal) {
				return nil, pos, fmt.Errorf("invalid literal, expected at least 2 characters after the '%%': %s", literal)
			}
			if !strings.ContainsRune(hexdig, rune(literal[pos+1])) || !strings.ContainsRune(hexdig, rune(literal[pos+2])) {
				return nil, pos, fmt.Errorf("invalid literal: %s", literal)
			}
			pos += 3
		} else {
			break
		}
	}
	if pos < 1 {
		return nil, 0, fmt.Errorf("invalid literal, expected at least one character: %s", literal)
	}
	literal = literal[:pos]
	return (*Literal)(&literal), pos, nil
}

func parseIdentifier(identifier string) (*Identifier, int, error) {
	if len(identifier) < 1 {
		return nil, 0, fmt.Errorf("invalid identifier, expected at least one character: %s", identifier)
	}
	if !strings.ContainsRune(alpha, rune(identifier[0])) {
		return nil, 0, fmt.Errorf("invalid identifier, expected it to start with a letter: %s", identifier)
	}
	pos := strings.IndexFunc(identifier, func(r rune) bool { return !strings.ContainsRune(alpha+digit+"_", r) })
	if pos == eof {
		pos = len(identifier)
	}
	identifier = identifier[0:pos]
	return (*Identifier)(&identifier), pos, nil
}
