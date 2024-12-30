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
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"strings"
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
//
// [RFC 3986]: https://datatracker.ietf.org/doc/html/rfc3986#section-3.3
// [Backus-Naur Form]: https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form
// [C++ HTTP Annotation parser]: https://github.com/googleapis/google-cloud-cpp/blob/4174d656136f4b849c8a3d327237f3a96be3e003/generator/internal/http_annotation_parser.h#L49-L58
// [google.api.http annotation]: https://github.com/googleapis/google-cloud-rust/blob/61b9d3bbac5530e4321ac19fe7d2760db82e31db/generator/testdata/googleapis/google/api/http.proto

func Parse(pathTemplate string) (*PathTemplate, error) {
	return parsePathTemplate(pathTemplate)
}

// ParseSegments flattens the result of Parse into a slice of api.PathSegment,
// ignoring variable values and match (* and **) segments.
// TODO(#557): This function is a temporary shim to allow the existing tests to pass.
func ParseSegments(pathTemplate string) ([]api.PathSegment, error) {
	path, err := parsePathTemplate(pathTemplate)
	if err != nil {
		return nil, err
	}
	var segments []api.PathSegment
	for _, s := range path.Segments {
		segment := api.PathSegment{}
		if s.Literal != nil {
			literal := string(*s.Literal)
			segment.Literal = &literal
		} else if s.Variable != nil {
			ids := make([]string, len(s.Variable.FieldPath))
			for i, id := range s.Variable.FieldPath {
				ids[i] = string(*id)
			}
			fieldPath := strings.Join(ids, ".")
			segment.FieldPath = &fieldPath
		}
		segments = append(segments, segment)
	}

	if path.Verb != nil {
		verb := string(*path.Verb)
		segments = append(segments, api.PathSegment{
			Verb: &verb,
		})
	}
	return segments, nil
}

// PathTemplate represents the structure in Go.
type PathTemplate struct {
	Segments []*Segment
	Verb     *Literal
}

// Match represents a single '*' match.
type Match struct{}

// MatchRecursive represents a '**' match.
type MatchRecursive struct{}

type Literal string
type Identifier string

// Variable represents a variable in the path template with its field path and nested segments.
type Variable struct {
	FieldPath []*Identifier
	Segments  []*Segment
}

// Segment represents a single segment of the path template, which can hold one of several types of values.
type Segment struct {
	Literal        *Literal
	Match          *Match
	MatchRecursive *MatchRecursive
	Variable       *Variable
}

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

func parsePathTemplate(pathTemplate string) (*PathTemplate, error) {
	if len(pathTemplate) < 2 {
		return nil, fmt.Errorf("invalid path template, expected at least two characters: %s", pathTemplate)
	} else if pathTemplate[0] != slash {
		return nil, fmt.Errorf("invalid path template, expected it to start with '/': %s", pathTemplate)
	} else if pathTemplate[len(pathTemplate)-1] == slash {
		return nil, fmt.Errorf("invalid path template, expected it to not end with '/': %s", pathTemplate)
	}

	lastPos := len(pathTemplate)
	var err error
	var verb *Literal
	if strings.ContainsRune(pathTemplate, verbSep) {
		lastPos = strings.LastIndex(pathTemplate, string(verbSep))
		verb, err = parseVerb(pathTemplate[lastPos:])
	}
	if err != nil {
		return nil, err
	}

	segments, err := parseSegments(pathTemplate[1:lastPos])
	if err != nil {
		return nil, err
	}

	return &PathTemplate{
		Segments: segments,
		Verb:     verb,
	}, nil

}

func parseVerb(verbString string) (*Literal, error) {
	if len(verbString) == 0 {
		return nil, nil
	}
	if len(verbString) < 2 {
		return nil, fmt.Errorf("invalid verb, when not empty, must have at least two characters")
	} else if verbString[0] != verbSep {
		return nil, fmt.Errorf("invalid verb, must start with '%q': %s", verbSep, verbString)
	}
	return parseLiteral(verbString[1:])
}

// parseSegments parses the first segment out of the provided string, and calls itself recursively to parse the remaining segments, if any.
func parseSegments(segmentsString string) ([]*Segment, error) {
	if len(segmentsString) < 1 {
		return nil, fmt.Errorf("invalid segments, expected at least one character")
	} else if segmentsString[0] == slash {
		return nil, fmt.Errorf("invalid segments, cannot start with '/': %s", segmentsString)
	} else if segmentsString[len(segmentsString)-1] == slash {
		return nil, fmt.Errorf("invalid segments, cannot end with '/': %s", segmentsString)
	}

	var firstSegment *Segment
	var lastPos int
	var err error
	if segmentsString[0] == varLeft {
		lastPos = strings.Index(segmentsString, string(varRight))
		if lastPos == eof {
			return nil, fmt.Errorf("invalid variable, expected to find '%q' before the end of the string: %s",
				varRight,
				segmentsString)
		}
		firstSegment, err = parseVarSegment(segmentsString[:lastPos+1])
		if lastPos == len(segmentsString)-1 {
			lastPos = eof
		} else {
			// If this isn't the last segment in the string, we need to skip the slash.
			lastPos += 1
		}
	} else {
		lastPos = strings.Index(segmentsString, string(slash))
		if lastPos != eof {
			firstSegment, err = parsePlainSegment(segmentsString[:lastPos])
		} else {
			firstSegment, err = parsePlainSegment(segmentsString)
		}
	}
	if err != nil {
		return nil, err
	}
	if lastPos == eof {
		return []*Segment{firstSegment}, nil
	}
	segments, err := parseSegments(segmentsString[lastPos+1:])
	if err != nil {
		return nil, err
	}
	return append([]*Segment{firstSegment}, segments...), nil
}

func parseVarSegment(varString string) (*Segment, error) {
	if len(varString) < 3 {
		return nil, fmt.Errorf("invalid variable, expected at least three characters: %s", varString)
	} else if varString[0] != varLeft {
		return nil, fmt.Errorf("invalid variable, expected it to start with '%q': %s", varLeft, varString)
	} else if varString[len(varString)-1] != varRight {
		return nil, fmt.Errorf("invalid variable, expected it to end with '%q': %s", varRight, varString)
	}
	// Remove the '{' and '}' from the variable string
	varString = varString[1 : len(varString)-1]
	indexOfSep := strings.Index(varString, string(varSep))
	if indexOfSep != eof {
		fieldPath, err := parseFieldPath(varString[:indexOfSep])
		if err != nil {
			return nil, err
		}
		segments, err := parsePlainSegments(varString[indexOfSep+1:])
		if err != nil {
			return nil, err
		}

		return &Segment{
			Variable: &Variable{
				FieldPath: fieldPath,
				Segments:  segments,
			},
		}, nil
	} else {
		fieldPath, err := parseFieldPath(varString)
		if err != nil {
			return nil, err
		}
		return &Segment{
			Variable: &Variable{
				FieldPath: fieldPath,
			},
		}, nil
	}
}

func parsePlainSegments(segmentsString string) ([]*Segment, error) {
	plainSegments := strings.Split(segmentsString, string(slash))
	parsedSegments := make([]*Segment, 0, len(plainSegments))
	for _, plainSegment := range plainSegments {
		parsedSegment, err := parsePlainSegment(plainSegment)
		if err != nil {
			return nil, err
		}
		parsedSegments = append(parsedSegments, parsedSegment)
	}
	return parsedSegments, nil
}

func parseFieldPath(fieldPathString string) ([]*Identifier, error) {
	identifiers := strings.Split(fieldPathString, string(identSep))
	parsedIdentifiers := make([]*Identifier, 0, len(identifiers))
	for _, identifier := range identifiers {
		identifier, err := parseIdentifier(identifier)
		if err != nil {
			return nil, err
		}
		parsedIdentifiers = append(parsedIdentifiers, identifier)
	}
	return parsedIdentifiers, nil
}

func parsePlainSegment(plainSegment string) (*Segment, error) {
	if plainSegment == string(star) {
		return &Segment{Match: &Match{}}, nil
	} else if plainSegment == string(star)+string(star) {
		return &Segment{MatchRecursive: &MatchRecursive{}}, nil
	} else {
		literal, err := parseLiteral(plainSegment)
		if err != nil {
			return nil, err
		}
		return &Segment{Literal: literal}, nil
	}
}

const (
	hexStart   = '%'
	hexdig     = "0123456789ABCDEFabcdef"
	digit      = "0123456789"
	alpha      = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	unreserved = alpha + digit + "-._~"
)

// parseLiteral validates that the provided string conforms to the LITERAL definition, and returns a Literal type if it does.
func parseLiteral(literal string) (*Literal, error) {
	if len(literal) < 1 {
		return nil, fmt.Errorf("invalid literal, expected at least one character: %s", literal)
	}
	i := 0
	for i < len(literal) {
		if strings.ContainsRune(unreserved, rune(literal[i])) {
			i++
		} else if literal[i] == hexStart {
			if i+2 >= len(literal) {
				return nil, fmt.Errorf("invalid literal, expected at least 2 characters after the '%%': %s", literal)
			}
			if !strings.ContainsRune(hexdig, rune(literal[i+1])) || !strings.ContainsRune(hexdig, rune(literal[i+2])) {
				return nil, fmt.Errorf("invalid literal: %s", literal)
			}
			i += 3
		} else {
			return nil, fmt.Errorf("invalid literal: %s", literal)
		}
	}
	return (*Literal)(&literal), nil
}

func parseIdentifier(identifier string) (*Identifier, error) {
	if len(identifier) < 1 {
		return nil, fmt.Errorf("invalid identifier, expected at least one character: %s", identifier)
	}
	if !strings.ContainsRune(alpha, rune(identifier[0])) {
		return nil, fmt.Errorf("invalid identifier, expected it to start with a letter: %s", identifier)
	}

	if i := strings.IndexFunc(identifier, func(r rune) bool { return !strings.ContainsRune(alpha+digit+"_", r) }); i != -1 {
		return nil, fmt.Errorf("invalid identifier, rune '%q' is not valid in an identifier: %s", identifier[i], identifier)
	}
	return (*Identifier)(&identifier), nil
}
