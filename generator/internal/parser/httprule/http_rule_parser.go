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
)

// The logic in this file is based on the Mustache template implementation.
// Reference:
// - https://go.dev/talks/2011/lex.slide (slides)
// - https://go.dev/talks/2011/lex/r59-lex.go (code)

func Parse(pathTemplate string) ([]api.PathSegment, error) {

	l := &lexer{
		input:    pathTemplate,
		state:    slashState, // the first state is always slashState
		segments: make(chan segment, 2),
	}
	var segments []api.PathSegment
	for {
		s := l.nextItem()
		switch s.typ {
		case segmentLiteral:
			segments = append(segments, api.NewLiteralPathSegment(s.val))
		case segmentIdentifier:
			segments = append(segments, api.NewFieldPathPathSegment(s.val))
		case segmentVerb:
			segments = append(segments, api.NewVerbPathSegment(s.val))
		case segmentError:
			return nil, fmt.Errorf("error parsing path template (%s): %s", pathTemplate, s.val)
		case segmentEOF:
			return segments, nil
		}

	}
}

// ### Path template syntax
//
//	Template = "/" Segments [ Verb ] ;
//	Segments = Segment { "/" Segment } ;
//	Segment  = "*" | "**" | LITERAL | Variable ;
//	Variable = "{" FieldPath [ "=" Segments ] "}" ;
//	FieldPath = IDENT { "." IDENT } ;
//	Verb     = ":" LITERAL ;
const (
	segmentError segmentType = iota // 0
	segmentLiteral
	segmentIdentifier
	segmentVerb //a verb is actually ':' + LITERAL,
	// but we need a way to differentiate a literal within a segment, from a literal within a verb
	segmentEOF
)

const (
	slash    = '/'
	star     = '*'
	varLeft  = '{'
	varRight = '}'
	varSep   = '='
	verbSep  = ':'
)

type stateFn func(*lexer) stateFn

// slashState is the first state before a segment
func slashState(l *lexer) stateFn {
	if !l.ignoreIfMatches(slash) {
		return l.unexpectedRuneError(slash, l.peek())
	}
	return segmentState
}

func segmentState(l *lexer) stateFn {
	switch l.peek() {
	case eof, slash, verbSep:
		return l.errorf("expected a segment, found %q", l.peek())
	case varLeft:
		return varState
	}
	return literalState
}

const validFirstIdentifierRunes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"abcdefghiljkmnopqrstuvwxyz"

const validIdentifierRunes = validFirstIdentifierRunes +
	"0123456789" +
	"_"
const validVariableValueRunes = validIdentifierRunes + "/*"

func varState(l *lexer) stateFn {
	if !l.ignoreIfMatches(varLeft) {
		return l.unexpectedRuneError(varLeft, l.peek())
	}

	if !l.accept(validFirstIdentifierRunes) {
		return l.errorf("expected a valid first rune for a variable identifier, got %q", l.peek())
	}

	// The identifier is a single or multiple characters, just take all that match.
	_ = l.acceptAll(validIdentifierRunes)

	// We have reached the end of the valid characters for an identifier
	// emit the segment now and process the rest of the variable for correctness.
	l.emit(segmentIdentifier)

	if l.ignoreIfMatches(varSep) {
		// This var has a value, we don't use it for anything, so lets just ignore it.
		//TODO The valid format for a variable value is a sequence of segments, we could validate that here.
		if !l.acceptAll(validVariableValueRunes) {
			return l.errorf("expected a valid variable value, got %q", l.peek())
		}
		l.ignore()
	}

	if !l.ignoreIfMatches(varRight) {
		return l.unexpectedRuneError(varRight, l.peek())
	}
	return eoSegmentState
}

// verbState must start with a verbSep
func verbState(l *lexer) stateFn {
	if !l.ignoreIfMatches(verbSep) {
		return l.unexpectedRuneError(verbSep, l.peek())
	}
	if !l.acceptAll(validLiteralRunes) {
		return l.errorf("expected a literal segment")
	}
	l.emit(segmentVerb)
	// the only valid rune after a verb is EOF
	return eofState
}

const validLiteralRunes = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789" +
	"-._~" +
	"%" +
	"!$&'()*+,;="

func literalState(l *lexer) stateFn {
	if l.accept(string(star)) {
		// if literal starts with a *, it can have either a * or ** as the value
		// we don't differentiate between those, so just accept the next character if it's a *
		_ = l.accept(string(star))
	} else if !l.acceptAll(validLiteralRunes) {
		return l.errorf("expected a literal segment")
	}

	l.emit(segmentLiteral)
	return eoSegmentState
}

// eoSegmentState represents the state at the end of a segment
// This state makes no modifications to the lexer, it just decides which state to transition to next
func eoSegmentState(l *lexer) stateFn {
	switch l.peek() {
	case eof:
		return eofState
	case slash:
		return slashState
	case verbSep:
		return verbState
	}
	return nil
}

func eofState(l *lexer) stateFn {
	if l.peek() != eof {
		return l.errorf("expected EOF, but got %q", l.peek())
	}
	l.emit(segmentEOF)
	return nil
}
