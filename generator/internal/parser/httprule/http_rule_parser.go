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
	"unicode/utf8"
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
			segments = append(segments, api.NewLiteralPathSegment(s.val))
		case segmentVerb:
			segments = append(segments, api.NewVerbPathSegment(s.val))
		case segmentError:
			return nil, fmt.Errorf("error parsing path template (%s): %s", pathTemplate, s.val)
		case segmentEOF:
			return segments, nil
		}

	}
}

func (l *lexer) nextItem() segment {
	for {
		select {
		case s := <-l.segments:
			return s
		default:
			l.state = l.state(l)
			if l.state == nil {
				l.errorf("lexer reached a nil state before EOF")
			}
		}
	}
}

type segment struct {
	typ segmentType
	val string
}

func (s segment) String() string {
	switch s.typ {
	case segmentError:
		return s.val // error messages are already formatted
	case segmentEOF:
		return "EOF"
	default:
		return fmt.Sprintf("{%v %q}", s.typ, s.val)

	}
}

type lexer struct {
	input    string // the input string never changes, the lexer's logic moves start and pos as it scans the input.
	start    int    // the position where the current segment starts
	pos      int    // the current position of the lexer in the input
	width    int
	segments chan segment
	state    stateFn
}

// emit sends the current segment to the lexer's segment channel,
// then moves the start position where the next segment should begin.
func (l *lexer) emit(t segmentType) {
	l.segments <- segment{
		typ: t,
		val: l.input[l.start:l.pos],
	}
	l.start = l.pos
}

const (
	eof      = -1
	slash    = '/'
	varLeft  = '{'
	varRight = '}'
	varSep   = '='
	verbSep  = ':'
)

// next returns the next rune in the input string, without changing the start position of the segment
func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}

	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += w
	return r
}

// peek returns the next rune in the input string without advancing the lexer's position
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup moves the lexer's position back to the last rune read
func (l *lexer) backup() {
	l.pos -= l.width
}

// ignore skips over any input not yet emitted
func (l *lexer) ignore() {
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.next()) {
		return true
	}
	l.backup()
	return false
}

// acceptAll consumes a run of runes from the valid set.
func (l *lexer) acceptAll(valid string) bool {
	accepted := false
	for l.accept(valid) {
		accepted = true // at least one rune was accepted
	}
	return accepted
}

// segmentType defines the type of segment held in the lexer.
// This information is used by the parser to instantiate the correct api.PathSegment
type segmentType int

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
	segmentVerb
	segmentEOF
)

type stateFn func(*lexer) stateFn

// slashState is the first state before a segment
func slashState(l *lexer) stateFn {
	if l.next() != slash {
		return l.errorf("expected '/' but got %q", l.input[l.pos])
	}
	l.ignore() // no need to emit a segment for the slash, just move on to the next state
	return segmentState
}

func segmentState(l *lexer) stateFn {
	switch l.peek() {
	case eof:
		return l.errorf("expected a segment, found EOF")
	case '*':
		return l.errorf("* is not implemented yet")
	}
	return literalState
}

func literalState(l *lexer) stateFn {
	if !l.acceptAll("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") {
		return l.errorf("expected a literal segment")
	}

	l.emit(segmentLiteral)
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

func verbState(l *lexer) stateFn {
	return nil
}

func eofState(l *lexer) stateFn {
	if l.peek() != eof {
		return l.errorf("expected EOF, but got %q", l.peek())
	}
	l.emit(segmentEOF)
	return nil
}

// errorf emits an error segment with the given error message into the lexer's segment channel.
// Then returns nil to stop the lexer.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.segments <- segment{
		typ: segmentError,
		val: fmt.Sprintf(format, args...),
	}
	return nil
}
