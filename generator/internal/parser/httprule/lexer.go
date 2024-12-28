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
	"unicode/utf8"
)

type lexer struct {
	input    string // the input string never changes, the lexer's logic moves start and pos as it scans the input.
	start    int    // the position where the current segment starts
	pos      int    // the current position of the lexer in the input
	width    int
	segments chan segment
	state    stateFn
}

// segmentType defines the type of segment held in the lexer.
// This information is used by the parser to instantiate the correct api.PathSegment
type segmentType int
type segment struct {
	typ segmentType
	val string
}

// formats a segment for debugging
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

const eof = -1

func (l *lexer) nextItem() segment {
	for {
		select {
		case s := <-l.segments:
			return s
		default:
			if l.state == nil {
				// the default case is only executed when the segment channel is empty
				// if by that time the lexer's state is nil, it means we reached an inconsistent state
				// pushing an error segment to the channel will cause the function to exit in the next iteration
				l.errorf("lexer reached a nil state before EOF")
			} else {
				l.state = l.state(l)
			}
		}
	}
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

// ignoreIfMatches ignores the next rune if it matches the given rune.
func (l *lexer) ignoreIfMatches(r rune) bool {
	if l.next() != r {
		l.backup()
		return false
	}
	l.ignore()
	return true
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

// errorf emits an error segment with the given error message into the lexer's segment channel.
// Then returns nil to stop the lexer.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.segments <- segment{
		typ: segmentError,
		val: fmt.Sprintf(format, args...),
	}
	return nil
}

// unexpectedRuneError emits a formatted error segment when an unexpected rune is found.
func (l *lexer) unexpectedRuneError(expected rune, actual rune) stateFn {
	return l.errorf("expected %q, but got %q", expected, actual)
}
