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
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPreserveParagraphs(t *testing.T) {
	input := `Leading text

More text`
	want := input
	got, err := processDocString(input)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in processDocString for paragraphs (-want, +got)\n:%s", diff)
	}
}

func TestPreserveLinks(t *testing.T) {
	input := `[This is a link](www.example.com).

And here is a [reference link] in the middle of text.

And here is [another link][].

And this is [a link with a title][title].

And this is [a link with a title][but-its-different].

[another link]: www.another-link.com
[but-its-different]: www.different.com
[reference link]: www.reference-link.com
[title]: www.title.com`
	want := input
	got, err := processDocString(input)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in processDocString for links (-want, +got)\n:%s", diff)
	}
}

func TestPreserveLists(t *testing.T) {
	input := `Leading text

- an unordered list
  - with a nested item
    - and another nested item
  - and an item with text that
    continues onto an extra line
- we should preserve this list

1. an ordered list
1. with an item

   that has a second paragraph
1. we should preserve this list

More text`
	want := input
	got, err := processDocString(input)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in processDocString for lists (-want, +got)\n:%s", diff)
	}
}
