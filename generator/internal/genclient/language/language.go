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

package language

import (
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language/internal/golang"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language/internal/rust"
)

// language is a supported programming language of the generator.
type language int

const (
	undefinedLanguage language = iota
	rustLanguage
	goLanguage
)

func strToLanguage(key string) language {
	strToLangMap := map[string]language{
		"rust": rustLanguage,
		"go":   goLanguage,
	}

	key = strings.ToLower(strings.TrimSpace(key))
	v, ok := strToLangMap[key]
	if !ok {
		return undefinedLanguage
	}
	return v
}

// NewCodec returns a new language codec based on the given language.
func NewCodec(language string) (*Codec, error) {
	var codec *Codec
	switch strToLanguage(language) {
	case rustLanguage:
		codec = &Codec{
			LanguageCodec: rust.NewCodec(),
		}
	case goLanguage:
		codec = &Codec{
			LanguageCodec: golang.NewCodec(),
		}
	default:
		// undefinedLanguage
		return nil, fmt.Errorf("unknown language: %s", language)
	}
	return codec, nil
}

// Codec is an adapter used to transform values into language idiomatic
// representations.
type Codec struct {
	genclient.LanguageCodec
}
