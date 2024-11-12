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

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language/internal/golang"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language/internal/rust"
)

type createCodec func(*genclient.CodecOptions) genclient.LanguageCodec

func knownCodecs() map[string]createCodec {
	return map[string]createCodec{
		"rust": func(*genclient.CodecOptions) genclient.LanguageCodec { return rust.NewCodec() },
		"go":   func(*genclient.CodecOptions) genclient.LanguageCodec { return golang.NewCodec() },
	}
}

// NewCodec returns a new language codec based on the given language.
func NewCodec(copts *genclient.CodecOptions) (genclient.LanguageCodec, error) {
	create, ok := knownCodecs()[copts.Language]
	if !ok {
		return nil, fmt.Errorf("unknown language: %s", copts.Language)
	}
	return create(copts), nil
}
