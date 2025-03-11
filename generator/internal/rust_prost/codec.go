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

package rust_prost

import (
	"fmt"
	"time"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

type codec struct {
	GenerationYear string
	PackageName    string
}

func newCodec(cfg *config.Config) *codec {
	year, _, _ := time.Now().Date()
	result := &codec{
		GenerationYear: fmt.Sprintf("%04d", year),
		PackageName:    "",
	}
	for key, definition := range cfg.Codec {
		switch key {
		case "copyright-year":
			result.GenerationYear = definition
		case "package-name-override":
			result.PackageName = definition
		default:
			// Ignore other options.
		}
	}
	return result
}
