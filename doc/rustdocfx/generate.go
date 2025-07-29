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
	"os"
	"path/filepath"

	"github.com/cbroglie/mustache"
)

func generate(crate crate) {
	fmt.Printf("Inside generate: crate.Rustdoc: %s\n", crate.Rustdoc)

	// TODO(NOW): Allow outdir to be defined
	wd, _ := os.Getwd()
	outdir := wd
	destination := filepath.Join(outdir, gen.OutputPath)
	os.MkdirAll(filepath.Dir(destination), 0777) // Ignore errors
	nestedProvider := mustacheProvider{
		impl:    provider,
		dirname: filepath.Dir(gen.TemplatePath),
	}
	s, err := mustache.RenderPartials(templateContents, &nestedProvider, model)
	if err != nil {
		errs = append(errs, err)
		continue
	}
	if err := os.WriteFile(destination, []byte(s), 0666); err != nil {
		errs = append(errs, err)
	}

	mustache.ParseFile()
}
