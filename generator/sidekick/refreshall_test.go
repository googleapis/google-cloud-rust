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

package main

import (
	"os"
	"testing"
)

func TestRefreshAll(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir("../.."); err != nil {
		t.Fatal(err)
	}
	rootConfig, err := LoadRootConfig(".sidekick.toml")
	if err != nil {
		t.Fatal(err)
	}
	if err := RefreshAll(rootConfig, []string{"-dry-run"}); err != nil {
		t.Fatal(err)
	}
}
