// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"os/exec"
	"testing"

	"mvdan.cc/unparam/check"
)

func TestStaticCheck(t *testing.T) {
	rungo(t, "run", "honnef.co/go/tools/cmd/staticcheck@v0.5.1", "./...")
}

func TestUnparam(t *testing.T) {
	warns, err := check.UnusedParams(false, false, false, "./...")
	if err != nil {
		t.Fatalf("check.UnusedParams: %v", err)
	}
	for _, warn := range warns {
		t.Error(warn)
	}
}

func TestVet(t *testing.T) {
	rungo(t, "vet", "-all", "./...")
}

func TestGoModTidy(t *testing.T) {
	rungo(t, "mod", "tidy")
}

func TestGoFmt(t *testing.T) {
	rungo(t, "fmt")
}

func TestGovulncheck(t *testing.T) {
	rungo(t, "run", "golang.org/x/vuln/cmd/govulncheck@v1.1.3", "./...")
}

func rungo(t *testing.T, args ...string) {
	t.Helper()

	cmd := exec.Command("go", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			t.Fatalf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		t.Fatalf("%v: %v\n%s", cmd, err, output)
	}
}
