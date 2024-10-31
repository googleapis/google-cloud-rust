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

package main_test

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

var googleHeader = regexp.MustCompile(`^// Copyright 20\d\d Google LLC
//
// Licensed under the Apache License, Version 2\.0 \(the "License"\);`)

func TestHeaders(t *testing.T) {
	sfs := os.DirFS(".")
	fs.WalkDir(sfs, ".", func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := sfs.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		if !googleHeader.MatchReader(bufio.NewReader(f)) {
			t.Errorf("%q: incorrect header", path)
		}
		return nil
	})
}

func TestStaticCheck(t *testing.T) {
	rungo(t, "run", "honnef.co/go/tools/cmd/staticcheck@v0.5.1", "./...")
}

func TestUnparam(t *testing.T) {
	rungo(t, "run", "mvdan.cc/unparam@v0.0.0-20240917084806-57a3b4290ba3", "./...")
}

func TestVet(t *testing.T) {
	rungo(t, "vet", "-all", "./...")
}

func TestGoModTidy(t *testing.T) {
	rungo(t, "mod", "tidy", "-diff")
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
