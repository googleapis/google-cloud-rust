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

package sidekick

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	toml "github.com/pelletier/go-toml/v2"
)

func TestUpdateRootConfig(t *testing.T) {
	// update() normally writes `.sidekick.toml` to cwd. We need to change to a
	// temporary directory to avoid changing the actual configuration, and any
	// conflicts with other tests running at the same time.
	tempDir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	const (
		getLatestShaPath      = "/repos/googleapis/googleapis/commits/master"
		latestSha             = "5d5b1bf126485b0e2c972bac41b376438601e266"
		tarballPath           = "/googleapis/googleapis/archive/5d5b1bf126485b0e2c972bac41b376438601e266.tar.gz"
		latestShaContents     = "The quick brown fox jumps over the lazy dog"
		latestShaContentsHash = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case getLatestShaPath:
			got := r.Header.Get("Accept")
			want := "application/vnd.github.VERSION.sha"
			if got != want {
				t.Fatalf("mismatched Accept header for %q, got=%q, want=%s", r.URL.Path, got, want)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(latestSha))
		case tarballPath:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(latestShaContents))
		default:
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	rootConfig := &config.Config{
		General: config.GeneralConfig{
			Language:            "rust",
			SpecificationFormat: "protobuf",
		},
		Source: map[string]string{
			"github-api":        server.URL,
			"github":            server.URL,
			"googleapis-root":   fmt.Sprintf("%s/googleapis/googleapis/archive/old.tar.gz", server.URL),
			"googleapis-sha256": "old-sha-unused",
		},
		Codec: map[string]string{},
	}
	if err := config.WriteSidekickToml(".", rootConfig); err != nil {
		t.Fatal(err)
	}

	if err := config.UpdateRootConfig(rootConfig); err != nil {
		t.Fatal(err)
	}

	got := &config.Config{}
	contents, err := os.ReadFile(path.Join(tempDir, ".sidekick.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := toml.Unmarshal(contents, got); err != nil {
		t.Fatal("error reading top-level configuration: %w", err)
	}
	want := &config.Config{
		General: rootConfig.General,
		Source: map[string]string{
			"github-api":        server.URL,
			"github":            server.URL,
			"googleapis-root":   fmt.Sprintf("%s/googleapis/googleapis/archive/%s.tar.gz", server.URL, latestSha),
			"googleapis-sha256": latestShaContentsHash,
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in loaded root config (-want, +got)\n:%s", diff)
	}
}
