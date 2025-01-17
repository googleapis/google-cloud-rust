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
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
	"github.com/walle/targz"
)

func TestExistingDirectory(t *testing.T) {
	tmp, err := os.MkdirTemp(t.TempDir(), "sidekick-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	rootConfig := config.Config{
		Source: map[string]string{
			"googleapis-root": tmp,
		},
	}
	root, err := makeSourceRoot(&rootConfig, "googleapis")
	if err != nil {
		t.Error(err)
	}
	if root != tmp {
		t.Errorf("mismatched root directory got=%s, want=%s", root, tmp)
	}
}

func TestValidateConfig(t *testing.T) {
	rootConfig := config.Config{
		Source: map[string]string{
			"googleapis-root": "https://unused",
		},
	}
	_, err := makeSourceRoot(&rootConfig, "googleapis")
	if err == nil {
		t.Errorf("expected error when missing `googleapis-sha256")
	}
}

func TestWithDownload(t *testing.T) {
	testDir, err := os.MkdirTemp(t.TempDir(), "sidekick-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	simulatedSha := "2d08f07eab9bbe8300cd20b871d0811bbb693fab"
	simulatedSubdir := fmt.Sprintf("googleapis-%s", simulatedSha)
	simulatedPath := fmt.Sprintf("/archive/%s.tar.gz", simulatedSha)
	tarball, err := makeTestTarball(t, testDir, simulatedSubdir)
	if err != nil {
		t.Fatal(err)
	}

	// In this test we expect that a download is needed.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != simulatedPath {
			t.Errorf("Expected to request '%s', got: %s", simulatedPath, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write(tarball.Contents)
	}))
	defer server.Close()

	rootConfig := &config.Config{
		Source: map[string]string{
			"googleapis-root":   server.URL + simulatedPath,
			"googleapis-sha256": tarball.Sha256,
			"cachedir":          testDir,
		},
	}
	got, err := makeSourceRoot(rootConfig, "googleapis")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, tarball.Sha256) {
		t.Errorf("mismatched suffix in makeSourceRoot want=%s, got=%s", tarball.Sha256, got)
	}
	if err := os.RemoveAll(got); err != nil {
		t.Error(err)
	}
	if err := os.Remove(got + ".tar.gz"); err != nil {
		t.Error(err)
	}
}

func TestTargetExists(t *testing.T) {
	testDir, err := os.MkdirTemp(t.TempDir(), "sidekick-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	sha256 := "eb853d49313f20a096607fea87dfc10bd6a1b917ad17ad5db8a205b457a940e1"
	rootConfig := &config.Config{
		Source: map[string]string{
			"googleapis-root":   "https://unused/path",
			"googleapis-sha256": sha256,
			"cachedir":          testDir,
		},
	}

	downloads, err := getCacheDir(rootConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(path.Join(downloads, sha256), 0755); err != nil {
		t.Fatal(err)
	}
	got, err := makeSourceRoot(rootConfig, "googleapis")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, sha256) {
		t.Errorf("mismatched suffix in makeSourceRoot want=%s, got=%s", sha256, got)
	}
	if err := os.RemoveAll(got); err != nil {
		t.Error(err)
	}
}

func TestDownloadGoogleapisRootTgzExists(t *testing.T) {
	testDir, err := os.MkdirTemp(t.TempDir(), "sidekick-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	tarball, err := makeTestContents(t)
	if err != nil {
		t.Fatal(err)
	}

	// In this test we will create the download file with the right contents.
	target := path.Join(testDir, "existing-file")
	if err := os.WriteFile(target, tarball.Contents, 0644); err != nil {
		t.Fatal(err)
	}

	if err := downloadSourceRoot(target, "https://unused/placeholder.tar.gz", tarball.Sha256); err != nil {
		t.Error(err)
	}
}

func TestDownloadGoogleapisRootNeedsDownload(t *testing.T) {
	testDir, err := os.MkdirTemp(t.TempDir(), "sidekick-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	tarball, err := makeTestContents(t)
	if err != nil {
		t.Fatal(err)
	}

	// In this test we expect that a download is needed.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/placeholder.tar.gz" {
			t.Errorf("Expected to request '/placeholder.tar.gz', got: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write(tarball.Contents)
	}))
	defer server.Close()

	expected := path.Join(testDir, "new-file")
	if err := downloadSourceRoot(expected, server.URL+"/placeholder.tar.gz", tarball.Sha256); err != nil {
		t.Error(err)
	}
	got, err := os.ReadFile(expected)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(tarball.Contents, got); diff != "" {
		t.Errorf("mismatched downloaded contents, (-want, +got):\n%s", diff)
	}
}

type contents struct {
	Sha256   string
	Contents []byte
}

func makeTestContents(t *testing.T) (*contents, error) {
	t.Helper()

	hasher := sha256.New()
	var data []byte
	for i := range 10 {
		line := []byte(fmt.Sprintf("%08d the quick brown fox jumps over the lazy dog\n", i))
		data = append(data, line...)
		hasher.Write(line)
	}

	return &contents{
		Sha256:   fmt.Sprintf("%x", hasher.Sum(nil)),
		Contents: data,
	}, nil
}

func makeTestTarball(t *testing.T, tempDir, subdir string) (*contents, error) {
	t.Helper()

	top := path.Join(tempDir, subdir)
	if err := os.MkdirAll(top, 0755); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		name := fmt.Sprintf("file-%04d", i)
		err := os.WriteFile(path.Join(top, name), []byte(fmt.Sprintf("%08d the quick brown fox jumps over the lazy dog\n", i)), 0644)
		if err != nil {
			return nil, err
		}
	}

	tgz := path.Join(tempDir, "tarball.tgz")
	defer os.Remove(tgz)

	if err := targz.Compress(top, tgz); err != nil {
		return nil, err
	}

	hasher := sha256.New()
	data, err := os.ReadFile(tgz)
	if err != nil {
		return nil, err
	}
	hasher.Write(data)

	return &contents{
		Sha256:   fmt.Sprintf("%x", hasher.Sum(nil)),
		Contents: data,
	}, nil
}

func TestExtractedName(t *testing.T) {
	var rootConfig config.Config
	got := extractedName(&rootConfig, "https://github.com/googleapis/googleapis/archive/2d08f07eab9bbe8300cd20b871d0811bbb693fab.tar.gz", "googleapis")
	want := "googleapis-2d08f07eab9bbe8300cd20b871d0811bbb693fab"
	if got != want {
		t.Errorf("mismatched extractedName, got=%s, want=%s", got, want)
	}
}

func TestExtractedNameOverride(t *testing.T) {
	want := "override"
	rootConfig := config.Config{
		Source: map[string]string{
			"googleapis-extracted-name": want,
		},
	}
	got := extractedName(&rootConfig, "https://github.com/googleapis/googleapis/archive/2d08f07eab9bbe8300cd20b871d0811bbb693fab.tar.gz", "googleapis")
	if got != want {
		t.Errorf("mismatched extractedName, got=%s, want=%s", got, want)
	}
}

func TestDownloadsCacheDir(t *testing.T) {
	dir, err := getCacheDir(&config.Config{Source: map[string]string{"cachedir": "test-only"}})
	if err != nil {
		t.Fatal(err)
	}
	checkDownloadsCacheDir(t, dir, "test-only")

	user, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	dir, err = getCacheDir(&config.Config{Source: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}
	checkDownloadsCacheDir(t, dir, user)
}

func checkDownloadsCacheDir(t *testing.T, got, root string) {
	t.Helper()
	if !strings.HasPrefix(got, root) {
		t.Errorf("mismatched downloadsCacheDir, want=%s, got=%s", root, got)
	}
	if !strings.Contains(got, path.Join("sidekick", "downloads")) {
		t.Errorf("mismatched downloadsCacheDir, want=%s, got=%s", "sidekick", root)
	}
}
