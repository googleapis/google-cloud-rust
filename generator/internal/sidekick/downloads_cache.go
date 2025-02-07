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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func makeSourceRoot(rootConfig *config.Config, configPrefix string) (string, error) {
	sourceRoot, ok := rootConfig.Source[fmt.Sprintf("%s-root", configPrefix)]
	if !ok {
		return "", nil
	}
	if ok := isDirectory(sourceRoot); ok {
		return sourceRoot, nil
	}
	if !requiresDownload(sourceRoot) {
		return "", fmt.Errorf("only directories and https URLs are supported for googleapis-root")
	}
	// Treat `googleapis-root` as a URL to download. We want to avoid downloads
	// if possible, so we will first try to use a cache directory in $HOME.
	// Only if that fails we try a new download.
	source, ok := rootConfig.Source[fmt.Sprintf("%s-sha256", configPrefix)]
	if !ok {
		return "", fmt.Errorf("using an https:// URL for googleapis-root requires setting googleapis-sha256")
	}
	cacheDir, err := getCacheDir(rootConfig)
	if err != nil {
		return "", err
	}
	target := path.Join(cacheDir, source)
	if isDirectory(target) {
		return target, nil
	}
	tgz := target + ".tar.gz"
	if err := downloadSourceRoot(tgz, sourceRoot, source); err != nil {
		return "", err
	}

	if err := extractTarball(tgz, cacheDir); err != nil {
		slog.Error("error extracting .tar.gz file", "file", tgz, "cacheDir", cacheDir, "error", err)
		return "", err
	}
	dirname := extractedName(rootConfig, sourceRoot, configPrefix)
	if err := os.Rename(path.Join(cacheDir, dirname), target); err != nil {
		return "", err
	}
	return target, nil
}

func extractTarball(source, destination string) error {
	cmd := exec.Command("tar", "-zxf", source)
	cmd.Dir = destination
	if output, err := cmd.CombinedOutput(); err != nil {
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return fmt.Errorf("%v: %v\n%s", cmd, err, ee.Stderr)
		}
		return fmt.Errorf("%v: %v\n%s", cmd, err, output)
	}
	return nil
}

func extractedName(rootConfig *config.Config, googleapisRoot, configPrefix string) string {
	name, ok := rootConfig.Source[fmt.Sprintf("%s-extracted-name", configPrefix)]
	if ok {
		return name
	}
	return "googleapis-" + filepath.Base(strings.TrimSuffix(googleapisRoot, ".tar.gz"))
}

func downloadSourceRoot(target, source, sha256 string) error {
	if fileExists(target) {
		return nil
	}
	var err error
	backoff := 10 * time.Second
	for i := range 3 {
		if i != 0 {
			time.Sleep(backoff)
			backoff = 2 * backoff
		}
		if err = downloadAttempt(target, source, sha256); err == nil {
			return nil
		}
	}
	return fmt.Errorf("download failed after 3 attempts, last error=%w", err)
}

func downloadAttempt(target, source, expectedSha256 string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0777); err != nil {
		return err
	}
	tempFile, err := os.CreateTemp(filepath.Dir(target), "temp-")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	response, err := http.Get(source)
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		return fmt.Errorf("http error in download %s", response.Status)
	}

	if _, err := io.Copy(tempFile, response.Body); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	if err := response.Body.Close(); err != nil {
		return err
	}
	file, err := os.Open(tempFile.Name())
	if err != nil {
		return err
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return err
	}
	got := fmt.Sprintf("%x", hasher.Sum(nil))
	if expectedSha256 != got {
		return fmt.Errorf("mismatched hash on download, expected=%s, got=%s", expectedSha256, got)
	}
	return os.Rename(tempFile.Name(), target)
}

func fileExists(name string) bool {
	stat, err := os.Stat(name)
	if err != nil {
		return false
	}
	return stat.Mode().IsRegular()
}

func isDirectory(name string) bool {
	stat, err := os.Stat(name)
	if err != nil {
		return false
	}
	if !stat.IsDir() {
		return false
	}
	return true
}

func getCacheDir(rootConfig *config.Config) (string, error) {
	cacheDir, ok := rootConfig.Source["cachedir"]
	if !ok {
		var err error
		if cacheDir, err = os.UserCacheDir(); err != nil {
			return "", err
		}
	}
	return path.Join(cacheDir, "sidekick", "downloads"), nil
}

func requiresDownload(googleapisRoot string) bool {
	return strings.HasPrefix(googleapisRoot, "https://") || strings.HasPrefix(googleapisRoot, "http://")
}
