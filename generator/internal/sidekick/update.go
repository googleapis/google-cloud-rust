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
	"io"
	"net/http"
	"os"
	"strings"
)

const (
	defaultGitHubApi = "https://api.github.com"
	defaultGitHub    = "https://github.com"
	repo             = "googleapis/googleapis"
	branch           = "master"
)

func update(rootConfig *Config, cmdLine *CommandLine) error {
	if err := updateRootConfig(rootConfig); err != nil {
		return err
	}
	// Reload the freshly minted configuration.
	rootConfig, err := loadRootConfig(".sidekick.toml")
	if err != nil {
		return err
	}
	return refreshAll(rootConfig, cmdLine)
}

func updateRootConfig(rootConfig *Config) error {
	gitHubApi, ok := rootConfig.Source["github-api"]
	if !ok {
		gitHubApi = defaultGitHubApi
	}
	gitHub, ok := rootConfig.Source["github"]
	if !ok {
		gitHub = defaultGitHub
	}

	query := fmt.Sprintf("%s/repos/%s/commits/%s", gitHubApi, repo, branch)
	fmt.Printf("getting latest SHA from %q\n", query)
	latestSha, err := getLatestSha(query)
	if err != nil {
		return err
	}

	newRoot := fmt.Sprintf("%s/%s/archive/%s.tar.gz", gitHub, repo, latestSha)
	fmt.Printf("computing SHA256 for %q\n", newRoot)
	newSha256, err := getSha256(newRoot)
	if err != nil {
		return err
	}
	fmt.Printf("updating .sidekick.toml\n")

	contents, err := os.ReadFile(".sidekick.toml")
	if err != nil {
		return err
	}
	var newContents []string
	for _, line := range strings.Split(string(contents), "\n") {
		switch {
		case strings.HasPrefix(line, "googleapis-root "):
			s := strings.SplitN(line, "=", 2)
			if len(s) != 2 {
				return fmt.Errorf("invalid googleapis-root line, expected = separator, got=%q", line)
			}
			newContents = append(newContents, fmt.Sprintf("%s= '%s'", s[0], newRoot))
		case strings.HasPrefix(line, "googleapis-sha256 "):
			s := strings.SplitN(line, "=", 2)
			if len(s) != 2 {
				return fmt.Errorf("invalid googleapis-sha256 line, expected = separator, got=%q", line)
			}
			newContents = append(newContents, fmt.Sprintf("%s= '%s'", s[0], newSha256))
		default:
			newContents = append(newContents, line)
		}
	}

	cwd, _ := os.Getwd()
	fmt.Printf("%s\n", cwd)
	f, err := os.Create(".sidekick.toml")
	if err != nil {
		return err
	}
	defer f.Close()
	for i, line := range newContents {
		f.Write([]byte(line))
		if i != len(newContents)-1 {
			f.Write([]byte("\n"))
		}
	}
	return f.Close()
}

func getLatestSha(query string) (string, error) {
	client := &http.Client{}
	request, err := http.NewRequest(http.MethodGet, query, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "application/vnd.github.VERSION.sha")
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	if response.StatusCode >= 300 {
		return "", fmt.Errorf("http error in download %s", response.Status)
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func getSha256(query string) (string, error) {
	response, err := http.Get(query)
	if err != nil {
		return "", err
	}
	if response.StatusCode >= 300 {
		return "", fmt.Errorf("http error in download %s", response.Status)
	}
	defer response.Body.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, response.Body); err != nil {
		return "", err
	}
	got := fmt.Sprintf("%x", hasher.Sum(nil))
	return got, nil
}
