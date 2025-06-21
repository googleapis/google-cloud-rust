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

// Package config provides functionality for working with the sidekick.toml
// configuration file.
package config

import (
	"crypto/sha256"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/license"
	toml "github.com/pelletier/go-toml/v2"
)

const (
	defaultGitHubApi = "https://api.github.com"
	defaultGitHub    = "https://github.com"
	repo             = "googleapis/googleapis"
	branch           = "master"
)

// Describe overrides for the documentation of a single element.
//
// This should be used sparingly. Generally we should prefer updating the
// comments upstream, and then getting a new version of the services
// specification. The exception may be when the fixes take a long time, or are
// specific to one language.
type DocumentationOverride struct {
	ID      string `toml:"id"`
	Match   string `toml:"match"`
	Replace string `toml:"replace"`
}

type Config struct {
	General GeneralConfig `toml:"general"`

	Source           map[string]string       `toml:"source,omitempty"`
	Codec            map[string]string       `toml:"codec,omitempty"`
	CommentOverrides []DocumentationOverride `toml:"documentation-overrides,omitempty"`
}

// Configuration parameters that affect Parsers and Codecs, including the
// selection of parser and codec.
type GeneralConfig struct {
	Language            string `toml:"language,omitempty"`
	SpecificationFormat string `toml:"specification-format,omitempty"`
	SpecificationSource string `toml:"specification-source,omitempty"`
	ServiceConfig       string `toml:"service-config,omitempty"`
}

// LoadConfig loads the top-level configuration file and validates its contents.
// If no top-level file is found, falls back to the default configuration.
// Where applicable, overrides the top level (or default) configuration values with the ones passed in the command line.
// Returns the merged configuration, or an error if the top level configuration is invalid.
func LoadConfig(language string, source, codec map[string]string) (*Config, error) {
	rootConfig, err := LoadRootConfig(".sidekick.toml")
	if err != nil {
		return nil, err
	}
	argsConfig := &Config{
		General: GeneralConfig{
			Language: language,
		},
		Source: maps.Clone(source),
		Codec:  maps.Clone(codec),
	}
	config, err := mergeConfigs(rootConfig, argsConfig)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func LoadRootConfig(filename string) (*Config, error) {
	config := &Config{
		Codec:  map[string]string{},
		Source: map[string]string{},
	}
	if contents, err := os.ReadFile(filename); err == nil {
		err = toml.Unmarshal(contents, &config)
		if err != nil {
			return nil, fmt.Errorf("error reading top-level configuration: %w", err)
		}
	}
	// Ignore errors reading the top-level file.
	return config, nil
}

func MergeConfigAndFile(rootConfig *Config, filename string) (*Config, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var local Config
	err = toml.Unmarshal(contents, &local)
	if err != nil {
		return nil, fmt.Errorf("error reading configuration %s: %w", filename, err)
	}
	return mergeConfigs(rootConfig, &local)
}

func mergeConfigs(rootConfig, local *Config) (*Config, error) {
	merged := Config{
		General: GeneralConfig{
			Language:            rootConfig.General.Language,
			SpecificationFormat: rootConfig.General.SpecificationFormat,
		},
		Source:           map[string]string{},
		Codec:            map[string]string{},
		CommentOverrides: local.CommentOverrides,
	}
	for k, v := range rootConfig.Codec {
		merged.Codec[k] = v
	}
	for k, v := range rootConfig.Source {
		merged.Source[k] = v
	}

	// Ignore `SpecificationSource` and `ServiceConfig` at the top-level
	// configuration. It makes no sense to set those globally.
	merged.General.SpecificationSource = local.General.SpecificationSource
	merged.General.ServiceConfig = local.General.ServiceConfig
	if local.General.SpecificationFormat != "" {
		merged.General.SpecificationFormat = local.General.SpecificationFormat
	}
	if local.General.Language != "" {
		merged.General.Language = local.General.Language
	}
	for k, v := range local.Codec {
		merged.Codec[k] = v
	}
	for k, v := range local.Source {
		merged.Source[k] = v
	}
	// Ignore errors reading the top-level file.
	return &merged, nil
}

func UpdateRootConfig(rootConfig *Config) error {
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

func WriteSidekickToml(outDir string, config *Config) error {
	if err := os.MkdirAll(outDir, 0777); err != nil {
		return err
	}
	f, err := os.Create(path.Join(outDir, ".sidekick.toml"))
	if err != nil {
		return err
	}
	defer f.Close()

	year := config.Codec["copyright-year"]
	for _, line := range license.LicenseHeader(year) {
		if line == "" {
			fmt.Fprintln(f, "#")
		} else {
			fmt.Fprintf(f, "#%s\n", line)
		}
	}
	fmt.Fprintln(f, "")

	t := toml.NewEncoder(f)
	if err := t.Encode(config); err != nil {
		return err
	}
	return f.Close()
}
