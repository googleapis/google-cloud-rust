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
	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func init() {
	newCommand(
		"sidekick update",
		"Updates .sidekick.toml with the latest googleapis version and SHA256 and refreshes all the libraries.",
		`
This command will update the googleapis-root and googleapis-sha256 fields in the .sidekick.toml file so they reference the latest googleapis version on Github. After successfully updating the fields, the command will regenerate all the libraries with the new version of the protos.
`,
		cmdSidekick,
		update,
	)
}

func update(rootConfig *config.Config, cmdLine *CommandLine) error {
	if err := config.UpdateRootConfig(rootConfig); err != nil {
		return err
	}
	// Reload the freshly minted configuration.
	rootConfig, err := config.LoadRootConfig(".sidekick.toml")
	if err != nil {
		return err
	}
	return refreshAll(rootConfig, cmdLine)
}
