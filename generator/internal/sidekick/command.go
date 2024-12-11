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
	"flag"
	"fmt"
)

type command struct {
	name  string
	short string
	flags *flag.FlagSet
	run   func(*Config, *CommandLine) error
}

var cmdGenerate = &command{
	name:  "generate",
	short: "generates a client library from an API specification",
	run:   generate,
}

var cmdRefresh = &command{
	name:  "refresh",
	short: "rerun the generator on a specific directory",
	run:   refresh,
}

var cmdRefreshAll = &command{
	name:  "refreshhall",
	short: "rerun the generator on all directories",
	run:   refreshAll,
}

var cmdUpdate = &command{
	name:  "update",
	short: "update .sidekick.toml and rerun the generator on all directories",
	run:   update,
}

var commands = []*command{
	cmdGenerate,
	cmdRefresh,
	cmdRefreshAll,
	cmdUpdate,
}

func init() {
	for _, c := range commands {
		c.flags = flag.NewFlagSet(c.name, flag.ContinueOnError)
		c.flags.Usage = constructUsage(c.flags, c.name)
	}

	fs := cmdGenerate.flags
	for _, fn := range []func(fs *flag.FlagSet){} {
		fn(fs)
	}

	fs = cmdRefresh.flags
	for _, fn := range []func(fs *flag.FlagSet){} {
		fn(fs)
	}

	fs = cmdRefreshAll.flags
	for _, fn := range []func(fs *flag.FlagSet){} {
		fn(fs)
	}

	fs = cmdUpdate.flags
	for _, fn := range []func(fs *flag.FlagSet){} {
		fn(fs)
	}
}

func lookup(name string) (*command, error) {
	for _, c := range commands {
		if c.name == name {
			return c, nil
		}
	}
	return nil, fmt.Errorf("invalid command: %q", name)
}

func constructUsage(fs *flag.FlagSet, name string) func() {
	output := fmt.Sprintf("Usage:\n\n  generator %s [arguments]\n", name)
	output += "\nFlags:\n\n"
	return func() {
		fmt.Fprint(fs.Output(), output)
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\n\n")
	}
}
