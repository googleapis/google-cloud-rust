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
	"os"
	"strings"
	"text/template"
)

var usageTemplate = `
{{.LongDescription | trim}}
` + // first prints the entire Long Description for the given command
	`
Usage:
	 sidekick {{.LongName}} {{if (gt (len .Commands) 0) }}<command>{{end}} {{if (gt (len .Flags) 0) }}[flags]{{end}}

` + // then prints the usage line, skipping over <command> if there are no sub-commands, and [flags] if there are no flags
	`{{if (gt (len .Commands) 0) }}The commands are:
{{range .Commands}}	{{.Name | printf "%-15s"}} {{.ShortDescription}}
{{end}}{{end}}
` + // if there are sub-commands, prints the list of sub-commands
	`{{if (gt (len .Flags) 0) }}The flags are:
{{range .Flags}}	-{{.Name | printf "%-25s"}} {{.Usage}}
{{if and (ne .DefValue nil) (gt (len .DefValue) 0)}}	                           Default Value: {{.DefValue}}
{{end}}{{end}}{{end}}
` + // list all flags accepted by this command, if any
	`{{if (gt (len .Commands) 0) }}Use "sidekick help{{with .LongName}} {{.}}{{end}} <command>" for more information about a command.
{{end}}
` // if any sub-commands are available, ends with a note about how to get more information about a sub-command

// Help implements the 'help' command.
// The logic in this function is heavily inspired by (and copied from) the 'go help' command in the Go tool.
// See https://github.com/golang/go/blob/go1.23.4/src/cmd/go/internal/help/help.go#L25 for reference.
func Help(args []string) error {

	cmd := CmdSidekick

Args:
	for i, arg := range args {
		for _, sub := range cmd.commands {
			if sub.Name() == arg {
				cmd = sub
				continue Args
			}
		}

		// helpSuccess is the help command using as many args as possible that would succeed.
		helpSuccess := "sidekick help"
		if i > 0 {
			helpSuccess += " " + strings.Join(args[:i], " ")
		}

		return fmt.Errorf(
			"sidekick help %s: unknown help topic. Run '%s'",
			strings.Join(args, " "),
			helpSuccess)
	}

	return cmd.PrintUsage()
}

// PrintUsage prints the usage of the command to os.Stdout.
func (c *Command) PrintUsage() error {
	c.flags.VisitAll(func(f *flag.Flag) {

	})

	return tmpl(usageTemplate, c)
}

// tmpl executes the given template text on data, writing the result to os.Stdout.
// This logic was copied from the Go source code, and simplified by defaulting to os.Stdout.
// See https://github.com/golang/go/blob/go1.23.4/src/cmd/go/internal/help/help.go#L161 for reference.
func tmpl(text string, data any) error {
	t := template.New("top")
	t.Funcs(template.FuncMap{
		"trim": strings.TrimSpace,
	})
	template.Must(t.Parse(text))
	if err := t.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("error executing template: %v", err)
	}
	return nil
}
