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
	"slices"
	"strings"
)

// A Command is an implementation of a sidekick command
// like sidekick generate
type Command struct {
	action           func(rootConfig *Config, cmdLine *CommandLine) error
	usageLine        string
	altNames         []string
	shortDescription string
	longDescription  string
	flags            *flag.FlagSet
	commands         []*Command
	parent           *Command
}

func (c *Command) UsageLine() string {
	return c.usageLine
}

func (c *Command) LongDescription() string {
	if c.longDescription != "" {
		return c.longDescription
	} else {
		return c.shortDescription
	}
}

func (c *Command) ShortDescription() string {
	return c.shortDescription
}

// LongName returns the command's long name: all the words in the usage line between "sidekick" and a flag or argument,
func (c *Command) LongName() string {
	name := c.usageLine
	if i := strings.Index(name, " ["); i >= 0 {
		name = name[:i]
	}
	return strings.TrimSpace(strings.TrimPrefix(name, "sidekick"))
}

func (c *Command) AddAltName(n string) *Command {
	c.altNames = append(c.altNames, n)
	return c
}

// Name returns the command's short name: the last word in the usage line before a flag or argument.
func (c *Command) Name() string {
	name := c.LongName()
	if i := strings.LastIndex(name, " "); i >= 0 {
		name = name[i+1:]
	}
	return name
}

func (c *Command) Names() []string {
	names := []string{c.Name()}
	names = append(names, c.altNames...)
	return names
}

func (c *Command) Runnable() bool {
	return c.action != nil
}

func (c *Command) Commands() []*Command {
	return c.commands
}

// Lookup recursively iterates through the command's sub-commands to find the one that matches the first argument,
// until no arguments are left or a flag is found.
// If an exact match is found, it returns the command, true, and the remaining args.
// If no exact match is found, it returns the last command to match the args, false, and the remaining args.
func (c *Command) Lookup(args []string) (*Command, bool, []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return c, true, args
	}
	for _, sub := range c.commands {
		if slices.Contains(sub.Names(), args[0]) {
			return sub.Lookup(args[1:])
		}
	}
	return c, false, args
}

// The following AddFlag* methods are syntax sugar to enable flags to be added in the same line as the Command is created.
// Not all flag types are supported yet, only the ones used in the current implementation.

func (c *Command) AddFlagBool(p *bool, name string, value bool, usage string) *Command {
	c.flags.BoolVar(p, name, value, usage)
	return c
}

func (c *Command) AddFlagString(p *string, name string, value string, usage string) *Command {
	c.flags.StringVar(p, name, value, usage)
	return c
}

func (c *Command) AddFlagFunc(name string, usage string, fn func(string) error) *Command {
	c.flags.Func(name, usage, fn)
	return c
}

// Flags returns all flags added to this command, as well as its parent hierarchy
func (c *Command) Flags() []*flag.Flag {
	var flags = []*flag.Flag{}
	c.VisitAllFlags(func(f *flag.Flag) {
		flags = append(flags, f)
	})
	return flags
}

// VisitAllFlags visits all flags in the command, including those of its parent hierarchy
func (c *Command) VisitAllFlags(fn func(f *flag.Flag)) {
	c.flags.VisitAll(fn)
	if c.parent != nil {
		c.parent.VisitAllFlags(fn)
	}
}

func (c *Command) Run(rootConfig *Config, cmdLine *CommandLine) error {
	if !c.Runnable() {
		return fmt.Errorf("command sidekick %s is not runnable", c.LongName())
	}

	return c.action(rootConfig, cmdLine)
}

func (c *Command) ParseCmdLine(args []string) (*CommandLine, error) {
	if c.parent != nil {
		c.parent.VisitAllFlags(func(f *flag.Flag) {
			c.flags.Var(f.Value, f.Name, f.Usage)
		})
	}

	if err := c.flags.Parse(args); err != nil {
		return nil, err
	}

	return &CommandLine{
		Command:             args,
		ProjectRoot:         flagProjectRoot,
		SpecificationFormat: format,
		SpecificationSource: source,
		ServiceConfig:       serviceConfig,
		Source:              sourceOpts,
		Language:            flagLanguage,
		Output:              output,
		Codec:               codecOpts,
		DryRun:              dryrun,
	}, nil
}

func NewCommand(
	usageLine string,
	shortDescription string,
	longDescription string,
	parent *Command,
	action func(rootConfig *Config, cmdLine *CommandLine) error,
) *Command {
	if len(usageLine) == 0 {
		panic("command usage line cannot be empty")
	}
	if !strings.HasPrefix(usageLine, "sidekick") {
		panic(fmt.Sprintf("command usage line must start with sidekick, got: %s", usageLine))
	}
	if len(shortDescription) == 0 {
		panic("command short description cannot be empty")
	}
	if parent == nil {
		panic("command must have a parent")
	}
	c := newCommand(usageLine, shortDescription, parent)
	c.longDescription = longDescription
	c.action = action
	return c
}

func newCommand(usageLine string, shortDescription string, parent *Command) *Command {
	c := &Command{
		usageLine:        usageLine,
		altNames:         []string{},
		shortDescription: shortDescription,
		flags:            flag.NewFlagSet(usageLine, flag.ContinueOnError),
		commands:         []*Command{},
	}

	// the only command that is valid without a parent is `sidekick`
	if parent != nil {
		parent.commands = append(parent.commands, c)
	}
	c.parent = parent

	c.flags.Usage = func() {
		_ = c.PrintUsage()
	}
	return c
}
