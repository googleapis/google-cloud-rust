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

// Name returns the command's short name: the last word in the usage line before a flag or argument.
func (c *Command) Name() string {
	name := c.LongName()
	if i := strings.LastIndex(name, " "); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// LongName returns the command's long name: all the words in the usage line before a flag or argument,
func (c *Command) LongName() string {
	name := c.usageLine
	if i := strings.Index(name, " ["); i >= 0 {
		name = name[:i]
	}
	return strings.TrimSpace(name)
}

// AddAltName adds an alternative name to the command. These alternative names are used for the Lookup function.
func (c *Command) AddAltName(n string) *Command {
	c.altNames = append(c.altNames, n)
	return c
}

// Names returns all the names of the command, including the main name declared in the usage line, and any alternative names.
func (c *Command) Names() []string {
	names := []string{c.Name()}
	names = append(names, c.altNames...)
	return names
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
	var flags []*flag.Flag
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
	if c.action == nil {
		return fmt.Errorf("command %s is not runnable", c.LongName())
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

	c := &Command{
		usageLine:        usageLine,
		altNames:         []string{},
		shortDescription: shortDescription,
		longDescription:  longDescription,
		action:           action,
		flags:            flag.NewFlagSet(usageLine, flag.ContinueOnError),
		commands:         []*Command{},
		parent:           parent,
	}

	if parent != nil {
		parent.commands = append(parent.commands, c)
	} else if c.Name() != "sidekick" {
		panic("Only the sidekick root command can have a nil parent")
	}

	c.flags.Usage = func() {
		c.PrintUsage()
	}

	return c
}

// PrintUsage prints the usage of the command to os.Stdout, following the same logic as the usageTemplate, but using standatd fmt.Println statements instead.
func (c *Command) PrintUsage() {

	// first prints the entire Long Description for the given command
	fmt.Println(c.LongDescription())
	fmt.Printf("\n")

	// Then prints the usage line, skipping over <command> if there are no sub-commands, and [flags] if there are no flags
	fmt.Printf("Usage:\n")
	fmt.Printf("    %s", c.LongName())
	if len(c.Commands()) > 0 {
		fmt.Printf(" <command>")
	}
	if len(c.Flags()) > 0 {
		fmt.Printf(" [flags]")
	}
	fmt.Printf("\n\n")

	// if this command supports sub-commands, prints their names, along with their short descriptions
	if len(c.Commands()) > 0 {
		fmt.Println("The commands are:")
		for _, sub := range c.Commands() {
			fmt.Printf("%s%-15s %s\n", strings.Repeat(" ", 4), sub.Name(), sub.ShortDescription())
		}
		fmt.Printf("\n\n")
	}

	// if this command supports any flags, prints their names and usage
	if len(c.Flags()) > 0 {
		fmt.Println("The flags are:")
		for _, f := range c.Flags() {
			fmt.Printf("%s-%-25s%s\n", strings.Repeat(" ", 4), f.Name, f.Usage)
			if f.DefValue != "" {
				fmt.Printf("%sDefault Value: %s\n", strings.Repeat(" ", 4+1+25), f.DefValue)
			}
		}
		fmt.Printf("\n\n")
	}

	// if the command supports sub-commands, prints a note about how to get more information about a specific sub-command
	if len(c.Commands()) > 0 {
		helpSuffix := strings.TrimSpace(strings.TrimPrefix(c.LongName(), "sidekick"))
		if len(helpSuffix) > 0 {
			helpSuffix = " " + helpSuffix
		}
		fmt.Printf("Use \"sidekick help%s <command>\" for more information about a command.\n", helpSuffix)
	}
}
