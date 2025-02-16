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

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

// command is an implementation of a sidekick command, like 'sidekick generate'.
type command struct {
	action           func(rootConfig *config.Config, cmdLine *CommandLine) error
	usageLine        string
	altNames         []string
	shortDescription string
	longDescription  string
	flags            *flag.FlagSet
	commands         []*command
	parent           *command
}

// name returns the command's short name: the last word in the usage line before a flag or argument.
func (c *command) name() string {
	name := c.longName()
	if i := strings.LastIndex(name, " "); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// longName returns the command's long name: all the words in the usage line before a flag or argument.
func (c *command) longName() string {
	name := c.usageLine
	if i := strings.Index(name, " ["); i >= 0 {
		name = name[:i]
	}
	return strings.TrimSpace(name)
}

// addAltName adds an alternative name to the command. These alternative names are used for the Lookup function.
func (c *command) addAltName(n string) *command {
	c.altNames = append(c.altNames, n)
	return c
}

// names returns all the names of the command, including the main name declared in the usage line,
// and any alternative names.
func (c *command) names() []string {
	return append([]string{c.name()}, c.altNames...)
}

// lookup recursively iterates through the command's sub-commands to find the one that matches the first argument,
// until no arguments are left or a flag is found.
// If an exact match is found, it returns the command, true, and the remaining args.
// If no exact match is found, it returns the last command to match the args, false, and the remaining args.
func (c *command) lookup(args []string) (*command, bool, []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return c, true, args
	}
	for _, sub := range c.commands {
		if slices.Contains(sub.names(), args[0]) {
			return sub.lookup(args[1:])
		}
	}
	return c, false, args
}

// The following addFlag* methods are syntax sugar to enable flags to be added in the same line as the command is created.
// Not all flag types are supported yet, only the ones used in the current implementation.

func (c *command) addFlagBool(p *bool, name string, value bool, usage string) *command {
	c.flags.BoolVar(p, name, value, usage)
	return c
}

func (c *command) addFlagString(p *string, name string, usage string) *command {
	c.flags.StringVar(p, name, "", usage)
	return c
}

func (c *command) addFlagFunc(name string, usage string, fn func(string) error) *command {
	c.flags.Func(name, usage, fn)
	return c
}

// allFlags returns all flags added to this command, as well as its parent hierarchy.
func (c *command) allFlags() []*flag.Flag {
	var flags []*flag.Flag
	c.visitAllFlags(func(f *flag.Flag) {
		flags = append(flags, f)
	})
	return flags
}

// visitAllFlags visits all flags in the command, including those of its parent hierarchy.
func (c *command) visitAllFlags(fn func(f *flag.Flag)) {
	c.flags.VisitAll(fn)
	if c.parent != nil {
		c.parent.visitAllFlags(fn)
	}
}

// run executes the command's action, if it has one.
func (c *command) run(rootConfig *config.Config, cmdLine *CommandLine) error {
	if c.action == nil {
		return fmt.Errorf("command %s is not runnable", c.longName())
	}
	return c.action(rootConfig, cmdLine)
}

// parseCmdLine parses the command line arguments and returns a CommandLine struct.
func (c *command) parseCmdLine(args []string) (*CommandLine, error) {
	if c.parent != nil {
		c.parent.visitAllFlags(func(f *flag.Flag) {
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

func newCommand(
	usageLine string,
	shortDescription string,
	longDescription string,
	parent *command,
	action func(rootConfig *config.Config, cmdLine *CommandLine) error,
) *command {
	if len(usageLine) == 0 {
		panic("command usage line cannot be empty")
	}
	if !strings.HasPrefix(usageLine, "sidekick") {
		panic(fmt.Sprintf("command usage line must start with sidekick, got: %s", usageLine))
	}
	if len(shortDescription) == 0 {
		panic("command short description cannot be empty")
	}

	c := &command{
		usageLine:        usageLine,
		altNames:         []string{},
		shortDescription: shortDescription,
		longDescription:  longDescription,
		action:           action,
		flags:            flag.NewFlagSet(usageLine, flag.ContinueOnError),
		commands:         []*command{},
		parent:           parent,
	}

	if parent != nil {
		parent.commands = append(parent.commands, c)
	} else if c.name() != "sidekick" {
		panic("Only the sidekick root command can have a nil parent")
	}

	c.flags.Usage = func() {
		c.printUsage()
	}

	return c
}

// printUsage prints the usage of the command to os.Stdout.
func (c *command) printUsage() {
	if len(c.longDescription) > 0 {
		fmt.Println(c.longDescription)
	} else {
		fmt.Println(c.shortDescription)
	}
	fmt.Printf("\n")

	fmt.Printf("Usage:\n")
	fmt.Printf("    %s", c.longName())
	if len(c.commands) > 0 {
		fmt.Printf(" <command>")
	}
	if len(c.allFlags()) > 0 {
		fmt.Printf(" [flags]")
	}
	fmt.Printf("\n\n")

	if len(c.commands) > 0 {
		fmt.Println("The commands are:")
		for _, sub := range c.commands {
			fmt.Printf("%s%-15s %s\n", strings.Repeat(" ", 4), sub.name(), sub.shortDescription)
		}
		fmt.Printf("\n\n")
	}

	if len(c.allFlags()) > 0 {
		fmt.Println("The flags are:")
		for _, f := range c.allFlags() {
			fmt.Printf("%s-%-25s%s\n", strings.Repeat(" ", 4), f.Name, f.Usage)
			if f.DefValue != "" {
				fmt.Printf("%sDefault Value: %s\n", strings.Repeat(" ", 4+1+25), f.DefValue)
			}
		}
		fmt.Printf("\n\n")
	}

	if len(c.commands) > 0 {
		helpSuffix := strings.TrimSpace(strings.TrimPrefix(c.longName(), "sidekick"))
		if len(helpSuffix) > 0 {
			helpSuffix = " " + helpSuffix
		}
		fmt.Printf("Use \"sidekick help%s <command>\" for more information about a command.\n", helpSuffix)
	}
}
