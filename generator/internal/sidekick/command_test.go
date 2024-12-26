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
	"testing"
)

var (
	rootCmd       = newCommand("sidekick", "root command", "", nil, nil)
	child1Cmd     = newCommand("sidekick child1", "child command", "", rootCmd, nil).addAltName("ch1")
	grandchildCmd = newCommand("sidekick child1 grandchild", "grandchild command", "", child1Cmd, nil)
	child2Cmd     = newCommand("sidekick child2", "another child command", "", rootCmd, nil)
)

func TestLookupFindsRootCommand(t *testing.T) {
	cmd, found, _ := rootCmd.lookup([]string{})
	if !found {
		t.Fatal("couldn't find root command")
	}
	if cmd != rootCmd {
		t.Fatalf("found the wrong command %v", cmd)
	}
}

func TestLookupFindsChildCommand(t *testing.T) {
	cmd, found, _ := rootCmd.lookup([]string{"child1"})
	if !found {
		t.Fatal("couldn't find child command")
	}
	if cmd != child1Cmd {
		t.Fatalf("found the wrong command %v", cmd)
	}
}

func TestLookupFindsCommandByAltName(t *testing.T) {
	cmd, found, _ := rootCmd.lookup([]string{"ch1"})
	if !found {
		t.Fatal("couldn't find child command by alternative name")
	}
	if cmd != child1Cmd {
		t.Fatalf("found the wrong command %v", cmd)
	}
}

func TestLookupFindsGrandChildCommand(t *testing.T) {
	cmd, found, _ := rootCmd.lookup([]string{"child1", "grandchild"})
	if !found {
		t.Fatal("couldn't find child command")
	}
	if cmd != grandchildCmd {
		t.Fatalf("found the wrong command %v", cmd)
	}
}

func TestLookupReturnsFalseWhenNoMatch(t *testing.T) {
	cmd, found, args := rootCmd.lookup([]string{"child2", "bad-param"})
	if found {
		t.Fatal("expected lookup to return false")
	}
	if cmd != child2Cmd {
		t.Fatalf("lookup should return the last matching command in the hierarchy, not %v", cmd)
	}

	if len(args) != 1 || args[0] != "bad-param" {
		t.Fatalf("expected to find one argument, got %v", args)
	}
}

func TestLookupStopsOnFirstFlag(t *testing.T) {
	cmd, found, args := rootCmd.lookup([]string{"child2", "-flag", "value", "not-a-flag"})
	if !found {
		t.Fatal("expected lookup to return true")
	}
	if cmd != child2Cmd {
		t.Fatalf("lookup should return child2, not %v", cmd)
	}

	if len(args) != 3 {
		t.Fatalf("expected lookup to return all args after first flag, got %v", args)
	}
}
