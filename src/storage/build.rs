// Copyright 2025 Google LLC
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

use anyhow::Result;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    verify_completeness()?;
    Ok(())
}

/// Verify that the handwritten client and stub expose all available RPCs.
fn verify_completeness() -> Result<()> {
    let storage = list_functions("src/generated/gapic/stub.rs")?;
    let mut control = list_functions("src/generated/gapic_control/stub.rs")?;
    // Filter methods we do not expose in the composite client or stub.
    control.remove("get_polling_error_policy");
    control.remove("get_polling_backoff_policy");
    let expected: HashSet<String> = storage.union(&control).cloned().collect();

    let stub = list_functions("src/control/stub.rs")?;
    let diff: HashSet<String> = expected.difference(&stub).cloned().collect();
    assert!(
        diff.is_empty(),
        "Handwritten stub is missing functions: {diff:?}"
    );

    let client = list_functions("src/control/client.rs")?;
    let diff: HashSet<String> = expected.difference(&client).cloned().collect();
    assert!(
        diff.is_empty(),
        "Handwritten client is missing functions: {diff:?}"
    );

    Ok(())
}

/// Extracts function names from a file
fn list_functions(filepath: &str) -> Result<HashSet<String>> {
    let mut names = HashSet::new();
    let path = Path::new(filepath);
    let content = fs::read_to_string(path)?;

    // Matches lines that start with some number of spaces, followed by a
    // `pub fn` or `fn`. This is good enough for our purposes.
    let re = regex::Regex::new(r"(?m)^ *(pub )?fn ([a-zA-Z0-9_]+)\(")?;
    for c in re.captures_iter(&content) {
        names.insert(c[2].to_string());
    }

    Ok(names)
}
