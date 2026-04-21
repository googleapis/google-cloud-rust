// Copyright 2026 Google LLC
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

use serde_json::Value;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::process::Command;

/// Internal structure tracking package metadata retrieved via cargo.
struct PackageInfo {
    /// The name of the package.
    name: String,
    /// Full path to the package's Cargo.toml file.
    manifest_path: String,
    /// True if the package can be published (publish != []).
    publish: bool,
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        eprintln!("Usage: {} [package_name...]", args[0]);
        eprintln!("   Checks for potential duplicate documentation.");
        eprintln!("   If no packages are specified, checks all relevant crates in the workspace.");
        return Ok(());
    }

    let target_packages: Vec<&str> = args.iter().skip(1).map(|s| s.as_str()).collect();
    let (workspace_packages, target_dir) = get_workspace_packages()?;

    // Validate user-supplied package list, if any.
    let invalid_packages: Vec<_> = target_packages
        .iter()
        .copied()
        .filter(|&target| !workspace_packages.iter().any(|p| p.name == target))
        .collect();

    if !invalid_packages.is_empty() {
        for name in invalid_packages {
            eprintln!("Error: Package '{}' not found in workspace.", name);
        }
        std::process::exit(1);
    }

    let final_packages: Vec<String> = if !target_packages.is_empty() {
        target_packages.iter().map(|&s| s.to_string()).collect()
    } else {
        workspace_packages
            .into_iter()
            .filter(|p| {
                !p.manifest_path.contains("src/generated")
                    && !p.manifest_path.contains("tests/")
                    && p.publish
            })
            .map(|p| p.name)
            .collect()
    };

    generate_and_validate_docs(&final_packages, target_dir.as_str())
}

/// Queries the workspace metadata using the `cargo_metadata` crate.
///
/// Returns a list of extracted `PackageInfo` definitions and the workspace's canonical target directory.
fn get_workspace_packages() -> Result<(Vec<PackageInfo>, String), Box<dyn std::error::Error>> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let target_dir = metadata.target_directory.to_string();

    let mut results = Vec::new();
    for p in metadata.packages {
        if metadata.workspace_members.contains(&p.id) {
            let publish = if let Some(pub_val) = &p.publish {
                !pub_val.is_empty()
            } else {
                true
            };

            results.push(PackageInfo {
                name: p.name.to_string(),
                manifest_path: p.manifest_path.to_string(),
                publish,
            });
        }
    }

    Ok((results, target_dir))
}

/// Executes `rustdoc --output-format json` across target packages sequentially and evaluates them.
fn generate_and_validate_docs(package_names: &[String], target_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    if package_names.is_empty() {
        println!("No packages to check.");
        return Ok(());
    }

    println!("Checking packages: {:?}", package_names);

    let mut total_errors = 0;
    let doc_dir = std::path::PathBuf::from(target_dir).join("doc");

    for name in package_names {
        println!("\n=== Processing crate: {} ===", name);

        // Run cargo rustdoc
        let status = Command::new("cargo")
            .arg("+nightly")
            .arg("rustdoc")
            .args(&[
                "-p",
                name,
                "--",
                "-Z",
                "unstable-options",
                "--output-format",
                "json",
            ])
            .status()?;

        if !status.success() {
            return Err(format!("Failed to generate docs for {}", name).into());
        }

        // Load generated JSON
        let json_filename = doc_dir.join(format!("{}.json", name.replace("-", "_")));
        let file = File::open(&json_filename).map_err(|e| {
            format!("JSON file not found for crate {} at {:?}: {}", name, json_filename, e)
        })?;

        let reader = BufReader::new(file);
        let doc_v: Value = serde_json::from_reader(reader)?;
        let doc_index = doc_v
            .get("index")
            .and_then(|i| i.as_object())
            .ok_or("No index found in doc JSON")?;

        total_errors += detect_duplicate_reexports(doc_index);
    }

    if total_errors > 0 {
        eprintln!(
            "\nError: Found {} instances of duplicate documentation.",
            total_errors
        );
        std::process::exit(1);
    }

    Ok(())
}

/// Analyzes deserialized rustdoc JSON index for document overlaps on crate re-exports (`pub use`).
fn detect_duplicate_reexports(index: &serde_json::Map<String, Value>) -> i32 {
    let mut error_count = 0;

    for (id, item) in index {
        let docs = item.get("docs").and_then(|d| d.as_str()).unwrap_or("");
        if docs.is_empty() {
            continue;
        }

        // We only care about re-exports
        let Some(inner_use) = item.get("inner").and_then(|inr| inr.get("use")) else {
            continue;
        };

        let target_id = inner_use
            .get("id")
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .or_else(|| v.as_i64().map(|n| n.to_string()))
                    .unwrap_or_default()
            })
            .unwrap_or_default();

        if target_id.is_empty() {
            continue;
        }

        let name = inner_use.get("name").and_then(|n| n.as_str()).unwrap_or("unnamed");
        let source = inner_use.get("source").and_then(|s| s.as_str()).unwrap_or("unknown");

        println!("\nFound documented re-export: {} (source: {}, ID: {})", name, source, id);

        if let Some(target_item) = index.get(&target_id) {
            let target_docs = target_item.get("docs").and_then(|d| d.as_str()).unwrap_or("");
            if !target_docs.is_empty() && !docs.ends_with("\n\n") {
                println!("  ERROR: Both re-export and target have documentation, and re-export docs do not end with double newline!");
                println!("    Re-export docs: {}", docs);
                println!("    Target docs: {}", target_docs);
                error_count += 1;
            }
        } else if !docs.ends_with("\n\n") {
            println!("  WARNING: Documented re-export of external item (target ID {} not in index), and re-export docs do not end with double newline.", target_id);
            println!("    Potential duplicate if target has docs.");
            println!("    Re-export docs: {}", docs);
        }
    }

    error_count
}
