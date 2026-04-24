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
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "--workspace" {
        return run_workspace_mode();
    }

    if args.len() < 2 {
        eprintln!("Usage: {} <rustdoc-json-file>", args[0]);
        eprintln!("   Or: {} --workspace", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let v: Value = serde_json::from_reader(reader)?;

    let index = v
        .get("index")
        .and_then(|i| i.as_object())
        .ok_or("No index found")?;

    println!("Scanning for potential duplicate documentation...");
    let error_count = check_docs(index);

    if error_count > 0 {
        eprintln!(
            "\nError: Found {} instances of duplicate documentation.",
            error_count
        );
        std::process::exit(1);
    }

    Ok(())
}

fn run_workspace_mode() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running in workspace mode...");

    let output = Command::new("cargo")
        .arg("metadata")
        .arg("--format-version")
        .arg("1")
        .output()?;

    if !output.status.success() {
        return Err("Failed to run cargo metadata".into());
    }

    let v: Value = serde_json::from_slice(&output.stdout)?;
    let packages = v
        .get("packages")
        .and_then(|p| p.as_array())
        .ok_or("No packages found")?;
    let members = v
        .get("workspace_members")
        .and_then(|m| m.as_array())
        .ok_or("No workspace_members found")?;

    let mut package_map = HashMap::new();
    for p in packages {
        let id = p.get("id").and_then(|i| i.as_str()).unwrap_or("");
        package_map.insert(id, p);
    }

    let mut total_errors = 0;

    for m in members {
        let id = m.as_str().unwrap_or("");
        if let Some(p) = package_map.get(id) {
            let name = p.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let manifest_path = p
                .get("manifest_path")
                .and_then(|m| m.as_str())
                .unwrap_or("");
            let publish = p.get("publish");

            // Filter
            if manifest_path.contains("src/generated") || manifest_path.contains("tests/") {
                continue;
            }
            if let Some(pub_val) = publish {
                if pub_val.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                    continue; // publish = []
                }
            }

            println!("\n=== Processing crate: {} ===", name);

            // Run cargo rustdoc
            let status = Command::new("cargo")
                .arg("+nightly")
                .arg("rustdoc")
                .arg("-p")
                .arg(name)
                .arg("--")
                .arg("-Z")
                .arg("unstable-options")
                .arg("--output-format")
                .arg("json")
                .status()?;

            if !status.success() {
                println!("Warning: Failed to generate docs for {}", name);
                continue;
            }

            // Load generated JSON
            let json_filename = format!("target/doc/{}.json", name.replace("-", "_"));
            let file = File::open(&json_filename);
            if let Ok(f) = file {
                let reader = BufReader::new(f);
                let doc_v: Value = serde_json::from_reader(reader)?;
                let doc_index = doc_v
                    .get("index")
                    .and_then(|i| i.as_object())
                    .ok_or("No index found in doc JSON")?;

                total_errors += check_docs(doc_index);
            } else {
                println!("Warning: JSON file not found: {}", json_filename);
            }
        }
    }

    if total_errors > 0 {
        eprintln!(
            "\nError: Found {} instances of duplicate documentation in workspace.",
            total_errors
        );
        std::process::exit(1);
    }

    Ok(())
}

fn check_docs(index: &serde_json::Map<String, Value>) -> i32 {
    let mut error_count = 0;

    for (id, item) in index {
        let docs = item.get("docs").and_then(|d| d.as_str()).unwrap_or("");

        // Check if it's a re-export (inner has "use")
        if let Some(inner_use) = item.get("inner").and_then(|inr| inr.get("use")) {
            if !docs.is_empty() {
                let name = inner_use
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unnamed");
                let source = inner_use
                    .get("source")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown");
                let target_id = inner_use
                    .get("id")
                    .map(|v| {
                        v.as_str()
                            .map(String::from)
                            .or_else(|| v.as_i64().map(|n| n.to_string()))
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                println!(
                    "\nFound documented re-export: {} (source: {}, ID: {})",
                    name, source, id
                );

                if !target_id.is_empty() {
                    if let Some(target_item) = index.get(&target_id) {
                        let target_docs = target_item
                            .get("docs")
                            .and_then(|d| d.as_str())
                            .unwrap_or("");
                        if !target_docs.is_empty() && !docs.ends_with("\n\n") {
                            println!(
                                "  ERROR: Both re-export and target have documentation, and re-export docs do not end with double newline!"
                            );
                            println!("    Re-export docs: {}", docs);
                            println!("    Target docs: {}", target_docs);
                            error_count += 1;
                        }
                    } else if !docs.ends_with("\n\n") {
                        println!(
                            "  WARNING: Documented re-export of external item (target ID {} not in index), and re-export docs do not end with double newline.",
                            target_id
                        );
                        println!("    Potential duplicate if target has docs.");
                        println!("    Re-export docs: {}", docs);
                    }
                }
            }
        }
    }

    error_count
}
