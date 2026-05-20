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

use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use toml_edit::DocumentMut;

#[derive(Debug, Deserialize)]
struct LibrarianConfig {
    libraries: Vec<Library>,
}

#[derive(Debug, Deserialize)]
struct Library {
    name: String,
    version: Option<String>,
}

fn parse_librarian_yaml(path: impl AsRef<std::path::Path>) -> Result<Vec<Library>, Box<dyn Error>> {
    let file = File::open(path)?;
    let config: LibrarianConfig = serde_yaml::from_reader(file)?;
    Ok(config.libraries)
}

fn parse_root_cargo_deps(path: impl AsRef<std::path::Path>) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let doc = content.parse::<DocumentMut>()?;
    let mut dependencies = HashMap::new();

    if let Some(deps) = doc
        .get("workspace")
        .and_then(|w| w.as_table_like())
        .and_then(|wt| wt.get("dependencies"))
        .and_then(|d| d.as_table_like())
    {
        for (key, value) in deps.iter() {
            if let Some(dep_table) = value.as_table_like() {
                if let Some(ver) = dep_table.get("version").and_then(|v| v.as_str()) {
                    dependencies.insert(key.to_string(), ver.to_string());
                }
            } else if let Some(ver) = value.as_str() {
                dependencies.insert(key.to_string(), ver.to_string());
            }
        }
    }

    Ok(dependencies)
}

fn main() -> Result<(), Box<dyn Error>> {
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let workspace_root = metadata.workspace_root.as_std_path();

    let librarian_path = workspace_root.join("librarian.yaml");
    let root_cargo_path = workspace_root.join("Cargo.toml");

    println!("Parsing librarian.yaml...");
    let libraries = parse_librarian_yaml(&librarian_path)?;
    println!("Found {} libraries in librarian.yaml.", libraries.len());

    println!("Parsing root Cargo.toml...");
    let root_deps = parse_root_cargo_deps(&root_cargo_path)?;

    let workspace_packages = metadata.workspace_packages();
    let mut ws_packages = HashMap::new();
    for pkg in workspace_packages {
        ws_packages.insert(pkg.name.as_str(), pkg);
    }

    let mut mismatches = Vec::new();

    for lib in libraries {
        let name = &lib.name;
        let expected_version = match &lib.version {
            Some(v) => v,
            None => continue, // Skip libraries with no version property
        };

        // 1. Check package version in its own Cargo.toml
        if let Some(pkg) = ws_packages.get(name.as_str()) {
            let pkg_version = pkg.version.to_string();
            if pkg_version != *expected_version {
                let rel_path = pkg.manifest_path.strip_prefix(&metadata.workspace_root).unwrap_or(&pkg.manifest_path);
                mismatches.push(format!(
                    "  - {}: expected {}, got {} in Cargo.toml ({})",
                    name, expected_version, pkg_version, rel_path
                ));
            }
        } else {
            println!("Warning: Library '{}' listed in librarian.yaml is not a package in the workspace.", name);
        }

        // 2. Check package version in root Cargo.toml workspace.dependencies
        if let Some(root_ver) = root_deps.get(name) {
            if root_ver != expected_version {
                mismatches.push(format!(
                    "  - {}: expected {}, got {} in root Cargo.toml [workspace.dependencies]",
                    name, expected_version, root_ver
                ));
            }
        }
    }

    if !mismatches.is_empty() {
        eprintln!("\nFound version mismatches:");
        for m in mismatches {
            eprintln!("{}", m);
        }
        eprintln!("\nUse librarian to change versions of a library.");
        std::process::exit(1);
    } else {
        println!("\nAll versions match perfectly!");
        Ok(())
    }
}
