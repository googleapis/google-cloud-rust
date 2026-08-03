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
use std::fs::{self, File};
use std::path::Path;
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

fn parse_librarian_yaml(path: &Path) -> anyhow::Result<Vec<Library>> {
    let file = File::open(path)?;
    let config: LibrarianConfig = serde_yaml::from_reader(file)?;
    Ok(config.libraries)
}

fn parse_root_cargo_deps(path: &Path) -> anyhow::Result<HashMap<String, String>> {
    let content = fs::read_to_string(path)?;
    let doc = content.parse::<DocumentMut>()?;
    Ok(cargo_deps(doc))
}

fn cargo_deps(doc: DocumentMut) -> HashMap<String, String> {
    let Some(deps) = doc
        .get("workspace")
        .and_then(|w| w.as_table_like())
        .and_then(|wt| wt.get("dependencies"))
        .and_then(|d| d.as_table_like())
    else {
        return HashMap::new();
    };

    deps.iter()
        .filter_map(|(key, value)| {
            if let Some(dep_table) = value.as_table_like() {
                if let Some(ver) = dep_table.get("version").and_then(|v| v.as_str()) {
                    return Some((key.to_string(), ver.to_string()));
                }
            } else if let Some(ver) = value.as_str() {
                return Some((key.to_string(), ver.to_string()));
            }
            None
        })
        .collect()
}

fn check_version_mismatches(
    libraries: &[Library],
    root_deps: &HashMap<String, String>,
    ws_packages: &HashMap<&str, &cargo_metadata::Package>,
    workspace_root: &Path,
) -> Vec<String> {
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
                let rel_path = pkg
                    .manifest_path
                    .as_std_path()
                    .strip_prefix(workspace_root)
                    .unwrap_or(pkg.manifest_path.as_std_path());
                mismatches.push(format!(
                    "  - {}: expected {expected_version}, got {pkg_version} in Cargo.toml ({})",
                    name,
                    rel_path.display()
                ));
            }
        } else {
            println!(
                "Warning: Library '{name}' listed in librarian.yaml is not a package in the workspace."
            );
        }

        // 2. Check package version in root Cargo.toml workspace.dependencies
        if let Some(root_ver) = root_deps.get(name).filter(|&v| v != expected_version) {
            mismatches.push(format!(
                "  - {name}: expected {expected_version}, got {root_ver} in root Cargo.toml [workspace.dependencies]"
            ));
        }
    }

    mismatches
}

/// Validates that publishable workspace crates specify explicit version requirements
/// for all non-dev workspace dependencies.
///
/// Crates.io does not permit path-only dependencies on published crates:
/// <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#local-paths-in-published-crates>
fn check_path_only_workspace_dependencies(
    ws_packages: &HashMap<&str, &cargo_metadata::Package>,
    workspace_root: &Path,
) -> Vec<String> {
    let mut errors = Vec::new();

    for pkg in ws_packages.values() {
        // Skip unpublishable packages (e.g. publish = false -> publish = Some([]))
        let is_publishable = pkg.publish.as_ref().is_none_or(|p| !p.is_empty());
        if !is_publishable {
            continue;
        }

        let rel_manifest_path = pkg
            .manifest_path
            .as_std_path()
            .strip_prefix(workspace_root)
            .unwrap_or(pkg.manifest_path.as_std_path());

        for dep in &pkg.dependencies {
            // Ignore dev-dependencies as crates.io permits path-only dev-dependencies.
            if dep.kind == cargo_metadata::DependencyKind::Development {
                continue;
            }

            // Check if dependency refers to a local workspace package
            let target_pkg_name = dep.name.as_str();
            let is_workspace_dep = ws_packages.contains_key(target_pkg_name)
                || dep
                    .path
                    .as_ref()
                    .is_some_and(|p| p.starts_with(workspace_root));

            if !is_workspace_dep {
                continue;
            }

            let dep_alias = dep.rename.as_deref().unwrap_or(&dep.name);

            // 1. Check if version requirement is missing (path-only / wildcard STAR)
            if dep.req == cargo_metadata::semver::VersionReq::STAR {
                errors.push(format!(
                    "  - {}: publishable package '{}' depends on workspace package '{}' (as '{}') without a version requirement in Cargo.toml ({})",
                    pkg.name,
                    pkg.name,
                    target_pkg_name,
                    dep_alias,
                    rel_manifest_path.display()
                ));
            }

            // 2. Check if dependency targets an unpublishable workspace package (publish = false)
            if let Some(target_pkg) = ws_packages.get(target_pkg_name) {
                let target_is_publishable =
                    target_pkg.publish.as_ref().is_none_or(|p| !p.is_empty());
                if !target_is_publishable {
                    errors.push(format!(
                        "  - {}: publishable package '{}' depends on unpublishable workspace package '{}' (as '{}') in Cargo.toml ({})",
                        pkg.name,
                        pkg.name,
                        target_pkg_name,
                        dep_alias,
                        rel_manifest_path.display()
                    ));
                }
            }
        }
    }

    errors
}

fn main() -> anyhow::Result<()> {
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

    let path_only_errors =
        check_path_only_workspace_dependencies(&ws_packages, metadata.workspace_root.as_std_path());

    let mismatches = check_version_mismatches(
        &libraries,
        &root_deps,
        &ws_packages,
        metadata.workspace_root.as_std_path(),
    );

    let mut has_errors = false;

    if !path_only_errors.is_empty() {
        has_errors = true;
        eprintln!("\nFound path-only workspace dependencies in publishable crates:");
        for e in path_only_errors {
            eprintln!("{e}");
        }
        eprintln!(
            "\nAll workspace dependencies in publishable crates must specify a version requirement in Cargo.toml."
        );
        eprintln!(
            "See: https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#local-paths-in-published-crates"
        );
    }

    if !mismatches.is_empty() {
        has_errors = true;
        eprintln!("\nFound version mismatches:");
        for m in mismatches {
            eprintln!("{m}");
        }
        eprintln!("\nUse librarian to change versions of a library.");
    }

    if has_errors {
        std::process::exit(1);
    }

    println!("\nAll versions and dependency requirements match perfectly!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn unpack_txtar(txtar_content: &str, target_dir: &Path) {
        let mut current_file: Option<PathBuf> = None;
        let mut current_content = String::new();

        for line in txtar_content.lines() {
            if line.starts_with("-- ") && line.ends_with(" --") {
                if let Some(path) = current_file.take() {
                    let full_path = target_dir.join(path);
                    fs::create_dir_all(full_path.parent().unwrap()).unwrap();
                    fs::write(full_path, &current_content).unwrap();
                    current_content.clear();
                }
                let rel_path = line[3..line.len() - 3].trim();
                current_file = Some(PathBuf::from(rel_path));
            } else if current_file.is_some() {
                current_content.push_str(line);
                current_content.push('\n');
            }
        }
        if let Some(path) = current_file {
            let full_path = target_dir.join(path);
            fs::create_dir_all(full_path.parent().unwrap()).unwrap();
            fs::write(full_path, &current_content).unwrap();
        }
    }

    #[test]
    fn test_check_path_only_workspace_dependencies_with_txtar() {
        let txtar_content = include_str!("../testdata/workspace.txtar");
        let dir = tempfile::tempdir().unwrap();
        unpack_txtar(txtar_content, dir.path());

        // Verify parse_root_cargo_deps on testdata root Cargo.toml
        let root_deps = parse_root_cargo_deps(&dir.path().join("Cargo.toml")).unwrap();
        assert_eq!(
            root_deps.get("ws-dep-versioned"),
            Some(&"1.0.0".to_string())
        );
        assert_eq!(root_deps.get("serde"), Some(&"1.0".to_string()));
        assert_eq!(root_deps.get("ws-dep-path-only"), None);
        assert_eq!(
            root_deps.get("unpublishable-pkg"),
            Some(&"0.1.0".to_string())
        );

        // Run cargo_metadata on testdata workspace
        let metadata = cargo_metadata::MetadataCommand::new()
            .current_dir(dir.path())
            .exec()
            .unwrap();

        let mut ws_packages = HashMap::new();
        for pkg in metadata.workspace_packages() {
            ws_packages.insert(pkg.name.as_str(), pkg);
        }

        let errors = check_path_only_workspace_dependencies(&ws_packages, dir.path());

        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|e| {
            e.contains("publishable-pkg")
                && e.contains("ws-dep-path-only")
                && e.contains("without a version requirement")
        }));
        assert!(errors.iter().any(|e| {
            e.contains("publishable-pkg")
                && e.contains("unpublishable-pkg")
                && e.contains("unpublishable workspace package")
        }));
    }
}
