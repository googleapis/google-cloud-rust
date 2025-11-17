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

use anyhow::{bail, Result};
use clap::Parser;
use semver::{Version, VersionReq};
use std::{collections::HashMap, fs};
use toml_edit::Document;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Prepares the CI environment by removing path dependencies, checking versions,
    /// and generating a minimal patch file for unpublished crates.
    Prep {
        /// Backup changed files so they can be restored by running `revert` after testing.
        #[arg(long)]
        local: bool,
    },
    /// Restores the original Cargo.toml from the backup.
    Revert,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Prep { local } => prep(local),
        Commands::Revert => revert(),
    }
}

struct Package {
    version: String,
    path: String,
}

fn prep(is_local: bool) -> Result<()> {
    println!("Running prep step");

    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let root_manifest_path = metadata.workspace_root.join("Cargo.toml");
    let mut root_manifest = fs::read_to_string(&root_manifest_path)?.parse::<Document>()?;

    if is_local {
        // 1. Backup original files
        println!("--local flag detected. Backing up files.");
        let cargo_backup_path = root_manifest_path.with_extension("toml.bak");
        if !cargo_backup_path.exists() {
            fs::copy(&root_manifest_path, &cargo_backup_path)?;
            println!("Backed up Cargo.toml to Cargo.toml.bak");
        }

        let config_path = metadata.workspace_root.join(".cargo/config.toml");
        if config_path.exists() {
            let config_backup_path = config_path.with_extension("toml.bak");
            if !config_backup_path.exists() {
                fs::copy(&config_path, &config_backup_path)?;
                println!("Backed up .cargo/config.toml to .cargo/config.toml.bak");
            }
        }
    }

    // 2. Collect `path` dependencies and their versions from the original manifest
    // and modify `root_manifest` to remove `path` keys
    let mut path_deps = HashMap::new();
    if let Some(deps) = root_manifest
        .get_mut("workspace")
        .and_then(|i| i.as_table_mut())
        .and_then(|w| w.get_mut("dependencies"))
        .and_then(|i| i.as_table_like_mut())
    {
        for (key, value) in deps.iter_mut() {
            if let Some(dep_table) = value.as_table_like_mut() {
                if dep_table.contains_key("path") && dep_table.contains_key("version") {
                    let package_name = dep_table
                        .get("package")
                        .and_then(|v| v.as_str())
                        .unwrap_or(key.get()) // if there is no name specified, use the key.
                        .to_string();
                    path_deps.insert(
                        package_name.clone(),
                        Package {
                            version: dep_table
                                .get("version")
                                .and_then(|v| v.as_str())
                                .unwrap()
                                .to_string(),
                            path: dep_table
                                .get("path")
                                .and_then(|v| v.as_str())
                                .unwrap()
                                .to_string(),
                        },
                    );
                    // Remove from the toml.
                    dep_table.remove("path");
                }
            }
        }
    }

    // 4. Overwrite Cargo.toml with the cleaned version
    fs::write(&root_manifest_path, root_manifest.to_string())?;
    println!("Removed path dependencies from Cargo.toml");

    // 5. Generate Minimal Patch File for unpublished crates
    println!("Querying crates.io for unpublished crates...");
    let client = crates_io_api::SyncClient::new(
        "google-cloud-rust-ci (https://github.com/googleapis/google-cloud-rust)",
        std::time::Duration::from_millis(1000),
    )?;
    let mut patch_content = String::new();
    let mut patched = Vec::new();
    for (name, Package { version, path }) in &path_deps {
        let local_version = VersionReq::parse(version)?;
        match client.get_crate(&name) {
            Ok(crate_info) => {
                if !crate_info.versions.iter().any(|v| {
                    Version::parse(&v.num)
                        .map_or(false, |remote_v| local_version.matches(&remote_v))
                }) {
                    println!("Found unpublished crate: {} v{}", name, version);
                    patched.push(name.clone());
                    patch_content.push_str(&format!("{} = {{ path = \"{}\" }}\n", name, path));
                }
            }
            Err(_) => {
                // If crate doesn't exist at all, it's unpublished
                println!("Found new crate: {} v{}", name, version);
                patch_content.push_str(&format!("{} = {{ path = \"{}\" }}\n", name, path));
            }
        }
    }

    if !patch_content.is_empty() {
        let config_path = metadata.workspace_root.join(".cargo/config.toml");
        fs::create_dir_all(config_path.parent().unwrap())?;
        let final_content = format!("[patch.crates-io]\n{}", patch_content);
        fs::write(&config_path, final_content)?;
        println!("Generated .cargo/config.toml with patches for unpublished crates.");
        // Create sentinel file
        let sentinel_path = config_path.with_extension("toml.generated");
        fs::write(&sentinel_path, "")?;
    } else {
        println!("No unpublished crates found to patch.");
    }

    // 6. Perform Version Consistency Check using the initial metadata
    println!("Checking for version consistency for patched crates...");
    let workspace_packages = metadata.workspace_packages();
    for package_name in patched {
        let root_metadata = path_deps.get(&package_name).unwrap();
        let root_version = &root_metadata.version;

        let package_metadata = workspace_packages
            .iter()
            .find(|p| p.name == package_name)
            .unwrap();
        let package_version = &package_metadata.version;

        let is_match = if package_version.patch == 0 {
            // If patch is 0, root version can be x.y or x.y.0
            let major_minor = format!("{}.{}", package_version.major, package_version.minor);

            if package_version.minor == 0 {
                // If minor is 0, root version can also be x.
                let major = format!("{}", package_version.major);
                root_version == &major
                    || root_version == &major_minor
                    || root_version == &package_version.to_string()
            } else {
                root_version == &major_minor || root_version == &package_version.to_string()
            }
        } else {
            // If patch is not 0, root version must be x.y.z
            root_version == &package_version.to_string()
        };

        if !is_match {
            bail!(
                    "Version mismatch for {}: workspace version is '{}', but crate version is '{}'. This is not a match as per project conventions.",
                    package_name,
                    package_version,
                    package_version
                );
        }
    }
    println!("Version consistency check passed.");

    Ok(())
}

fn revert() -> Result<()> {
    println!("Running revert step");
    let metadata = cargo_metadata::MetadataCommand::new().exec()?;
    let workspace_root = &metadata.workspace_root;

    // Revert Cargo.toml
    let root_manifest_path = workspace_root.join("Cargo.toml");
    let cargo_backup_path = root_manifest_path.with_extension("toml.bak");
    if cargo_backup_path.exists() {
        fs::rename(&cargo_backup_path, &root_manifest_path)?;
        println!("Restored Cargo.toml from backup.");
    } else {
        println!("No Cargo.toml backup file found.");
    }

    // Revert .cargo/config.toml
    let config_path = workspace_root.join(".cargo/config.toml");
    let config_backup_path = config_path.with_extension("toml.bak");
    let sentinel_path = config_path.with_extension("toml.generated");

    if config_backup_path.exists() {
        fs::rename(&config_backup_path, &config_path)?;
        fs::remove_file(&sentinel_path)?;
        println!("Restored .cargo/config.toml from backup.");
    } else if sentinel_path.exists() {
        fs::remove_file(&config_path)?;
        fs::remove_file(&sentinel_path)?;
        println!("Removed generated .cargo/config.toml.");
    } else {
        println!("No .cargo/config.toml backup file found and no generated file to remove.");
    }

    Ok(())
}
