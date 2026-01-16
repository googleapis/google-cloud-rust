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

use anyhow::bail;
use cargo_metadata::{FeatureName, Metadata, MetadataCommand, PackageId, semver::Version};

const RING_CRATE_NAME: &str = "ring";
const AWS_LC_RS_CRATE_NAME: &str = "aws-lc-rs";
// TODO(#4170) - will become "default-tls" with reqwest 0.13.0
const REQWEST_DEFAULT_FEATURE: &str = "rustls-tls";
// TODO(#4170) - will become aws-lc-rs
const RUSTLS_DEFAULT_FEATURE: &str = "ring";

pub fn has_default_crypto_provider(cargo: &str, dir: &str) -> anyhow::Result<()> {
    let metadata = metadata()?;
    let features = find_reqwest_features(&metadata)?;
    if !features.contains(&FeatureName::new(REQWEST_DEFAULT_FEATURE.to_string())) {
        bail!("reqwest should have {REQWEST_DEFAULT_FEATURE} enabled")
    }
    let features = find_rustls_features(&metadata)?;
    if !features.contains(&FeatureName::new(RUSTLS_DEFAULT_FEATURE.to_string())) {
        bail!("rustls should have {RUSTLS_DEFAULT_FEATURE} enabled")
    }
    only_ring(cargo, dir)
}

pub fn only_aws_lc_rs(cargo: &str, dir: &str) -> anyhow::Result<()> {
    use std::process::Stdio;
    let output = std::process::Command::new(cargo)
        .args(["tree"])
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()?;
    if !output.status.success() {
        bail!("cargo tree failed: {output:?}")
    }
    let stdout = String::try_from(output.stdout)?;
    // TODO(#4170) - enable this code
    // if stdout.contains(format!(" {RING_CRATE_NAME} ").as_str())
    // {
    //     bail!("{RING_CRATE_NAME} should **not** be a dependency")
    // }
    if !stdout.contains(format!(" {AWS_LC_RS_CRATE_NAME} ").as_str()) {
        bail!(
            "{AWS_LC_RS_CRATE_NAME} should be a dependency: {}",
            env!("CARGO_MANIFEST_DIR")
        )
    }
    Ok(())
}

pub fn only_ring(cargo: &str, dir: &str) -> anyhow::Result<()> {
    use std::process::Stdio;
    let output = std::process::Command::new(cargo)
        .args(["tree"])
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()?;
    if !output.status.success() {
        bail!("cargo tree failed: {output:?}")
    }
    let stdout = String::try_from(output.stdout)?;
    if !stdout.contains(format!(" {RING_CRATE_NAME} ").as_str()) {
        bail!("{RING_CRATE_NAME} should be a dependency")
    }
    // TODO(#4170) - enable this code
    // if stdout.contains(format!(" {AWS_LC_RS_CRATE_NAME} ").as_str())
    // {
    //     bail!("{AWS_LC_RS_CRATE_NAME} should **not** be a dependency")
    // }
    Ok(())
}

/// This function returns an error if the jsonwebtoken crate is not configured
/// with the default backend.
pub fn idtoken_has_default_backend() -> anyhow::Result<()> {
    idtoken_has_rust_crypto_backend()
}

/// This function returns an error if the jsonwebtoken crate is not configured
/// with the `rust_crypto` backend.
pub fn idtoken_has_rust_crypto_backend() -> anyhow::Result<()> {
    let metadata = metadata()?;
    let features = find_jsonwebtoken_features(&metadata)?;
    if !features.contains(&FeatureName::new("rust_crypto".to_string())) {
        bail!("jsonwebtoken should have rust_crypto enabled: {features:?}")
    }
    if features.contains(&FeatureName::new("aws_lc_rs".to_string())) {
        // The jsonwebtoken library would not compile if both features are
        // enabled, but it does not hurt to test.
        bail!("jsonwebtoken should **not** have aws_lc_rs enabled: {features:?}")
    }
    Ok(())
}

/// This function returns an error if the jsonwebtoken crate is not configured
/// with the `aws-lc-rs` backend.
pub fn idtoken_has_aws_lc_rs_backend() -> anyhow::Result<()> {
    let metadata = metadata()?;
    let features = find_jsonwebtoken_features(&metadata)?;
    if !features.contains(&FeatureName::new("aws_lc_rs".to_string())) {
        bail!("jsonwebtoken should have aws_lc_rs enabled: {features:?}")
    }
    if features.contains(&FeatureName::new("rust_crypto".to_string())) {
        // The jsonwebtoken library would not compile if both features are
        // enabled, but it does not hurt to test.
        bail!("jsonwebtoken should **not** have rust_crypto enabled: {features:?}")
    }
    Ok(())
}

fn metadata() -> anyhow::Result<Metadata> {
    let metadata = MetadataCommand::new().exec()?;
    Ok(metadata)
}

fn reqwest_version(_metadata: &Metadata) -> Version {
    // TODO(4170) - we would like to read this version from the top-level
    //   workspace metadata.
    Version::new(0, 12, 0)
}

fn rustls_version(_metadata: &Metadata) -> Version {
    // TODO(4170) - we would like to read this version from the top-level
    //   workspace metadata.
    Version::new(0, 23, 0)
}

fn jsonwebtoken_version(_metadata: &Metadata) -> Version {
    // TODO(4170) - we would like to read this version from the top-level
    //   workspace metadata.
    Version::new(10, 0, 0)
}

fn find_reqwest_features(metadata: &Metadata) -> anyhow::Result<Vec<FeatureName>> {
    find_dependency_features(metadata, "reqwest", reqwest_version(metadata))
}

fn find_rustls_features(metadata: &Metadata) -> anyhow::Result<Vec<FeatureName>> {
    find_dependency_features(metadata, "rustls", rustls_version(metadata))
}

fn find_jsonwebtoken_features(metadata: &Metadata) -> anyhow::Result<Vec<FeatureName>> {
    find_dependency_features(metadata, "jsonwebtoken", jsonwebtoken_version(metadata))
}

fn find_dependency(metadata: &Metadata, name: &str, version: Version) -> anyhow::Result<PackageId> {
    let matches = metadata
        .packages
        .iter()
        .filter_map(|p| {
            if p.name == name && p.version >= version {
                Some(p.id.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    match &matches[..] {
        [id] => Ok(id.clone()),
        [] => bail!("no matches for package {name}@{version:?}"),
        _ => bail!("too many matches for package {name}@{version:?}"),
    }
}

fn find_dependency_features(
    metadata: &Metadata,
    name: &str,
    version: Version,
) -> anyhow::Result<Vec<FeatureName>> {
    let id = find_dependency(metadata, name, version)?;
    let root = metadata
        .resolve
        .as_ref()
        .expect("metadata has resolved nodes");
    let features = root
        .nodes
        .iter()
        .find(|n| n.id == id)
        .map(|n| n.features.clone())
        .unwrap_or_default();
    Ok(features)
}
