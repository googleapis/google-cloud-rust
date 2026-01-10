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

pub fn has_default_crypto_provider() -> anyhow::Result<()> {
    let metadata = metadata()?;
    let features = find_reqwest_features(&metadata)?;
    if !features.contains(&FeatureName::new("rustls-tls".to_string())) {
        bail!("reqwest should have rustls-tls enabled")
    }
    let features = find_rustls_features(&metadata)?;
    if !features.contains(&FeatureName::new("ring".to_string())) {
        bail!("rustls should have ring enabled")
    }
    let _id = find_dependency(&metadata, "ring", Version::new(0, 17, 0))?;
    let id = find_dependency(&metadata, "aws-lc-rs", Version::new(1, 0, 0));
    if id.is_ok() {
        bail!("aws-lc-rs should not be a required dependency")
    }
    Ok(())
}

// TODO(#4170) - make this function verify that no crypto provided dependency
//   is linked.
pub fn no_default_crypto_provider() -> anyhow::Result<()> {
    let result = has_default_crypto_provider();
    if result.is_ok() {
        bail!("default crypto provider found")
    }
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
        bail!("jsonwebtoken should have rust_crypto enabled")
    }
    Ok(())
}

/// This function returns an error if the jsonwebtoken crate is not configured
/// with the `aws-lc-rs` backend.
pub fn idtoken_has_aws_lc_rs_backend() -> anyhow::Result<()> {
    let metadata = metadata()?;
    let features = find_jsonwebtoken_features(&metadata)?;
    if !features.contains(&FeatureName::new("aws_lc_rs".to_string())) {
        bail!("jsonwebtoken should have aws_lc_rs enabled")
    }
    Ok(())
}

fn metadata() -> anyhow::Result<Metadata> {
    let metadata = MetadataCommand::new().exec()?;
    Ok(metadata)
}

fn find_reqwest_features(metadata: &Metadata) -> anyhow::Result<Vec<FeatureName>> {
    find_dependency_features(metadata, "reqwest", Version::new(0, 12, 0))
}

fn find_rustls_features(metadata: &Metadata) -> anyhow::Result<Vec<FeatureName>> {
    find_dependency_features(metadata, "rustls", Version::new(0, 23, 0))
}

fn find_jsonwebtoken_features(metadata: &Metadata) -> anyhow::Result<Vec<FeatureName>> {
    find_dependency_features(metadata, "jsonwebtoken", Version::new(10, 0, 0))
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
