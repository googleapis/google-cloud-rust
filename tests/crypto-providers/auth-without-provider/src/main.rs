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
use cargo_metadata::{FeatureName, Metadata, MetadataCommand, semver::Version};

fn main() -> anyhow::Result<()> {
    let metadata = MetadataCommand::new().exec()?;
    let auth_features = find_dependency(&metadata, "google-cloud-auth", Version::new(1, 0, 0))?;
    assert!(
        !auth_features.contains(&FeatureName::new("default-crypto-provider".to_string())),
        "`google-cloud-auth` should not have the default crypto-provider enabled, found={auth_features:?}"
    );

    let reqwest_features = find_dependency(&metadata, "reqwest", Version::new(0, 13, 0))?;
    assert!(
        !reqwest_features.contains(&FeatureName::new("rustls".to_string())),
        "`reqwest` should not have the default rustls feature enabled, found={reqwest_features:?}"
    );

    Ok(())
}

fn find_dependency(
    metadata: &Metadata,
    name: &str,
    version: Version,
) -> anyhow::Result<Vec<FeatureName>> {
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
    let id = match &matches[..] {
        [id] => id,
        [] => bail!("no matches for package {name}@{version:?}"),
        _ => bail!("too many matches for package {name}@{version:?}"),
    };
    let root = metadata
        .resolve
        .as_ref()
        .expect("metadata has resolved nodes");
    let features = root
        .nodes
        .iter()
        .find(|n| &n.id == id)
        .map(|n| n.features.clone())
        .unwrap_or_default();
    Ok(features)
}
