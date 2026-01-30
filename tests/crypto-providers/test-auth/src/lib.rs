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
use std::time::Duration;

/// Use a public bucket, which does not require authentication, to verify the
/// reqwest crate can connect to a Google Cloud service using TLS.
const PUBLIC_BUCKET_URL: &str =
    "https://storage.googleapis.com/storage/v1/b/gcp-public-data-landsat";

pub async fn run() -> anyhow::Result<()> {
    // Need to create a `Client` so we can set a timeout.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let response = client.get(PUBLIC_BUCKET_URL).send().await?;
    if !response.status().is_success() {
        bail!("error in test request: {:?}", response.status())
    }
    Ok(())
}
