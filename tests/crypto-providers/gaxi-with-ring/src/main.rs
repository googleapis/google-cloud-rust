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

use rustls::crypto::{CryptoProvider, ring::default_provider};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    test_metadata::only_ring(env!("CARGO"), env!("CARGO_MANIFEST_DIR"), true)?;

    // Install a default crypto provider and verify gax-internal works.
    CryptoProvider::install_default(default_provider())
        .map_err(|p| anyhow::anyhow!("default provider was already installed: {p:?}"))?;
    test_gaxi::run().await
}
