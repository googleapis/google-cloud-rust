// Copyright 2024 Google LLC
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

use anyhow::Error;
use rand::{Rng, distr::Alphanumeric};
use storage_samples::RandomChars;

pub type Result<T> = anyhow::Result<T>;
pub mod aiplatform;
pub mod bigquery;
pub mod compute;
pub mod error_details;
pub mod firestore;
#[cfg(google_cloud_unstable_tracing)]
pub mod observability;
pub mod pubsub;
pub mod secret_manager;
pub mod showcase;
pub mod storage;
pub mod workflows;
pub mod workflows_executions;

use storage_samples::random_bucket_id;

pub const SECRET_ID_LENGTH: usize = 64;

pub const VM_ID_LENGTH: usize = 63;

pub const WORKFLOW_ID_LENGTH: usize = 64;

/// Returns the project id used for the integration tests.
pub fn project_id() -> Result<String> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    Ok(project_id)
}

/// Returns an existing, but disabled service account.
pub fn test_service_account() -> Result<String> {
    let value = std::env::var("GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT")?;
    Ok(value)
}

/// Returns the preferred region id used for the integration tests.
pub fn region_id() -> String {
    std::env::var("GOOGLE_CLOUD_RUST_TEST_REGION")
        .ok()
        .unwrap_or("us-central1".to_string())
}

/// Returns the preferred zone id used for the integration tests.
pub fn zone_id() -> String {
    std::env::var("GOOGLE_CLOUD_RUST_TEST_REGION")
        .ok()
        .unwrap_or("us-central1-a".to_string())
}

pub fn report_error(e: anyhow::Error) -> anyhow::Error {
    eprintln!("\n\nERROR {e:?}\n");
    tracing::error!("ERROR {e:?}");
    e
}

pub(crate) fn random_workflow_id() -> String {
    // Workflow ids must start with a letter, we use `wf-` as a prefix to
    // meet this requirement.
    const PREFIX: &str = "wf-";
    let workflow_id: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(WORKFLOW_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{workflow_id}")
}

pub(crate) fn random_image_name() -> String {
    const PREFIX: &str = "img-";
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let vm_id: String = rand::rng()
        .sample_iter(&RandomChars::new(CHARSET))
        .take(VM_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{vm_id}")
}

pub fn random_vm_id() -> String {
    const PREFIX: &str = "vm-";
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let vm_id: String = rand::rng()
        .sample_iter(&RandomChars::new(CHARSET))
        .take(VM_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{vm_id}")
}

pub(crate) fn random_vm_prefix(len: usize) -> String {
    const PREFIX: &str = "vm-";
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let vm_id: String = rand::rng()
        .sample_iter(&RandomChars::new(CHARSET))
        .take(len)
        .map(char::from)
        .collect();
    format!("{PREFIX}{vm_id}")
}

pub fn enable_tracing() -> tracing::subscriber::DefaultGuard {
    use tracing_subscriber::fmt::format::FmtSpan;
    let builder = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_max_level(tracing::Level::WARN);
    #[cfg(feature = "log-integration-tests")]
    let builder = builder.with_max_level(tracing::Level::INFO);
    let subscriber = builder.finish();

    tracing::subscriber::set_default(subscriber)
}
