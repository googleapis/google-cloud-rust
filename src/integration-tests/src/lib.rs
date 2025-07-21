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
use rand::{Rng, distr::Alphanumeric, distr::Distribution};

pub type Result<T> = anyhow::Result<T>;
pub mod bigquery;
pub mod error_details;
pub mod firestore;
pub mod secret_manager;
pub mod showcase;
pub mod sql;
pub mod storage;
pub mod workflows;
pub mod workflows_executions;

pub const SECRET_ID_LENGTH: usize = 64;

pub const WORKFLOW_ID_LENGTH: usize = 64;

pub const BUCKET_ID_LENGTH: usize = 63;

/// Returns the project id used for the integration tests.
pub fn project_id() -> Result<String> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    Ok(project_id)
}

/// Returns an existing, but disabled service account to test IAM RPCs.
pub fn service_account_for_iam_tests() -> Result<String> {
    let value = std::env::var("GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT")?;
    Ok(value)
}

/// Returns the preferred region id used for the integration tests.
pub fn region_id() -> String {
    std::env::var("GOOGLE_CLOUD_RUST_TEST_REGION")
        .ok()
        .unwrap_or("us-central1".to_string())
}

/// Returns the preferred service account for the test workflows.
pub fn workflows_runner() -> Result<String> {
    let value = std::env::var("GOOGLE_CLOUD_RUST_TEST_WORKFLOWS_RUNNER")?;
    Ok(value)
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

pub fn random_bucket_id() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

    let distr = RandomChars { chars: CHARSET };
    const PREFIX: &str = "rust-sdk-testing-";
    let bucket_id: String = rand::rng()
        .sample_iter(distr)
        .take(BUCKET_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{bucket_id}")
}

pub(crate) struct RandomChars {
    chars: &'static [u8],
}

impl Distribution<u8> for RandomChars {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
        let index = rng.random_range(0..self.chars.len());
        self.chars[index]
    }
}
