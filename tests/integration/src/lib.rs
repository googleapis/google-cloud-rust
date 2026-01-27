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

use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use google_cloud_test_utils::resource_names::random_bucket_id;
use rand::{Rng, distr::Alphanumeric};

pub type Result<T> = anyhow::Result<T>;
pub mod aiplatform;
pub mod bigquery;
pub mod compute;
pub mod error_details;
pub mod firestore;
#[cfg(google_cloud_unstable_tracing)]
pub mod observability;
pub mod secret_manager;
pub mod showcase;
pub mod storage;
pub mod workflows;
pub mod workflows_executions;

pub const SECRET_ID_LENGTH: usize = 64;

pub const VM_ID_LENGTH: usize = 63;

pub const WORKFLOW_ID_LENGTH: usize = 64;

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
    let image_id = LowercaseAlphanumeric.random_string(VM_ID_LENGTH - PREFIX.len());
    format!("{PREFIX}{image_id}")
}

pub fn random_vm_id() -> String {
    const PREFIX: &str = "vm-";
    let vm_id = LowercaseAlphanumeric.random_string(VM_ID_LENGTH - PREFIX.len());
    format!("{PREFIX}{vm_id}")
}

pub(crate) fn random_vm_prefix(len: usize) -> String {
    const PREFIX: &str = "vm-";
    let vm_id = LowercaseAlphanumeric.random_string(len);
    format!("{PREFIX}{vm_id}")
}

pub fn enable_tracing() -> tracing::subscriber::DefaultGuard {
    google_cloud_test_utils::tracing::enable_tracing()
}
