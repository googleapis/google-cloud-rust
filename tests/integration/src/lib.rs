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

use google_cloud_test_utils::resource_names::random_bucket_id;
pub(crate) use google_cloud_test_utils::resource_names::random_workflow_id;

pub type Result<T> = anyhow::Result<T>;
pub mod bigquery;
pub mod error_details;

pub fn report_error(e: anyhow::Error) -> anyhow::Error {
    eprintln!("\n\nERROR {e:?}\n");
    tracing::error!("ERROR {e:?}");
    e
}
