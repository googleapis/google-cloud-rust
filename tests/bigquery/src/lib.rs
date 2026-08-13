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

mod dataset;
mod job;
mod query;
mod writes;

use rand::{RngExt, distr::Alphanumeric};

const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

pub use dataset::dataset_admin;
pub use job::job_service;
pub use query::{
    query_client, query_client_datatypes, query_client_job, query_client_multi_page,
    query_client_nested_types, query_client_numeric_limits,
};
pub use writes::run_writes;

fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}
