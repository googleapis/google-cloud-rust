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

// TODO(#6443) - consolidate crates

pub use google_cloud_bigquery_write::AppendFuture;
pub use google_cloud_bigquery_write::arrow;

pub(crate) use google_cloud_bigquery_write::builder;
pub(crate) use google_cloud_bigquery_write::client;
pub(crate) use google_cloud_bigquery_write::error;

// TODO(#6443) - relocate this.
pub use google_cloud_bigquery_write::model;
