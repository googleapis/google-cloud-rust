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

//! Define types to compute and compare Cloud Storage object checksums.
//!
//! # Example
//! ```
//! use google_cloud_storage::builder::storage::WriteObject;
//! use google_cloud_storage::model::Object;
//! use google_cloud_storage::{streaming_source::StreamingSource};
//!
//! async fn example<S, C>(builder: WriteObject<S>) -> anyhow::Result<Object>
//! where
//!     S: StreamingSource + Send + Sync + 'static,
//! {
//!     // Finish configuring `builder` and complete the upload.
//!     let object = builder
//!         .set_if_generation_match(0)
//!         .with_resumable_upload_threshold(0_usize)
//!         .send_buffered()
//!         .await?;
//!     Ok(object)
//! }
//! ```

use crate::error::ChecksumMismatch;
use crate::model::ObjectChecksums;

pub(crate) mod details;
