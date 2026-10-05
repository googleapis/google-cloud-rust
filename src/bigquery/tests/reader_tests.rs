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

//! use google_cloud_bigquery::client::Read;
//! use google_cloud_bigquery::model::{DataFormat, ReadSession};
//! # async fn sample() -> anyhow::Result<()> {
//! let client = Read::builder().build().await?;
//!
//! let session = client
//!     .create_read_session()
//!     .set_parent("projects/my-project")
//!     .set_read_session(
//!         ReadSession::new()
//!             .set_data_format(DataFormat::Arrow)
//!             .set_table("projects/my-project/datasets/my-dataset/tables/my-table"),
//!     )
//!     .set_max_stream_count(1)
//!     .send()
//!     .await?;
//!
//! for stream in &session.streams {
//!     let mut rows = client
//!         .read_rows()
//!         .set_read_stream(&stream.name)
//!         .send()
//!         .await?;
//!
//!     while let Some(response) = rows.next().await.transpose()? {
//!         println!("Read {} rows", response.row_count);
//!     }
//! }
//! # Ok(())
//! # }
