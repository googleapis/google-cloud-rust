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

//! Google Cloud Client Libraries for Rust - BigQuery
//!
//! **WARNING:** this is a preview release of the crate. We believe the APIs to be
//! stable. We also are seeking feedback about the APIs and may need to make
//! breaking changes if we discover that some parts are hard to use.
//!
//! We welcome feedback about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [Google Cloud BigQuery][bigquery]. Most applications will use the structs
//! defined in the [client] module.
//!
//! For executing queries and managing jobs:
//! * [BigQuery][client::BigQuery]
//!
//! For handling query execution:
//! * [Query]
//! * [CompleteQuery]
//!
//! For streaming and reading results:
//! * [RowIterator]
//! * [Row]
//!
//! For converting results to Rust types:
//! * [FromRow]
//! * [FromSql]
//!
//! [bigquery]: https://cloud.google.com/bigquery
//!
//! # Example: Executing a Query
//!
//! ```
//! # use google_cloud_bigquery::client::BigQuery;
//! # async fn sample() -> anyhow::Result<()> {
//! // Create a client configured with a default project ID.
//! let client = BigQuery::builder()
//!     .with_project_id("my-project-id")
//!     .build()
//!     .await?;
//!
//! // Configure, run, and read query results.
//! let mut rows = client
//!     .query("SELECT 'hello world' AS greeting")
//!     .run()
//!     .await?
//!     .until_done()
//!     .await?
//!     .read();
//!
//! while let Some(row) = rows.next().await.transpose()? {
//!     let greeting: String = row.get("greeting");
//!     println!("Greeting: {greeting}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Mapping Rows to Rust Structs
//!
//! Define typed Rust structs with `#[derive(FromRow)]` to convert rows
//! directly into domain types using `TryFrom<Row>`:
//!
//! ```
//! # use google_cloud_bigquery::client::BigQuery;
//! # use google_cloud_bigquery::FromRow;
//! #[derive(FromRow, Debug)]
//! struct UserStats {
//!     name: String,
//!     count: i64,
//! }
//!
//! # async fn sample(client: BigQuery) -> anyhow::Result<()> {
//! let mut rows = client
//!     .query("SELECT name, count FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = 'WA' LIMIT 5")
//!     .run()
//!     .await?
//!     .until_done()
//!     .await?
//!     .read();
//!
//! while let Some(row) = rows.next().await.transpose()? {
//!     let user: UserStats = row.try_into()?;
//!     println!("{} has count {}", user.name, user.count);
//! }
//! # Ok(())
//! # }
//! ```

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;
pub mod error;
pub use crate::error::{ConvertError, QueryError, RowError};
pub use crate::query::{CompleteQuery, FromSql, Interval, Query, Range, Row, RowIterator};
pub use google_cloud_bigquery_derive::{FromRow, FromSql};

pub(crate) mod generated;
pub(crate) mod query;
pub(crate) mod retry_policy;
pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;

/// Clients to interact with Google Cloud BigQuery.
pub mod client;
mod client_builder;

pub mod model {
    //! Re-exports for the Google Cloud BigQuery v2 API types.
    pub use crate::generated::{QueryCreationMetadata, QueryMetadata, RunQueryRequest};
    pub use google_cloud_bigquery_v2::model::*;
}

pub mod builder {
    //! Builders for the BigQuery client.
    pub mod bigquery {
        //! Builder for [BigQuery][crate::client::BigQuery].
        pub use crate::client_builder::ClientBuilder;
        pub use crate::query::RunQuery;
    }
}
