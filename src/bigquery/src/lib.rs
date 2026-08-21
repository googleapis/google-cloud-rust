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
//! * [Query](crate::query::Query)
//! * [CompleteQuery](crate::query::CompleteQuery)
//!
//! For streaming and reading results:
//! * [RowIterator](crate::query::RowIterator)
//! * [Row](crate::query::Row)
//!
//! For converting results to Rust types:
//! * [FromRow](crate::query::FromRow)
//! * [FromSql](crate::query::FromSql)
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
//! # use google_cloud_bigquery::query::FromRow;
//! #[derive(FromRow, Debug)]
//! struct UserStats {
//!     name: String,
//!     count: i64,
//! }
//!
//! # async fn sample(client: BigQuery) -> anyhow::Result<()> {
//! let mut rows = client
//!     .query("SELECT name, count FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = 'WA' LIMIT 5")
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

pub(crate) mod generated;

/// Clients to interact with Google Cloud BigQuery.
pub mod client {
    pub use crate::query::client::BigQuery;
}

/// Extends [google_cloud_bigquery_v2::model] with types that improve ergonomics.
pub mod model_ext {
    pub use crate::generated::{CompleteQueryMetadata, QueryMetadata};
}

/// Request and client builders.
pub mod builder {
    /// Request and client builders for the [BigQuery][crate::client::BigQuery] client.
    pub mod bigquery {
        pub use crate::generated::QueryRequest;
        pub use crate::query::builder::Query;
        pub use crate::query::client_builder::ClientBuilder;
    }
}

/// Custom errors for the BigQuery clients.
pub mod error;

/// Types related to querying with a [BigQuery][crate::client::BigQuery] client.
pub mod query;

pub mod datatypes;
