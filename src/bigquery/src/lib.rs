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
//! For streaming data to BigQuery:
//! * [Write][client::Write]
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
//!     let greeting: String = row.get("greeting")?;
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
//!
//! # Example: Writing to BigQuery
//!
//! ```
//! use google_cloud_bigquery::client::Write;
//! use google_cloud_bigquery::model::{ArrowSchema, ArrowRecordBatch};
//! # async fn sample() -> anyhow::Result<()> {
//! let client = Write::builder().build().await?;
//! let writer = client
//!     .arrow(schema())
//!     .default("projects/my-project/datasets/my-dataset/tables/my-table")?;
//!
//! let f1 = writer.append(rows()).send();
//! let f2 = writer.append(rows()).send();
//!
//! let _ = f1.await?;
//! let _ = f2.await?;
//! # Ok(()) }
//!
//! fn schema() -> ArrowSchema {
//!     todo!("Define your table's schema...")
//! }
//! fn rows() -> ArrowRecordBatch {
//!     todo!("Serialize your rows...")
//! }
//! ```

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;

pub(crate) mod generated;

/// Clients to interact with Google Cloud BigQuery.
pub mod client {
    pub use crate::query::client::BigQuery;
    pub use crate::write::client::Write;
    // TODO(#6152) - add Write admin client
}

/// The messages and enums that are part of this client library
pub mod model {
    pub(crate) use crate::write::generated::gapic_storage::model::*;
    pub use crate::write::generated::gapic_storage::model::{
        ArrowRecordBatch, ArrowSchema, BatchCommitWriteStreamsResponse,
        FinalizeWriteStreamResponse, FlushRowsResponse, RowError, StorageError, TableFieldSchema,
        TableSchema, row_error, storage_error, table_field_schema,
    };
}

/// Extends [crate::model].
///
/// Note that there is no real distinction between the types in `model` and
/// `model_ext`. The two modules are separate for library maintenance reasons.
pub mod model_ext {
    pub use crate::generated::{CompleteQueryMetadata, QueryMetadata, QueryRequest};
    pub use crate::write::append_response::AppendResponse;
}

/// Request and client builders.
pub mod builder {
    /// Request and client builders for the [BigQuery][crate::client::BigQuery] client.
    pub mod bigquery {
        pub use crate::generated::QueryRequest;
        pub use crate::query::builder::Query;
        pub use crate::query::client_builder::ClientBuilder;
    }
    /// Request and client builders for the [Write][crate::client::Write] client.
    pub mod write {
        pub use crate::write::append_builder::{Append, AppendWithOffset};
        pub use crate::write::client_builder::ClientBuilder;
    }
}

/// Custom errors for the BigQuery clients.
pub mod error;

/// Types related to querying with a [BigQuery][crate::client::BigQuery] client.
pub mod query;

/// Types related to writing with a [Write][crate::client::Write] client.
pub mod write;

pub mod datatypes;

pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;
pub(crate) use google_cloud_gax::options::RequestOptions;
pub(crate) use google_cloud_gax::options::internal::RequestBuilder;
pub(crate) use google_cloud_gax::response::Response;

#[allow(dead_code)]
pub(crate) mod google {
    pub mod api {
        include!("write/generated/protos/storage/google.api.rs");
    }
    pub mod cloud {
        pub mod bigquery {
            pub mod storage {
                pub mod v1 {
                    #![allow(deprecated)]
                    include!("write/generated/protos/storage/google.cloud.bigquery.storage.v1.rs");
                    include!("write/generated/convert/storage/convert.rs");
                }
            }
        }
    }
    pub mod rpc {
        include!("write/generated/protos/storage/google.rpc.rs");
    }
}
