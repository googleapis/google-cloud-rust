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

//! Google Cloud Client Libraries for Rust - Spanner
//!
//! **NOTE:** While the version is still `0.x`, we believe the APIs to be stable.
//! We plan to release a `1.0` version of this crate in the following release.
//!
//! We welcome feedback about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [Google Cloud Spanner][spanner]. Cloud Spanner is a fully managed,
//! mission-critical, relational database service that offers transactional
//! consistency at global scale, schemas, SQL (ANSI 2011 with extensions),
//! and automatic, synchronous replication for high availability.
//!
//! Most applications will use the clients defined in the [client] module:
//!
//! * [Spanner][client::Spanner]: The root service client. Manages gRPC channel pools,
//!   credentials, and emulator settings, and constructs [DatabaseClient][client::DatabaseClient]
//!   instances and admin client builders.
//! * [DatabaseClient][client::DatabaseClient]: The primary client for reading, querying,
//!   and modifying data in a specific database.
//! * [DatabaseAdmin][client::DatabaseAdmin]: Admin client for managing databases, backups,
//!   and applying DDL schema updates.
//! * [InstanceAdmin][client::InstanceAdmin]: Admin client for creating and managing Spanner
//!   instances and configurations.
//!
//! [spanner]: https://cloud.google.com/spanner
//!
//! # Architecture and Client Lifecycle
//!
//! - **`Spanner` (Root Service Client)**: Represents a shared gRPC transport and credential
//!   context. It should be initialized once at application startup and reused across the
//!   entire process.
//! - **`DatabaseClient` (Per-Database Client)**: Manages a long-lived multiplexed session
//!   for a specific database in the background. Creating a `DatabaseClient` initializes session
//!   and connection state, so an application should create a single `DatabaseClient` per
//!   database and share it across all tasks.
//! - **Thread Safety & Cheap Cloning**: Both `Spanner` and `DatabaseClient` wrap internal
//!   state in `Arc` pointers. Cloning them is cheap ($O(1)$) and shares
//!   underlying sessions, channel pools, and routing state. Never create a new client
//!   per request.
//!
//! # Transaction Models
//!
//! Spanner provides specialized transaction types tailored for different access patterns:
//!
//! * **[Single-Use][client::DatabaseClient::single_use]**: Optimized for executing a single read
//!   or query with minimal latency and no transaction initialization overhead. Supports
//!   [timestamp bounds][transaction::TimestampBound] to configure read staleness.
//! * **[Read-Write][client::DatabaseClient::read_write_transaction]**: Atomic reads and writes
//!   coordinated via [`TransactionRunner`][transaction::TransactionRunner]. Automatically retries
//!   the closure if Spanner aborts the transaction.
//! * **[Multi-Use Read-Only][client::DatabaseClient::read_only_transaction]**: Executes multiple
//!   queries and reads against a consistent point-in-time snapshot without taking locks.
//! * **[Write-Only][client::DatabaseClient::write_only_transaction]**: High-throughput blind writes
//!   using mutations when reads prior to writing are not required.
//! * **[Partitioned DML][client::DatabaseClient::partitioned_dml_transaction]**: Executes distributed,
//!   parallel bulk updates or deletions across large tables without transaction mutation limits.
//! * **[Batch Read-Only][client::DatabaseClient::batch_read_only_transaction]**: Splits queries or
//!   reads into parallel partitions that can be distributed across worker processes (e.g., Apache Beam).
//! * **[Batch Write][client::DatabaseClient::batch_write_transaction]**: Executes multiple mutation
//!   groups in parallel with per-group atomicity.
//!
//! # Example: Executing a Parameterized Query
//!
//! Parameterized queries prevent SQL injection and maximize Spanner's query plan cache hits:
//!
//! ```
//! use google_cloud_spanner::client::Spanner;
//! use google_cloud_spanner::statement::Statement;
//!
//! # async fn sample() -> anyhow::Result<()> {
//! // Initialize the root service client.
//! let spanner = Spanner::builder().build().await?;
//!
//! // Create a long-lived client for a specific database.
//! let database_client = spanner
//!     .database_client("projects/my-project/instances/my-instance/databases/my-database")
//!     .build()
//!     .await?;
//!
//! // Execute a single-use query.
//! let transaction = database_client.single_use().build();
//! let statement = Statement::builder(
//!     "SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId = @id",
//! )
//! .add_param("id", 42i64)
//! .build();
//!
//! let mut result_set = transaction.execute_query(statement).await?;
//! while let Some(row) = result_set.next().await.transpose()? {
//!     let singer_id: i64 = row.get("SingerId")?;
//!     let first_name: String = row.get("FirstName")?;
//!     let last_name: String = row.get("LastName")?;
//!     println!("Singer {singer_id}: {first_name} {last_name}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Read-Write Transaction
//!
//! Read-write transactions execute reads and modifications atomically. Use
//! [`TransactionRunner`][transaction::TransactionRunner] to execute operations;
//! it automatically retries the transaction closure if Spanner aborts it:
//!
//! ```
//! use google_cloud_spanner::client::DatabaseClient;
//! use google_cloud_spanner::statement::Statement;
//!
//! # async fn sample(database_client: &DatabaseClient) -> anyhow::Result<()> {
//! let runner = database_client.read_write_transaction().build();
//! let singer_ids = vec![1i64, 2i64, 3i64];
//!
//! let transaction_result = runner
//!     .run(async |transaction| {
//!         // 1. Query singers matching the array parameter using `IN UNNEST`:
//!         let query_statement = Statement::builder(
//!             "SELECT SingerId, Rating FROM Singers WHERE SingerId IN UNNEST(@ids)",
//!         )
//!         .add_param("ids", &singer_ids)
//!         .build();
//!
//!         let mut result_set = transaction.execute_query(query_statement).await?;
//!         let mut statements = Vec::new();
//!
//!         while let Some(row) = result_set.next().await.transpose()? {
//!             let singer_id: i64 = row.get("SingerId")?;
//!             let current_rating: Option<i64> = row.get("Rating")?;
//!             let update_statement = Statement::builder(
//!                 "UPDATE Singers SET Rating = @new_rating WHERE SingerId = @id",
//!             )
//!             .add_param("new_rating", current_rating.unwrap_or(0) + 1)
//!             .add_param("id", singer_id)
//!             .build();
//!             statements.push(update_statement);
//!         }
//!
//!         // 2. If no singers matched, return early to avoid unnecessary updates:
//!         if statements.is_empty() {
//!             return Ok(Vec::new());
//!         }
//!
//!         // 3. Update singer ratings in a single round trip using Batch DML:
//!         let update_counts = transaction.execute_batch_update(statements).await?;
//!
//!         // 4. Update summary statistics using a single DML statement:
//!         let summary_statement = Statement::builder(
//!             "UPDATE SingerStatistics SET AverageRating = (SELECT AVG(Rating) FROM Singers) WHERE Metric = 'Global'",
//!         )
//!         .build();
//!         transaction.execute_update(summary_statement).await?;
//!
//!         Ok(update_counts)
//!     })
//!     .await?;
//!
//! println!("Updated singer rows: {:?}", transaction_result.result);
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Consistent Snapshot Multi-Query Read
//!
//! Multi-use read-only transactions guarantee that multiple reads and queries observe
//! the exact same consistent snapshot in time without taking locks.
//!
//! While strong reads (the default) may require consulting the Paxos leader region to negotiate
//! timestamps, specifying a staleness of at least 15 seconds allows nearby replicas
//! (including read-only replicas) to serve reads locally without leader communication,
//! reducing latency in multi-region instances:
//!
//! ```
//! use std::time::Duration;
//! use google_cloud_spanner::client::DatabaseClient;
//! use google_cloud_spanner::statement::Statement;
//! use google_cloud_spanner::transaction::TimestampBound;
//!
//! # async fn sample(database_client: &DatabaseClient) -> anyhow::Result<()> {
//! // Configure staleness (>= 15s) so nearby replicas can serve the query without leader consultation.
//! let timestamp_bound = TimestampBound::exact_staleness(Duration::from_secs(15));
//! let transaction = database_client
//!     .read_only_transaction()
//!     .set_timestamp_bound(timestamp_bound)
//!     .build()
//!     .await?;
//!
//! let first_statement = Statement::builder(
//!     "SELECT SingerId, FirstName FROM Singers WHERE SingerId = @id",
//! )
//! .add_param("id", 1i64)
//! .build();
//! let mut first_results = transaction.execute_query(first_statement).await?;
//! while let Some(row) = first_results.next().await.transpose()? {
//!     let singer_id: i64 = row.get("SingerId")?;
//!     let first_name: String = row.get("FirstName")?;
//!     println!("Singer {singer_id}: {first_name}");
//! }
//!
//! let second_statement = Statement::builder(
//!     "SELECT AlbumId, AlbumTitle FROM Albums WHERE SingerId = @id",
//! )
//! .add_param("id", 1i64)
//! .build();
//! let mut second_results = transaction.execute_query(second_statement).await?;
//! while let Some(row) = second_results.next().await.transpose()? {
//!     let album_id: i64 = row.get("AlbumId")?;
//!     let album_title: String = row.get("AlbumTitle")?;
//!     println!("Album {album_id}: {album_title}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Writing Mutations
//!
//! For blind writes where reading prior to writing is not required, write mutations
//! directly using [`write_only_transaction`][client::DatabaseClient::write_only_transaction]:
//!
//! ```
//! use google_cloud_spanner::client::DatabaseClient;
//! use google_cloud_spanner::mutation::Mutation;
//!
//! # async fn sample(database_client: &DatabaseClient) -> anyhow::Result<()> {
//! let mutations = vec![
//!     Mutation::new_insert_or_update_builder("Singers")
//!         .set("SingerId").to(1i64)
//!         .set("FirstName").to("Marc")
//!         .set("LastName").to("Richards")
//!         .set("Rating").to(4i64)
//!         .build(),
//!     Mutation::new_insert_or_update_builder("Singers")
//!         .set("SingerId").to(2i64)
//!         .set("FirstName").to("Catalina")
//!         .set("LastName").to("Smith")
//!         .set("Rating").to(5i64)
//!         .build(),
//! ];
//!
//! database_client
//!     .write_only_transaction()
//!     .build()
//!     .write(mutations)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Local Development with the Spanner Emulator
//!
//! To develop locally against the [Spanner Emulator][emulator], set the
//! `SPANNER_EMULATOR_HOST` environment variable before creating the client:
//!
//! ```bash
//! export SPANNER_EMULATOR_HOST=localhost:9010
//! ```
//!
//! The client builder automatically detects this environment variable, configures
//! anonymous credentials, points gRPC calls to the emulator host, and defaults
//! to a static channel pool with a single channel.
//!
//! [emulator]: https://cloud.google.com/spanner/docs/emulator
//!
//! # Features
//!
//! - `builtin-metrics`: enabled by default. Automatically exports Spanner built-in request
//!   and attempt latency metrics to Google Cloud Monitoring in the background.
//! - `default-rustls-provider`: enabled by default. Use the default rustls crypto provider
//!   ([aws-lc-rs]) for TLS and authentication.
//! - `unstable-metrics`: enable integration with a custom caller-supplied OpenTelemetry
//!   `MeterProvider`. Because the `opentelemetry` crate is pre-1.0, this feature is
//!   subject to SemVer-incompatible changes across OpenTelemetry minor releases.
//! - `unstable-stream`: enable the (unstable) features to convert several types to
//!   a `futures::Stream`.
//! - `unstable-time`: enable support for types from the `time` crate (`time::OffsetDateTime`
//!   and `time::Date`).
//!
//! [aws-lc-rs]: https://crates.io/crates/aws-lc-rs

#![cfg_attr(docsrs, feature(doc_cfg))]

// Public domain modules.

/// Configuration types for the gRPC channel pool.
pub mod channel_pool;
/// Key and key range definition types.
pub mod key;
/// Write mutations and transaction commit binders.
pub mod mutation;
/// Spanner Omni options and configuration.
pub mod omni;
/// Configurable read requests and builders.
pub mod read;
/// Spanner execution result streams and rows.
pub mod result;
/// RPC retry policies used by the Spanner client.
pub mod retry_policy;
/// SQL statement builders and parameter bindings.
pub mod statement;
/// Spanner primitive type constructors for typed parameter binding.
pub mod types;
/// Type and value representations and conversion traits.
pub mod value;

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;
/// Re-export of the `opentelemetry` crate.
///
/// **Warning:** This re-export is subject to SemVer-incompatible changes across
/// OpenTelemetry minor releases until OpenTelemetry reaches 1.0.
#[cfg(feature = "unstable-metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-metrics")))]
pub use opentelemetry;
pub use rust_decimal::Decimal;

pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;
pub(crate) use google_cloud_gax::options::RequestOptions;
pub(crate) use google_cloud_gax::options::internal::RequestBuilder;
pub(crate) use google_cloud_gax::response::Response;

/// Spanner client implementations.
pub mod client;

/// Consolidates all client and request builders.
pub mod builder;

/// Crate error types.
pub mod error;

/// Transaction-scoped interfaces and transaction runners.
pub mod transaction;

/// Batch execution and query partitioning support.
pub mod batch;

/// The messages and enums that are part of this client library.
pub mod model {
    pub use crate::generated::gapic_dataplane::model::*;
}

/// Mocking and stub definitions.
pub mod stub {
    pub use crate::generated::gapic_dataplane::stub::*;
}

// Internal modules
pub(crate) mod batch_dml;
pub(crate) mod batch_read_only_transaction;
pub(crate) mod batch_write_transaction;
pub(crate) mod database_client;
pub(crate) mod from_row;
pub(crate) mod from_value;
pub(crate) mod observability;
pub(crate) mod partitioned_dml_transaction;
pub(crate) mod precommit;
pub(crate) mod read_only_transaction;
pub(crate) mod read_write_transaction;
pub(crate) mod request_id;
pub(crate) mod request_id_interceptor;
pub(crate) mod result_set;
pub(crate) mod result_set_metadata;
pub(crate) mod retry_delay;
pub(crate) mod routing;
pub(crate) mod row;
pub(crate) mod server_streaming;
pub(crate) mod session_maintainer;
pub(crate) mod timestamp_bound;
pub(crate) mod to_value;
pub(crate) mod transaction_retry_policy;
pub(crate) mod transaction_runner;
pub(crate) mod write_only_transaction;

mod status;

#[cfg(test)]
mod test;

#[allow(dead_code)]
#[allow(rustdoc::broken_intra_doc_links)]
#[allow(rustdoc::private_intra_doc_links)]
#[allow(clippy::enum_variant_names)]
pub(crate) mod generated;

#[allow(dead_code)]
#[allow(clippy::all)]
pub(crate) mod google {
    pub mod api {
        include!("generated/protos/spanner/google.api.rs");
    }
    pub mod rpc {
        include!("generated/protos/spanner/google.rpc.rs");
    }
    #[allow(clippy::enum_variant_names)]
    pub mod spanner {
        pub mod v1 {
            include!("generated/protos/spanner/google.spanner.v1.rs");
            include!("generated/convert/spanner/convert.rs");
        }
    }
}
