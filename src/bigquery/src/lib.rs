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
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [BigQuery].
//!
//! [bigquery]: https://cloud.google.com/bigquery

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;
pub mod error;
pub use crate::error::{ConvertError, QueryError, RowError};
pub use crate::query::{FromSql, Range, Row};
pub use google_cloud_bigquery_derive::{FromRow, FromSql};

pub(crate) mod generated;
pub(crate) mod query;
pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;

/// High-level BigQuery client and execution entrypoints.
pub mod client;
mod client_builder;

pub mod model {
    //! Re-exports for the Google Cloud BigQuery v2 API types.
    pub use crate::generated::{QueryMetadata, RunQueryRequest};
    pub use crate::query::{QueryReference, RunQuery};
    pub use google_cloud_bigquery_v2::model::*;
}

pub mod builder {
    //! Builders for the BigQuery client.
    pub mod bigquery {
        //! Builder for [BigQuery][crate::client::BigQuery].
        pub use crate::client_builder::ClientBuilder;
        pub use crate::generated::{QueryMetadata, RunQueryRequest};
        pub use crate::query::RunQuery;
    }
}
