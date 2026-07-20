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
//! **WARNING:** this is a preview release of the crate. We believe the APIs to be stable. We also
//! are seeking feedback about the APIs and may need to make breaking changes if we discover that
//! some parts are hard to use.
//!
//! We welcome feedback about the APIs, documentation, missing features, bugs, etc.

// Public domain modules.

/// Key and key range definition types.
pub mod key;
/// Write mutations and transaction commit binders.
pub mod mutation;
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
pub(crate) mod from_value;
pub(crate) mod partitioned_dml_transaction;
pub(crate) mod precommit;
pub(crate) mod read_only_transaction;
pub(crate) mod read_write_transaction;
pub(crate) mod result_set;
pub(crate) mod result_set_metadata;
pub(crate) mod row;
pub(crate) mod server_streaming;
pub(crate) mod session_maintainer;
pub(crate) mod timestamp_bound;
pub(crate) mod to_value;
pub(crate) mod transaction_retry_policy;
pub(crate) mod transaction_runner;
pub(crate) mod write_only_transaction;

mod status;

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
