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

#![warn(missing_docs)]

//! Google Cloud Client Libraries for Rust - BigQuery Write
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [BigQuery Write].
//!
//! [bigquery write]: https://docs.cloud.google.com/bigquery/docs/write-api

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;

pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;
pub(crate) use google_cloud_gax::options::RequestOptions;
pub(crate) use google_cloud_gax::options::internal::RequestBuilder;
pub(crate) use google_cloud_gax::response::Response;

/// Clients to interact with Cloud BigQuery Storage Write API
pub mod client;
/// Builders to interact with Cloud BigQuery Storage Write API
pub mod builder {
    /// Request and client builders for the [Write][crate::client::Write] client
    pub mod write {
        pub use crate::append_builder::Append;
        pub use crate::client_builder::ClientBuilder;
    }
    // TODO(#6152) - add admin client
}
/// Types to write data in [Arrow] format
///
/// [arrow]: https://arrow.apache.org/
pub mod arrow;

mod append_builder;
mod append_response;
mod client_builder;
mod error;
mod proto_schema;
#[cfg_attr(not(test), expect(dead_code))]
mod runner;
mod stream;
mod transport;

// TODO(#4832) - remove handwritten code.
mod status;
/// The messages and enums that are part of this client library
pub mod model {
    pub use crate::append_response::AppendResponse;
    // TODO(#6224) - restrict exports
    pub use crate::generated::gapic_storage::model::*;
}

#[allow(dead_code)]
pub(crate) mod generated;

#[allow(dead_code)]
pub(crate) mod google {
    pub mod api {
        include!("generated/protos/storage/google.api.rs");
    }
    pub mod cloud {
        pub mod bigquery {
            pub mod storage {
                pub mod v1 {
                    #![allow(deprecated)]
                    include!("generated/protos/storage/google.cloud.bigquery.storage.v1.rs");
                    include!("generated/convert/storage/convert.rs");
                }
            }
        }
    }
    pub mod rpc {
        include!("generated/protos/storage/google.rpc.rs");
    }
}
