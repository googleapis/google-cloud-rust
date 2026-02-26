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

#![cfg_attr(docsrs, feature(doc_cfg))]

#[allow(rustdoc::broken_intra_doc_links)]
pub(crate) mod generated;
mod status;

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;
// Define some shortcuts for imported crates.
pub(crate) use google_cloud_gax::client_builder::ClientBuilder;
pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;
pub(crate) use google_cloud_gax::client_builder::internal::ClientFactory;
pub(crate) use google_cloud_gax::client_builder::internal::new_builder as new_client_builder;
pub(crate) use google_cloud_gax::options::RequestOptions;
pub(crate) use google_cloud_gax::options::internal::RequestBuilder;
pub(crate) use google_cloud_gax::response::Response;

pub mod builder {
    pub use crate::generated::gapic_dataplane::builder::spanner;
}

pub mod model {
    pub use crate::generated::gapic_dataplane::model::*;
}

pub mod client;
pub mod database_client;
pub mod read_context;
pub mod read_only_transaction;
pub mod read_write_transaction;
pub mod result_set;
pub mod row;
pub mod statement;
pub mod partitioned_dml;
pub mod types;
pub mod mutation;
pub mod value;
pub mod stub {
    pub use crate::generated::gapic_dataplane::stub::*;
}

#[allow(dead_code)]
pub mod google {
    pub mod api {
        include!("generated/protos/spanner/google.api.rs");
    }
    pub mod rpc {
        include!("generated/protos/spanner/google.rpc.rs");
    }
    pub mod spanner {
        #[allow(clippy::enum_variant_names)]
        pub mod v1 {
            include!("generated/protos/spanner/google.spanner.v1.rs");
            include!("generated/convert/spanner/convert.rs");
        }
    }
}
