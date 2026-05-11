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

//! Google Cloud Client Libraries for Rust - Cloud Firestore API
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with Firestore.
//! Most applications will use the structs defined in the [client] module.
//! More specifically:
//!
//! * [Firestore](client/struct.Firestore.html)

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
// TODO(#1549) - remove this workaround once all code is generated.
#[allow(rustdoc::broken_intra_doc_links)]
pub(crate) mod generated;

pub use generated::gapic::builder;
pub use generated::gapic::client;
pub use generated::gapic::model;
pub use generated::gapic::stub;

#[allow(dead_code)]
pub(crate) mod google {
    pub mod firestore {
        #[allow(clippy::enum_variant_names)]
        #[allow(clippy::large_enum_variant)]
        pub mod v1 {
            include!("generated/protos/firestore/google.firestore.v1.rs");
            include!("generated/convert/firestore/convert.rs");
        }
    }
    pub mod r#type {
        // TODO(#1414) - decide if we want to generate this as its own directory.
        include!("generated/protos/firestore/google.r#type.rs");
        include!("generated/convert/type/convert.rs");
    }
    pub mod rpc {
        include!("generated/protos/firestore/google.rpc.rs");
    }
}

mod convert;
pub mod status;
