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
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;

pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;
pub(crate) use google_cloud_gax::options::RequestOptions;
pub(crate) use google_cloud_gax::options::internal::RequestBuilder;
pub(crate) use google_cloud_gax::response::Response;

pub mod client {}
pub mod builder {}
pub mod model {
    pub use crate::generated::gapic_dataplane::model::*;
}

mod status;

#[allow(dead_code)]
#[allow(rustdoc::broken_intra_doc_links)]
#[allow(rustdoc::private_intra_doc_links)]
pub(crate) mod generated;

#[allow(dead_code)]
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
