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

//! Google Cloud Client Libraries for Rust - Storage Control
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with the
//! [Storage] control APIs.
//!
//! [storage]: https://cloud.google.com/storage

pub use gax::Result;
pub use gax::error::Error;
#[allow(dead_code)]
// TODO(#1813) - fix the broken link to `[here]`.
#[allow(rustdoc::broken_intra_doc_links)]
pub(crate) mod generated;

/// Request builders.
pub mod builder {
    pub mod storage_control {
        pub use crate::generated::gapic::builder::storage_control::*;
        pub use crate::generated::gapic_control::builder::storage_control::*;
    }
}
/// The messages and enums that are part of this client library.
pub mod model {
    pub use crate::generated::gapic::model::*;
    pub use crate::generated::gapic_control::model::*;
}
pub mod client;
pub mod stub;

pub(crate) mod google {
    pub mod iam {
        pub mod v1 {
            include!("generated/protos/storage/google.iam.v1.rs");
            include!("generated/convert/iam/convert.rs");
        }
    }
    pub mod longrunning {
        include!("generated/protos/control/google.longrunning.rs");
        include!("generated/convert/longrunning/convert.rs");
    }
    pub mod r#type {
        include!("generated/protos/storage/google.r#type.rs");
        include!("generated/convert/type/convert.rs");
    }
    pub mod rpc {
        include!("generated/protos/storage/google.rpc.rs");
    }
    pub mod storage {
        #[allow(deprecated)]
        #[allow(clippy::large_enum_variant)]
        pub mod v2 {
            include!("generated/protos/storage/google.storage.v2.rs");
            include!("generated/convert/storage/convert.rs");
        }
        pub mod control {
            pub mod v2 {
                include!("generated/protos/control/google.storage.control.v2.rs");
                include!("generated/convert/control/convert.rs");
            }
        }
    }
}

mod convert;
