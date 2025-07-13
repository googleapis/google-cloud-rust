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

//! Google Cloud Client Libraries for Rust - Storage
//!
//! This crate contains traits, types, and functions to interact with [Google
//! Cloud Storage]. Most applications will use the structs defined in the
//! [client] module. More specifically:
//!
//! * [Storage][client::Storage]
//! * [StorageControl][client::StorageControl]
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! [Google Cloud Storage]: https://cloud.google.com/storage

pub use gax::Result;
pub use gax::error::Error;

pub mod backoff_policy;
pub mod retry_policy;
pub use crate::storage::upload_source;

mod control;
mod storage;

/// Clients to interact with Google Cloud Storage.
pub mod client {
    pub use crate::control::client::StorageControl;
    pub use crate::storage::client::{KeyAes256, KeyAes256Error, Storage};
}
/// Request builders.
pub mod builder {
    pub mod storage {
        pub use crate::storage::client::ClientBuilder;
        pub use crate::storage::read_object::ReadObject;
        pub use crate::storage::upload_object::UploadObject;
    }
    pub mod storage_control {
        pub use crate::control::builder::*;
        pub use crate::control::client::ClientBuilder;
    }
}
/// The messages and enums that are part of this client library.
pub use crate::control::model;
pub use crate::control::stub;

pub(crate) mod google {
    pub mod iam {
        pub mod v1 {
            include!("control/generated/protos/storage/google.iam.v1.rs");
            include!("control/generated/convert/iam/convert.rs");
        }
    }
    pub mod longrunning {
        include!("control/generated/protos/control/google.longrunning.rs");
        include!("control/generated/convert/longrunning/convert.rs");
    }
    pub mod r#type {
        include!("control/generated/protos/storage/google.r#type.rs");
        include!("control/generated/convert/type/convert.rs");
    }
    pub mod rpc {
        include!("control/generated/protos/storage/google.rpc.rs");
    }
    pub mod storage {
        #[allow(deprecated)]
        #[allow(clippy::large_enum_variant)]
        pub mod v2 {
            include!("control/generated/protos/storage/google.storage.v2.rs");
            include!("control/generated/convert/storage/convert.rs");
        }
        pub mod control {
            pub mod v2 {
                include!("control/generated/protos/control/google.storage.control.v2.rs");
                include!("control/generated/convert/control/convert.rs");
            }
        }
    }
}
