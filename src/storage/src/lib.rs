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

mod storage;

/// Clients to interact with Google Cloud Storage.
pub mod client {
    pub use crate::storage::client::{KeyAes256, KeyAes256Error, Storage};
    pub use control::client::StorageControl;
}
/// Request builders.
pub mod builder {
    pub mod storage {
        // TODO(#2403) - Move `ClientBuilder` into a scoped namespace within the
        // builder mod, like we do for GAPICs.
        pub use crate::storage::client::ClientBuilder;
        pub use crate::storage::read_object::ReadObject;
        pub use crate::storage::upload_object::UploadObject;
    }
    pub mod storage_control {
        pub use control::builder::storage_control::*;
        // TODO(#2403) - Move `ClientBuilder` into a scoped namespace within the
        // builder mod, like we do for GAPICs.
        pub use control::client::ClientBuilder;
    }
}
// TODO(#2403) - This includes implementation details like `ReadObjectRequest`.
// We do not want to expose those in the long run.
pub use control::model;
pub use control::stub;
