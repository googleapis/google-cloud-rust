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
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [Storage].
//!
//! [storage]: https://cloud.google.com/storage

pub use gax::Result;
pub use gax::error::Error;
#[allow(dead_code)]
// TODO(#1813) - fix the broken link to [here].
#[allow(rustdoc::broken_intra_doc_links)]
// TODO(#1813) - fix x-goog-request-params and this is not needed
#[allow(clippy::op_ref)]
#[allow(clippy::needless_borrow)]
pub(crate) mod generated;

pub use generated::gapic::builder;
pub use generated::gapic::client;
pub use generated::gapic::model;

pub(crate) mod google {
    pub mod iam {
        pub mod v1 {
            include!("generated/protos/storage/google.iam.v1.rs");
        }
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
        pub mod v2 {
            include!("generated/protos/storage/google.storage.v2.rs");
            include!("generated/convert/storage/convert.rs");
        }
    }
}

impl gaxi::prost::Convert<google::rpc::Status> for rpc::model::Status {
    fn cnv(self) -> google::rpc::Status {
        google::rpc::Status {
            code: self.code.cnv(),
            message: self.message.cnv(),
            // TODO(#...) - detail with the error details
            ..Default::default()
        }
    }
}

impl gaxi::prost::Convert<rpc::model::Status> for google::rpc::Status {
    fn cnv(self) -> rpc::model::Status {
        rpc::model::Status::new()
            .set_code(self.code)
            .set_message(self.message)
        // TODO(#...) - detail with the error details
        // .set_details(self.details.into_iter().filter_map(any_from_prost))
    }
}
