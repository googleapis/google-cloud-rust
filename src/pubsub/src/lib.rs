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

//! Google Cloud Client Libraries for Rust - Pub/Sub
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [Pub/Sub].
//!
//! [pub/sub]: https://cloud.google.com/pubsub

pub(crate) mod generated;

pub(crate) mod publisher;

pub use gax::Result;
pub use gax::error::Error;

pub mod builder {
    pub use crate::generated::gapic::builder::*;
    pub mod publisher {
        pub use crate::generated::gapic_dataplane::builder::publisher::*;
        pub use crate::publisher::client::ClientBuilder;
        pub use crate::publisher::publisher::PublisherBuilder;
    }
}
pub mod model {
    pub use crate::generated::gapic::model::*;
    pub use crate::generated::gapic_dataplane::model::*;
}

pub mod model_ext {
    pub use crate::publisher::model_ext::*;
}

pub mod client {
    pub use crate::generated::gapic::client::*;
    pub use crate::publisher::client::PublisherClient;
    pub use crate::publisher::publisher::Publisher;
}
pub mod stub {
    pub use crate::generated::gapic::stub::*;
}
pub mod options {
    pub mod publisher {
        pub use crate::publisher::options::BatchingOptions;
    }
}

const DEFAULT_HOST: &str = "https://pubsub.googleapis.com";

mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gaxi::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gaxi::api_header::GAPIC,
            };
            ac.grpc_header_value()
        };
    }
}

#[allow(dead_code)]
pub(crate) mod google {
    pub mod pubsub {
        #[allow(clippy::enum_variant_names)]
        pub mod v1 {
            include!("generated/protos/pubsub/google.pubsub.v1.rs");
            include!("generated/convert/pubsub/convert.rs");
        }
    }
}
