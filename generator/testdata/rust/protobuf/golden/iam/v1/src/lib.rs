// Copyright 2024 Google LLC
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
//
// Code generated by sidekick. DO NOT EDIT.

/// The messages and enums that are part of this client library.
pub mod model;

/// Common error returned by the RPCs in this client library.
use gax::error::Error;

/// The traits implemented by this client library.
#[allow(rustdoc::invalid_html_tags)]
#[allow(rustdoc::redundant_explicit_links)]
pub mod traits;

/// Concrete implementations of this client library traits.
pub mod client;

/// Request builders.
pub mod builders;

#[doc(hidden)]
pub(crate) mod tracing;

#[doc(hidden)]
pub(crate) mod transport;

/// The default host used by the service.
const DEFAULT_HOST: &str = "https://iam-meta-api.googleapis.com/";

pub(crate) mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gax::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gax::api_header::GAPIC, 
            };
            ac.header_value()
        };
    }
}

/// A `Result` alias where the `Err` case is an [Error].
pub type Result<T> = std::result::Result<T, Error>;

pub type ConfigBuilder = gax::http_client::ClientConfig;
