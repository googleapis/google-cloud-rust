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

pub(crate) mod checksum;
pub(crate) mod client;
pub(crate) mod common_options;
pub(crate) mod perform_upload;
pub(crate) mod read_object;
pub mod request_options;
pub mod streaming_source;
pub mod stub;
pub(crate) mod transport;
pub(crate) mod v1;
pub(crate) mod write_object;

use crate::model::Object;
use crate::streaming_source::Payload;
use crate::{Error, Result};

/// The default host used by the service.
const DEFAULT_HOST: &str = "https://storage.googleapis.com";

pub(crate) mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gaxi::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gaxi::api_header::GCCL,
            };
            ac.grpc_header_value()
        };
    }
}
