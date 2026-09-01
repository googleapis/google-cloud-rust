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

//! Implements the common features of all gRPC-based client.

pub mod from_status;
pub(crate) mod grpc_helpers;
#[cfg(google_cloud_unstable_grpc_rust)]
pub mod grpc_rust;
pub mod status;
pub(crate) mod streaming;
pub mod tonic;
pub(crate) mod tracing_attributes;
pub(crate) mod transport_policies;

#[cfg(google_cloud_unstable_grpc_rust)]
pub use grpc_rust::{GrpcRustClient, GrpcRustStreaming};
pub use tracing_attributes::TracingAttributes;

#[cfg(all(google_cloud_unstable_grpc_rust, feature = "_internal-grpc-rust"))]
pub use grpc_rust::{GrpcRustClient as Client, GrpcRustStreaming as Streaming};

#[cfg(not(all(google_cloud_unstable_grpc_rust, feature = "_internal-grpc-rust")))]
pub use tonic::{Client, Streaming};

use google_cloud_gax::Result;
use google_cloud_gax::error::Error;
use google_cloud_gax::response::{Parts, Response};

/// Convert a `tonic::Response` wrapping a prost message into a
/// `google_cloud_gax::response::Response` wrapping our equivalent message
pub fn to_gax_response<T, G>(response: ::tonic::Response<T>) -> Result<Response<G>>
where
    T: crate::prost::FromProto<G>,
{
    let (metadata, body, _extensions) = response.into_parts();
    Ok(Response::from_parts(
        Parts::new().set_headers(metadata.into_headers()),
        body.cnv().map_err(Error::deser)?,
    ))
}
