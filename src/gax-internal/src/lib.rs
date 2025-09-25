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

//! Implementation details for Google Cloud clients.
//!
//! All the types, traits, and functions defined in this crate are **not**
//! intended for general use. This crate will remain unstable for the
//! foreseeable future, even if used in the implementation for stable client
//! libraries. We (the Google Cloud Client Libraries for Rust team) control
//! both and will change both if needed.
//!
//! The types, traits, and functions defined in this crate are undocumented.
//! This is intentional, as they are not intended for general use and will be
//! changed without notice.

#[cfg(feature = "_internal-common")]
pub mod api_header;

#[cfg(feature = "_internal-common")]
pub mod path_parameter;

#[cfg(feature = "_internal-http-client")]
pub mod query_parameter;

#[cfg(feature = "_internal-http-client")]
pub mod http;

#[cfg(feature = "_internal-http-client")]
pub mod observability;

#[cfg(feature = "_internal-grpc-client")]
pub mod grpc;

#[cfg(feature = "_internal-grpc-client")]
pub mod prost;

#[cfg(feature = "_internal-common")]
pub mod options;

#[cfg(feature = "_internal-common")]
pub mod unimplemented;

#[cfg(feature = "_internal-common")]
pub mod routing_parameter;

// TODO(#3375) - use host logic in gRPC too.
#[cfg(feature = "_internal-http-client")]
pub(crate) mod host;

#[cfg(feature = "_internal-grpc-client")]
pub(crate) mod google {
    pub mod rpc {
        include!("generated/protos/rpc/google.rpc.rs");
        include!("generated/convert/rpc/convert.rs");
    }
}
