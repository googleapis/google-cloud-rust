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

mod core_error;
mod http_error;
mod service_error;
pub use core_error::*;
pub use http_error::*;
pub use service_error::*;

/// Errors and error details returned by Service RPCs.
///
/// The Google Cloud Client Libraries for Rust distinguishes between errors detected while
/// trying to send a RPC (e.g. cannot open a connection), errors trying to
/// receive a response (e.g. the connection is dropped before the full response),
/// and errors returned by the service itself.
///
/// The types in this module represent detailed information returned by the
/// Gooogle Cloud services.
///
/// # Examples
///
/// ```
/// # use std::result::Result;
/// # use gcp_sdk_gax::error;
/// use error::Error;
/// use error::ServiceError;
/// use error::rpc::Status;
/// fn handle_error(e: Error) {
///     if let Some(e) = e.as_inner::<ServiceError>() {
///         let status : Status = e.status().clone();
///         println!("{status:?}")
///     }
/// }
/// ```
pub mod rpc;
