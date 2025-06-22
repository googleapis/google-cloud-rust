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
pub use core_error::*;
mod credentials;
pub use credentials::CredentialsError;

/// Errors and error details related to local path validation.
///
/// These errors occur when required fields in a request are either missing, or
/// are present, but in an invalid format. The client fails these requests
/// locally because it does not know how to send such requests.
pub mod binding;

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
/// # use google_cloud_gax::error;
/// use error::Error;
/// use error::rpc::Status;
/// fn handle_error(e: Error) {
///     if let Some(status) = e.status() {
///         println!("the service reported {status:?}")
///     }
/// }
/// ```
pub mod rpc;
