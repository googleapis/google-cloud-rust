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

//! Google APIs helpers.
//!
//! This crate contains a number of types and functions used in the
//! implementation of the Google Cloud Client Libraries for Rust.
//!
//! <div class="warning">
//! All the types, traits, and functions defined in the <code>unstable-sdk-client</code>
//! feature are <b>not</b> intended for general use. The APIs enabled by this
//! feature will remain unstable for the foreseeable future, even if used in
//! stable SDKs. We (the Google Cloud Client Libraries for Rust team) control both and will
//! change both if needed.
//! </div>

/// An alias of [std::result::Result] where the error is always [crate::error::Error].
///
/// This is the result type used by all functions wrapping RPCs.
pub type Result<T> = std::result::Result<T, crate::error::Error>;

#[cfg(feature = "unstable-sdk-client")]
#[doc(hidden)]
pub mod query_parameter;

/// Defines traits and helpers to serialize path parameters.
///
/// Path parameters in the Google APIs are always required, but they may need
/// to be source from fields that are inside a message field, which are always
/// `Option<T>`.
///
/// This module defines some traits and helpers to simplify the code generator.
/// They automatically convert `Option<T>` to `Result<T, Error>`, so the
/// generator always writes:
///
/// gax::path_parameter::required(req.field)?.sub
///
/// If accessing deeply nested fields that can results in multiple calls to
/// `required`.
#[cfg(feature = "unstable-sdk-client")]
#[doc(hidden)]
pub mod path_parameter;

/// Implementation details for [query_parameter](::crate::query_parameter) and
/// [path_parameter](::crate::path_parameter).
#[cfg(feature = "unstable-sdk-client")]
#[doc(hidden)]
mod request_parameter;

/// Implements helpers to create telemetry headers.
#[cfg(feature = "unstable-sdk-client")]
#[doc(hidden)]
pub mod api_header;

/// The core error types used by generated clients.
pub mod error;

/// Defines some types and traits to convert and use List RPCs as a Stream.
/// Async streams are not yet stable, so neither is the use of this feature.
#[cfg(feature = "unstable-stream")]
pub mod paginator;

/// Defines traits and helpers for HTTP client implementations.
#[cfg(feature = "unstable-sdk-client")]
#[doc(hidden)]
pub mod http_client;

pub mod options;
pub mod retry_policy;
