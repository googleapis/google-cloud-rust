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

//! Google APIs eXtensions for Rust.
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains a number of types and functions used in the
//! implementation of the Google Cloud Client Libraries for Rust.
//!
//! <div class="warning">
//! All the types, traits, and functions defined in any module with `internal`
//! in its name are <b>not</b> intended for general use. Such symbols will
//! remain unstable for the foreseeable future, even if used in stable SDKs.
//! We (the Google Cloud Client Libraries for Rust team) control both and will
//! change both if needed.
//! </div>

#![cfg_attr(docsrs, feature(doc_cfg))]

/// An alias of [std::result::Result] where the error is always [Error][crate::error::Error].
///
/// This is the result type used by all functions wrapping RPCs.
pub type Result<T> = std::result::Result<T, crate::error::Error>;

/// The core error types used by generated clients.
pub mod error;

pub mod paginator;

pub mod response;

pub mod backoff_policy;
pub mod client_builder;
pub mod exponential_backoff;
pub mod options;
pub mod polling_backoff_policy;
pub mod polling_error_policy;
pub mod polling_state;
pub mod retry_policy;
pub mod retry_result;
pub mod retry_state;
pub mod retry_throttler;
pub mod throttle_result;

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
pub mod retry_loop_internal;

#[cfg(test)]
pub(crate) mod mock_rng;
