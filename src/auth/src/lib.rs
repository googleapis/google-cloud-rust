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

//! Google Cloud Client Libraries for Rust - Authentication Components
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains types and functions used to authenticate applications
//! on Google Cloud.  The SDK clients consume an implementation of
//! [credentials::Credentials] and use these credentials to authenticate RPCs
//! issued by the application.
//!
//! [Authentication methods at Google] is a good introduction on the topic of
//! authentication for Google Cloud services and other Google products. The
//! guide also describes the common terminology used with authentication, such
//! as [Principals], [Tokens], and [Credentials].
//!
//! [Authentication methods at Google]: https://cloud.google.com/docs/authentication
//! [Principals]: https://cloud.google.com/docs/authentication#principal
//! [Tokens]: https://cloud.google.com/docs/authentication#token
//! [Credentials]: https://cloud.google.com/docs/authentication#credentials

pub mod errors;

/// Types and functions to work with Google Cloud authentication [Credentials].
///
/// [Credentials]: https://cloud.google.com/docs/authentication#credentials
pub mod credentials;

/// Types and functions to work with auth [Tokens].
///
/// [Tokens]: https://cloud.google.com/docs/authentication#token
pub mod token;

/// The token cache
pub(crate) mod token_cache;

/// A `Result` alias where the `Err` case is
/// `google_cloud_auth::errors::CredentialsError`.
pub(crate) type Result<T> = std::result::Result<T, crate::errors::CredentialsError>;

/// Headers utility functions to work with Google Cloud authentication [Credentials].
///
/// [Credentials]: https://cloud.google.com/docs/authentication#credentials
pub(crate) mod headers_util;

pub(crate) mod http;
