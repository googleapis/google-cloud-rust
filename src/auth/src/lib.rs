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

pub mod build_errors;
pub mod errors;

/// A `Result` alias where the `Err` case is [BuildCredentialsError].
pub(crate) type BuildResult<T> = std::result::Result<T, build_errors::Error>;

/// Types and functions to work with Google Cloud authentication [Credentials].
///
/// [Credentials]: https://cloud.google.com/docs/authentication#credentials
pub mod credentials;

pub(crate) mod constants;

pub(crate) mod token;

/// The token cache
pub(crate) mod token_cache;

/// A `Result` alias where the `Err` case is [CredentialsError][errors::CredentialsError].
pub(crate) type Result<T> = std::result::Result<T, errors::CredentialsError>;

/// The retry module
pub(crate) mod retry;

/// Headers utility functions to work with Google Cloud authentication [Credentials].
///
/// [Credentials]: https://cloud.google.com/docs/authentication#credentials
pub(crate) mod headers_util;

pub mod signer;
