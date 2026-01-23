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
//! # Features
//!
//! - `idtoken`: disabled by default, this feature enables support to create and
//!   verify [OIDC ID Tokens].
//! - `default-idtoken-backend`: enabled by default, this feature enables a default
//!   backend for the `idtoken` feature. Currently the feature is implemented using
//!   the [jsonwebtoken] crate and uses `rust_crypto` as its default backend. We may
//!   change the default backend at any time, applications that have specific needs
//!   for this backend should not rely on the current default. To control the
//!   backend selection:
//!   - Configure this crate with `default-features = false`, and
//!     `features = ["idtoken"]`
//!   - Configure the `jsonwebtoken` crate to use the desired backend.
//!
//! [jsonwebtoken]: https://crates.io/crates/jsonwebtoken
//! [oidc id tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
//! [Authentication methods at Google]: https://cloud.google.com/docs/authentication
//! [Principals]: https://cloud.google.com/docs/authentication#principal
//! [Tokens]: https://cloud.google.com/docs/authentication#token
//! [Credentials]: https://cloud.google.com/docs/authentication#credentials

pub mod build_errors;
pub(crate) mod constants;
pub mod credentials;
pub mod errors;
pub(crate) mod headers_util;
pub(crate) mod mds;
pub(crate) mod retry;
pub mod signer;
pub(crate) mod token;
pub(crate) mod token_cache;

/// A `Result` alias where the `Err` case is [BuildCredentialsError].
pub(crate) type BuildResult<T> = std::result::Result<T, build_errors::Error>;

/// A `Result` alias where the `Err` case is [CredentialsError][errors::CredentialsError].
pub(crate) type Result<T> = std::result::Result<T, errors::CredentialsError>;
