// Copyright 2026 Google LLC
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

//! # Introduction
//!
//! The 1.5.0 release introduces different features to control the default
//! [rustls] crypto provider, applications using `google-cloud-auth` with the
//! default features disabled[^1] may need to enable some features to select
//! the right crypto provider. This guide discusses how to change your
//! application to use the new features.
//!
//! # Using default crypto provider
//!
//! If you have no preference about what crypto provider, just add the
//! `default-rustls-provider` feature. Edit your `Cargo.toml` file, for example,
//! change the `google-cloud-auth` dependency from:
//!
//! ```toml
//! [dependencies.google-cloud-auth]
//! version = "1"
//! default-features = false
//! ```
//!
//! to:
//!
//! ```toml
//! [dependencies.google-cloud-auth]
//! version = "1"
//! default-features = false
//! features = ["default-rustls-provider"]
//! ```
//!
//! # Selecting your own crypto provider
//!
//! The current default crypto provider is [ring]. This default may change in
//! the future. If you need to use a different crypto provider or want to
//! isolate your application from future changes on the default provider, then
//! you need to change your `main()` function to install a different provider.
//!
//! Furthermore, you may want to disable the default provider to prune the
//! `ring` dependency from your dependency graph.
//!
//! In this guide we will use [aws-lc-rs]. The changes to use other providers
//! are similar. Consult the `rustls` documentation to find out about available
//! providers.
//!
//! First, modify your `Cargo.toml` to disable the default provider and to
//! depend on `rustls` with the desired provider enabled:
//!
//! ```toml
//! [dependencies]
//! google-cloud-auth = { version = "1", default-features = false }
//! rustls            = { version = "0.23", features = ["aws_lc_rs"] }
//! ```
//!
//! <div class="warning">
//! You must use the same version of `rustls` as `google-cloud-auth`.
//! </div>
//!
//! Then change your `main()` function to install this provider:
//!
//! ```
//! use rustls::crypto::{CryptoProvider, aws_lc_rs::default_provider};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Install a default crypto provider.
//!     CryptoProvider::install_default(default_provider())
//!         .map_err(|_| anyhow::anyhow!("default crypto provider already installed"))?;
//!     // ... ... ...
//!     Ok(())
//! }
//! ```
//!
//! [^1]: Either via `cargo add --no-default-features` or via
//!       `default-features = false` in your `Cargo.toml` file.
//!
//! [aws-lc-rs]: https://crates.io/crates/aws-lc-rs
//! [ring]: https://crates.io/crates/ring
//! [rustls]: https://crates.io/crates/rustls
