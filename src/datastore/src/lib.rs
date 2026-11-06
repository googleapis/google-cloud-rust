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

//! Google Cloud Client Libraries for Rust - Datastore
//!
//! **WARNING:** this crate is under active development. We expect multiple
//! breaking changes in the upcoming releases. Testing is also incomplete, we do
//! **not** recommend that you use this crate in production. We welcome feedback
//! about the APIs, documentation, missing features, bugs, etc.
//!
//! This crate contains traits, types, and functions to interact with
//! [Datastore].
//!
//! For administrative control-plane operations (such as managing composite
//! indexes and importing/exporting entities), see the [admin] module, which
//! re-exports [`DatastoreAdmin`][admin::client::DatastoreAdmin].
//!
//! # Features
//!
//! - `default-rustls-provider`: enabled by default. Use the default rustls
//!   crypto provider ([aws-lc-rs]) for TLS and authentication. Applications
//!   with specific requirements for cryptography (such as exclusively using the
//!   [ring] crate) should disable this default and call
//!   `rustls::crypto::CryptoProvider::install_default()`.
//!
//! [aws-lc-rs]: https://crates.io/crates/aws-lc-rs
//! [datastore]: https://cloud.google.com/datastore
//! [ring]: https://crates.io/crates/ring

#![cfg_attr(docsrs, feature(doc_cfg))]

#[allow(rustdoc::broken_intra_doc_links)]
pub(crate) mod generated;

pub use google_cloud_gax::Result;
pub use google_cloud_gax::error::Error;

// Internal shortcuts required by the generated gapic_dataplane module.
pub(crate) use google_cloud_gax::client_builder::Result as ClientBuilderResult;
pub(crate) use google_cloud_gax::options::RequestOptions;
pub(crate) use google_cloud_gax::options::internal::RequestBuilder;
pub(crate) use google_cloud_gax::response::Response;

pub(crate) const DEFAULT_HOST: &str = "https://datastore.googleapis.com";

mod info {
    use std::sync::LazyLock;

    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub(crate) static X_GOOG_API_CLIENT_HEADER: LazyLock<String> = LazyLock::new(|| {
        let ac = gaxi::api_header::XGoogApiClient {
            name: NAME,
            version: VERSION,
            library_type: gaxi::api_header::GAPIC,
        };
        ac.grpc_header_value()
    });
}

#[allow(dead_code)]
pub(crate) mod model {
    pub(crate) use crate::generated::gapic_dataplane::model::*;
}

/// Control-plane clients, builders, and models for Cloud Datastore
/// Administration (`google.datastore.admin.v1`).
///
/// Re-exports [`google_cloud_datastore_admin_v1`] for managing indexes,
/// importing/exporting entities via Long-Running Operations, and inspecting
/// Datastore-to-Firestore migration progress.
pub mod admin {
    pub use google_cloud_datastore_admin_v1::builder;
    pub use google_cloud_datastore_admin_v1::client;
    pub use google_cloud_datastore_admin_v1::model;
    pub use google_cloud_datastore_admin_v1::stub;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_host_and_header() {
        assert_eq!(DEFAULT_HOST, "https://datastore.googleapis.com");
        assert!(!info::X_GOOG_API_CLIENT_HEADER.is_empty());
    }

    #[tokio::test]
    async fn admin_reexport_and_internal_gapic_construct() -> anyhow::Result<()> {
        let creds = google_cloud_auth::credentials::anonymous::Builder::new().build();
        let _admin = admin::client::DatastoreAdmin::builder()
            .with_credentials(creds.clone())
            .build()
            .await?;
        let mut config = gaxi::options::ClientConfig::default();
        config.cred = Some(creds);
        let _dataplane = generated::gapic_dataplane::client::Datastore::new(config).await?;
        Ok(())
    }
}
