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

//! Spanner Omni instance types and configuration utilities.

use gaxi::grpc::tonic::transport::{Certificate, ClientTlsConfig, Identity};
use std::fs;
use std::io::Error as IoError;
use std::path::Path;
use thiserror::Error;
use url::Url;

pub use crate::client::SpannerBuilderExt;

/// Specifies the type of Spanner instance to connect to (`Cloud` or `Omni`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum InstanceType {
    /// Google Cloud Spanner instance (default).
    #[default]
    Cloud,
    /// Spanner Omni instance.
    Omni,
}

/// Configuration for Spanner Omni TLS / mTLS transport security.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    root_certificate: Option<Vec<u8>>,
    client_certificate: Option<Vec<u8>>,
    client_private_key: Option<Vec<u8>>,
    domain_name_override: Option<String>,
}

impl TlsConfig {
    /// Creates a new, empty TLS configuration.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::omni::TlsConfig;
    /// let config = TlsConfig::new();
    /// assert!(!config.has_custom_certificates());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures a custom Root CA certificate in PEM format to verify the Omni server's certificate.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::omni::TlsConfig;
    /// let ca_pem = b"-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----";
    /// let config = TlsConfig::new().with_root_certificate_pem(ca_pem);
    /// ```
    pub fn with_root_certificate_pem(mut self, root_certificate_pem: impl Into<Vec<u8>>) -> Self {
        self.root_certificate = Some(root_certificate_pem.into());
        self
    }

    /// Loads a custom Root CA certificate from a PEM file on disk.
    ///
    /// # Example
    /// ```no_run
    /// # use google_cloud_spanner::omni::{TlsConfig, TlsError};
    /// # fn sample() -> Result<(), TlsError> {
    /// let config = TlsConfig::new().with_root_certificate_file("path/to/ca.pem")?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Returns `TlsError::Io` if the file cannot be read.
    pub fn with_root_certificate_file(self, path: impl AsRef<Path>) -> Result<Self, TlsError> {
        let root_certificate_bytes = fs::read(path)?;
        Ok(self.with_root_certificate_pem(root_certificate_bytes))
    }

    /// Configures client certificate and private key in PEM format for mutual TLS (mTLS) authentication.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::omni::TlsConfig;
    /// let cert_pem = b"-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----";
    /// let key_pem = b"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----";
    /// let config = TlsConfig::new().with_client_certificate_pem(cert_pem, key_pem);
    /// ```
    pub fn with_client_certificate_pem(
        mut self,
        client_certificate_pem: impl Into<Vec<u8>>,
        client_private_key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_certificate = Some(client_certificate_pem.into());
        self.client_private_key = Some(client_private_key_pem.into());
        self
    }

    /// Loads client certificate and private key from PEM files on disk for mutual TLS (mTLS) authentication.
    ///
    /// # Example
    /// ```no_run
    /// # use google_cloud_spanner::omni::{TlsConfig, TlsError};
    /// # fn sample() -> Result<(), TlsError> {
    /// let config = TlsConfig::new()
    ///     .with_client_certificate_file("path/to/client.pem", "path/to/client.key")?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Returns `TlsError::Io` if either file cannot be read.
    pub fn with_client_certificate_file(
        self,
        certificate_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> Result<Self, TlsError> {
        let certificate_bytes = fs::read(certificate_path)?;
        let private_key_bytes = fs::read(private_key_path)?;
        Ok(self.with_client_certificate_pem(certificate_bytes, private_key_bytes))
    }

    /// Configures an explicit TLS domain name override (SNI / server name verification).
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::omni::TlsConfig;
    /// let config = TlsConfig::new().with_domain_name_override("spanner.internal");
    /// ```
    pub fn with_domain_name_override(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name_override = Some(domain_name.into());
        self
    }

    /// Returns the configured Root CA certificate bytes in PEM format, if any.
    pub fn root_certificate(&self) -> Option<&[u8]> {
        self.root_certificate.as_deref()
    }

    /// Returns the configured client certificate bytes in PEM format, if any.
    pub fn client_certificate(&self) -> Option<&[u8]> {
        self.client_certificate.as_deref()
    }

    /// Returns the configured client private key bytes in PEM format, if any.
    pub fn client_private_key(&self) -> Option<&[u8]> {
        self.client_private_key.as_deref()
    }

    /// Returns the configured TLS domain name override, if any.
    pub fn domain_name_override(&self) -> Option<&str> {
        self.domain_name_override.as_deref()
    }

    /// Returns `true` if any custom root CA or client certificates are configured.
    pub fn has_custom_certificates(&self) -> bool {
        self.root_certificate.is_some()
            || self.client_certificate.is_some()
            || self.client_private_key.is_some()
    }

    /// Validates that the TLS configuration is internally consistent.
    ///
    /// # Errors
    /// - Returns `TlsError::MissingClientKey` if a client certificate is set without a private key.
    /// - Returns `TlsError::MissingClientCertificate` if a client private key is set without a certificate.
    pub fn validate(&self) -> Result<(), TlsError> {
        match (&self.client_certificate, &self.client_private_key) {
            (Some(_), None) => Err(TlsError::MissingClientKey),
            (None, Some(_)) => Err(TlsError::MissingClientCertificate),
            _ => Ok(()),
        }
    }

    /// Converts this [`TlsConfig`] into a tonic [`ClientTlsConfig`] for transport connection.
    pub(crate) fn to_tonic_client_tls_config(&self) -> Option<ClientTlsConfig> {
        if !self.has_custom_certificates() && self.domain_name_override.is_none() {
            return None;
        }

        let mut tls = ClientTlsConfig::new();
        if let Some(root_certificate) = &self.root_certificate {
            let certificate = Certificate::from_pem(root_certificate);
            tls = tls.ca_certificate(certificate);
        } else {
            tls = tls.with_enabled_roots();
        }
        if let (Some(client_certificate), Some(client_private_key)) =
            (&self.client_certificate, &self.client_private_key)
        {
            let identity = Identity::from_pem(client_certificate, client_private_key);
            tls = tls.identity(identity);
        }
        if let Some(domain_name) = &self.domain_name_override {
            tls = tls.domain_name(domain_name);
        }
        Some(tls)
    }
}

/// Errors that can occur when configuring TLS / mTLS for Spanner Omni.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TlsError {
    /// Client certificate was provided without a corresponding private key.
    #[error("client certificate was provided without a client private key for mTLS")]
    MissingClientKey,

    /// Client private key was provided without a corresponding certificate.
    #[error("client private key was provided without a client certificate for mTLS")]
    MissingClientCertificate,

    /// Cannot use plaintext endpoint while providing TLS certificates.
    #[error("cannot use plaintext connection and provide TLS certificates at the same time")]
    PlaintextWithTls,

    /// I/O error occurred while reading certificate or key files.
    #[error("failed to read certificate or key file: {0}")]
    Io(#[from] IoError),
}

/// Helper function to check if an endpoint string is plaintext (does not use an `https://` scheme).
pub(crate) fn is_plaintext_endpoint(endpoint: &str) -> bool {
    let trimmed = endpoint.trim();
    match Url::parse(trimmed) {
        Ok(url) => url.scheme() != "https",
        Err(_) => !trimmed
            .get(..8)
            .is_some_and(|s| s.eq_ignore_ascii_case("https://")),
    }
}

/// Helper function to format database resource names for Spanner Omni.
///
/// If project or instance IDs are omitted, defaults to `projects/default/instances/default/databases/{database}`.
pub(crate) fn format_database_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return trimmed.to_string();
    }

    let parts: Vec<&str> = trimmed.split('/').collect();
    match parts.as_slice() {
        [
            "projects",
            _project,
            "instances",
            _instance,
            "databases",
            _database,
        ] => trimmed.to_string(),
        ["instances", instance, "databases", database] => {
            format!(
                "projects/default/instances/{}/databases/{}",
                instance, database
            )
        }
        ["databases", database] => {
            format!("projects/default/instances/default/databases/{}", database)
        }
        [database] => {
            format!("projects/default/instances/default/databases/{}", database)
        }
        _ => trimmed.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use static_assertions::assert_impl_all;
    use std::env::temp_dir;
    use std::error::Error as StdError;
    use std::fmt::Debug;
    use std::path::PathBuf;

    const SAMPLE_CA_PEM: &[u8] = include_bytes!("../../../testdata/tls/ca_cert.pem");
    const SAMPLE_CERT_PEM: &[u8] = include_bytes!("../../../testdata/tls/server_cert.pem");
    const SAMPLE_KEY_PEM: &[u8] = include_bytes!("../../../testdata/tls/server_key.pem");

    #[test]
    fn traits() {
        assert_impl_all!(InstanceType: Send, Sync, Debug, Clone, Copy, PartialEq, Eq);
        assert_impl_all!(TlsConfig: Send, Sync, Debug, Clone, Default, PartialEq, Eq);
        assert_impl_all!(TlsError: Send, Sync, Debug, StdError);
    }

    #[test]
    fn format_database_name_resolution() {
        assert_eq!(
            format_database_name("projects/p/instances/i/databases/d"),
            "projects/p/instances/i/databases/d"
        );
        assert_eq!(
            format_database_name("instances/i/databases/d"),
            "projects/default/instances/i/databases/d"
        );
        assert_eq!(
            format_database_name("databases/d"),
            "projects/default/instances/default/databases/d"
        );
        assert_eq!(
            format_database_name("retail-sample"),
            "projects/default/instances/default/databases/retail-sample"
        );
        assert_eq!(format_database_name(""), "");
    }

    #[test]
    fn plaintext_endpoint_detection() {
        assert!(is_plaintext_endpoint("http://localhost:15000"));
        assert!(is_plaintext_endpoint("HTTP://localhost:15000"));
        assert!(!is_plaintext_endpoint("https://spanner.internal:15000"));
        assert!(!is_plaintext_endpoint("HTTPS://spanner.internal:15000"));
        assert!(is_plaintext_endpoint("127.0.0.1:15000"));
        assert!(is_plaintext_endpoint("localhost:15000"));
        assert!(is_plaintext_endpoint("http://not a valid url:1234"));
        assert!(!is_plaintext_endpoint("https://not a valid url:1234"));
        assert!(!is_plaintext_endpoint("HTTPS://not a valid url:1234"));
        assert!(is_plaintext_endpoint(""));
        assert!(is_plaintext_endpoint("a"));
        assert!(is_plaintext_endpoint("https:"));
        assert!(is_plaintext_endpoint("🦀"));
    }

    fn create_test_temp_dir() -> PathBuf {
        let path = temp_dir().join(format!("spanner-omni-test-{}", rand::random::<u64>()));
        fs::create_dir_all(&path).expect("failed to create temporary test directory");
        path
    }

    #[test]
    fn tls_config_empty_is_valid() {
        let config = TlsConfig::new();
        assert!(!config.has_custom_certificates());
        assert!(config.root_certificate().is_none());
        assert!(config.client_certificate().is_none());
        assert!(config.client_private_key().is_none());
        assert!(config.domain_name_override().is_none());
        assert!(config.validate().is_ok());
        assert!(config.to_tonic_client_tls_config().is_none());
    }

    #[test]
    fn tls_config_with_domain_name_override_only() {
        let config = TlsConfig::new().with_domain_name_override("spanner.internal");
        assert!(!config.has_custom_certificates());
        assert_eq!(config.domain_name_override(), Some("spanner.internal"));
        assert!(config.validate().is_ok());
        assert!(config.to_tonic_client_tls_config().is_some());
    }

    #[test]
    fn tls_config_with_root_ca_pem() {
        let config = TlsConfig::new().with_root_certificate_pem(SAMPLE_CA_PEM);
        assert!(config.has_custom_certificates());
        assert_eq!(config.root_certificate(), Some(SAMPLE_CA_PEM));
        assert!(config.validate().is_ok());
        assert!(config.to_tonic_client_tls_config().is_some());
    }

    #[test]
    fn tls_config_with_root_ca_file() {
        let test_temp_dir = create_test_temp_dir();
        let ca_path = test_temp_dir.join("ca.pem");
        fs::write(&ca_path, SAMPLE_CA_PEM).expect("write ca.pem");

        let config = TlsConfig::new()
            .with_root_certificate_file(&ca_path)
            .expect("load root certificate file");
        assert_eq!(config.root_certificate(), Some(SAMPLE_CA_PEM));
        assert!(config.validate().is_ok());
        let _ = fs::remove_dir_all(&test_temp_dir);
    }

    #[test]
    fn tls_config_with_root_ca_missing_file_fails() {
        let result = TlsConfig::new().with_root_certificate_file("/nonexistent/path/to/ca.pem");
        assert!(
            result.is_err(),
            "expected error when Root CA file does not exist"
        );
    }

    #[test]
    fn tls_config_with_mtls_pem() {
        let config = TlsConfig::new()
            .with_root_certificate_pem(SAMPLE_CA_PEM)
            .with_client_certificate_pem(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)
            .with_domain_name_override("spanner.internal");

        assert!(config.has_custom_certificates());
        assert_eq!(config.root_certificate(), Some(SAMPLE_CA_PEM));
        assert_eq!(config.client_certificate(), Some(SAMPLE_CERT_PEM));
        assert_eq!(config.client_private_key(), Some(SAMPLE_KEY_PEM));
        assert_eq!(config.domain_name_override(), Some("spanner.internal"));
        assert!(config.validate().is_ok());
        assert!(config.to_tonic_client_tls_config().is_some());
    }

    #[test]
    fn tls_config_with_mtls_files() {
        let test_temp_dir = create_test_temp_dir();
        let cert_path = test_temp_dir.join("client.pem");
        let key_path = test_temp_dir.join("client.key");
        fs::write(&cert_path, SAMPLE_CERT_PEM).expect("write client.pem");
        fs::write(&key_path, SAMPLE_KEY_PEM).expect("write client.key");

        let config = TlsConfig::new()
            .with_client_certificate_file(&cert_path, &key_path)
            .expect("load client certificate and key files");

        assert_eq!(config.client_certificate(), Some(SAMPLE_CERT_PEM));
        assert_eq!(config.client_private_key(), Some(SAMPLE_KEY_PEM));
        assert!(config.validate().is_ok());
        let _ = fs::remove_dir_all(&test_temp_dir);
    }

    #[test]
    fn tls_config_client_cert_without_key_fails_validation() {
        let mut config = TlsConfig::new();
        config.client_certificate = Some(SAMPLE_CERT_PEM.to_vec());
        let validation_result = config.validate();
        assert!(
            matches!(validation_result, Err(TlsError::MissingClientKey)),
            "expected MissingClientKey, got {:?}",
            validation_result
        );
    }

    #[test]
    fn tls_config_client_key_without_cert_fails_validation() {
        let mut config = TlsConfig::new();
        config.client_private_key = Some(SAMPLE_KEY_PEM.to_vec());
        let validation_result = config.validate();
        assert!(
            matches!(validation_result, Err(TlsError::MissingClientCertificate)),
            "expected MissingClientCertificate, got {:?}",
            validation_result
        );
    }

    #[test]
    fn tls_config_missing_client_cert_file_fails() {
        let test_temp_dir = create_test_temp_dir();
        let key_path = test_temp_dir.join("client.key");
        fs::write(&key_path, SAMPLE_KEY_PEM).expect("write client.key");

        let result =
            TlsConfig::new().with_client_certificate_file("/nonexistent/client.pem", &key_path);
        assert!(
            result.is_err(),
            "expected error when client certificate file does not exist"
        );
        let _ = fs::remove_dir_all(&test_temp_dir);
    }

    #[test]
    fn tls_config_missing_client_key_file_fails() {
        let test_temp_dir = create_test_temp_dir();
        let cert_path = test_temp_dir.join("client.pem");
        fs::write(&cert_path, SAMPLE_CERT_PEM).expect("write client.pem");

        let result =
            TlsConfig::new().with_client_certificate_file(&cert_path, "/nonexistent/client.key");
        assert!(
            result.is_err(),
            "expected error when client private key file does not exist"
        );
        let _ = fs::remove_dir_all(&test_temp_dir);
    }

    #[tokio::test]
    async fn spanner_with_instance_type() {
        let spanner = Spanner::builder()
            .with_instance_type(InstanceType::Omni)
            .build()
            .await
            .expect("build client");
        assert_eq!(spanner.instance_type(), InstanceType::Omni);
    }

    #[tokio::test]
    async fn spanner_with_omni_tls_automatically_sets_omni_instance_type() {
        let tls_config = TlsConfig::new().with_root_certificate_pem(SAMPLE_CA_PEM);
        let spanner = Spanner::builder()
            .with_endpoint("https://spanner.internal:15000")
            .with_omni_tls(tls_config)
            .build()
            .await
            .expect("build client");
        assert_eq!(spanner.instance_type(), InstanceType::Omni);
    }

    #[tokio::test]
    async fn spanner_builder_with_omni_tls_populates_tonic_tls_extension() {
        let tls_config = TlsConfig::new()
            .with_root_certificate_pem(SAMPLE_CA_PEM)
            .with_client_certificate_pem(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)
            .with_domain_name_override("spanner.internal");
        let spanner = Spanner::builder()
            .with_endpoint("https://spanner.internal:15000")
            .with_omni_tls(tls_config)
            .build()
            .await
            .expect("build client");
        assert_eq!(spanner.instance_type(), InstanceType::Omni);
        assert!(
            spanner.config.extensions.get::<ClientTlsConfig>().is_some(),
            "expected ClientTlsConfig to be inserted into extensions for transport channel"
        );
    }

    #[tokio::test]
    async fn spanner_plaintext_endpoint_with_tls_certificates_fails() {
        let tls_config = TlsConfig::new().with_root_certificate_pem(SAMPLE_CA_PEM);
        let result = Spanner::builder()
            .with_endpoint("http://localhost:15000")
            .with_omni_tls(tls_config)
            .build()
            .await;
        assert!(
            result.is_err(),
            "expected error when combining plaintext endpoint with TLS certificates"
        );
    }

    #[tokio::test]
    async fn spanner_invalid_tls_config_fails_build() {
        let mut invalid_tls_config = TlsConfig::new();
        invalid_tls_config.client_certificate = Some(SAMPLE_CERT_PEM.to_vec());
        let result = Spanner::builder()
            .with_omni_tls(invalid_tls_config)
            .build()
            .await;
        assert!(
            result.is_err(),
            "expected builder error when TLS configuration fails validation"
        );
    }

    #[tokio::test]
    #[ignore = "requires live Omni instance at localhost:15000"]
    async fn query_local_omni_instance() {
        let spanner = Spanner::builder()
            .with_endpoint("http://localhost:15000")
            .with_instance_type(InstanceType::Omni)
            .build()
            .await
            .expect("build client");

        let database_client = spanner
            .database_client("retail-sample")
            .build()
            .await
            .expect("build db client");

        let transaction = database_client.single_use().build();
        let mut result_set = transaction
            .execute_query("SELECT * FROM Products LIMIT 5")
            .await
            .expect("execute query");

        let mut count = 0;
        while let Some(row) = result_set.next().await {
            let row = row.expect("read row");
            println!("Fetched Omni row {}: {:?}", count, row);
            count += 1;
        }
        println!("Successfully queried Omni! Total rows fetched: {}", count);
    }
}
