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

use crate::credentials::{CacheableResource, Credentials};
use http::Extensions;
use reqwest::Client;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, SigningError>;

/// An implementation of [crate::credentials::SigningProvider].
#[derive(Clone, Debug)]
pub struct Signer {
    pub(crate) inner: Arc<dyn dynamic::SigningProvider>,
}

impl<T> std::convert::From<T> for Signer
where
    T: SigningProvider + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }
}

impl Signer {
    pub async fn client_email(&self) -> Result<String> {
        self.inner.client_email().await
    }

    pub async fn sign(self, content: &str) -> Result<String> {
        self.inner.sign(content).await
    }
}

// Implements Signer using IAM signBlob API and reusing using existing [Credentials] to
// authenticate to it.
#[derive(Clone, Debug)]
pub(crate) struct CredentialsSigner {
    pub(crate) client_email: String,
    pub(crate) inner: Credentials,
}

#[derive(serde::Serialize)]
struct SignBlobRequest {
    payload: String,
}

#[derive(Debug, serde::Deserialize)]
struct SignBlobResponse {
    #[serde(rename = "signedBlob")]
    signed_blob: String,
}

#[async_trait::async_trait]
impl SigningProvider for CredentialsSigner {
    async fn client_email(&self) -> Result<String> {
        Ok(self.client_email.clone())
    }

    async fn sign(&self, content: &str) -> Result<String> {
        use base64::{Engine, prelude::BASE64_STANDARD};

        let source_headers = self
            .inner
            .headers(Extensions::new())
            .await
            .map_err(SigningError::transport)?;
        let source_headers = match source_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => {
                unreachable!("requested source credentials without a caching etag")
            }
        };

        let client_email = self.client_email.clone();
        let url = format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:signBlob",
            client_email
        );

        let client = Client::new();
        let payload = BASE64_STANDARD.encode(content);
        let body = SignBlobRequest { payload };

        let response = client
            .post(url)
            .header("Content-Type", "application/json")
            .headers(source_headers)
            .json(&body)
            .send()
            .await
            .map_err(|e| SigningError::transport(e))?;

        if !response.status().is_success() {
            let err_text = response.text().await.map_err(SigningError::transport)?;
            return Err(SigningError::transport(format!("err status: {err_text:?}")));
        }

        let res = response
            .json::<SignBlobResponse>()
            .await
            .map_err(SigningError::parsing)?;

        let signature = BASE64_STANDARD
            .decode(res.signed_blob)
            .map_err(SigningError::parsing)?;

        let signature = hex::encode(signature);

        Ok(signature)
    }
}

#[async_trait::async_trait]
pub trait SigningProvider: Send + Sync + std::fmt::Debug {
    // represents the authorizer of the signed URL generation.
    // It is typically the Google service account client email address from the Google Developers Console in the form of "xxx@developer.gserviceaccount.com". Required.
    async fn client_email(&self) -> Result<String>;
    // creates a signed URL using the v4 schema.
    async fn sign(&self, content: &str) -> Result<String>;
}

pub(crate) mod dynamic {
    use super::Result;

    /// A dyn-compatible, crate-private version of `SigningProvider`.
    #[async_trait::async_trait]
    pub trait SigningProvider: Send + Sync + std::fmt::Debug {
        async fn client_email(&self) -> Result<String>;
        async fn sign(&self, content: &str) -> Result<String>;
    }

    /// The public CredentialsProvider implements the dyn-compatible CredentialsProvider.
    #[async_trait::async_trait]
    impl<T> SigningProvider for T
    where
        T: super::SigningProvider + Send + Sync,
    {
        async fn client_email(&self) -> Result<String> {
            T::client_email(self).await
        }

        async fn sign(&self, content: &str) -> Result<String> {
            T::sign(self, content).await
        }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct SigningError(SigningErrorKind);

impl SigningError {
    /// A problem using API to sign blob.
    pub fn is_transport(&self) -> bool {
        matches!(self.0, SigningErrorKind::Transport(_))
    }

    /// A problem parsing a credentials JSON specification.
    pub fn is_parsing(&self) -> bool {
        matches!(self.0, SigningErrorKind::Parsing(_))
    }

    /// A problem parsing a credentials specification.
    pub(crate) fn parsing<T>(source: T) -> SigningError
    where
        T: Into<BoxError>,
    {
        SigningError(SigningErrorKind::Parsing(source.into()))
    }

    /// A problem using API to sign blob.
    pub(crate) fn transport<T>(source: T) -> SigningError
    where
        T: Into<BoxError>,
    {
        SigningError(SigningErrorKind::Transport(source.into()))
    }
}

#[derive(thiserror::Error, Debug)]
enum SigningErrorKind {
    #[error("failed to generate signature via IAM API: {0}")]
    Transport(#[source] BoxError),
    #[error("failed to parse credentials: {0}")]
    Parsing(#[source] BoxError),
}
