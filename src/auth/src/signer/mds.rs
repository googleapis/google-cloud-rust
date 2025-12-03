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

use crate::credentials::Credentials;
use crate::credentials::mds::{MDS_DEFAULT_URI, METADATA_FLAVOR, METADATA_FLAVOR_VALUE};
use crate::signer::{Result, SigningError, dynamic::SigningProvider};
use http::HeaderValue;
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::RwLock;

// Implements Signer for MDS that extends the existing CredentialsSigner by fetching
// email via MDS email endpoint.
#[derive(Clone, Debug)]
pub(crate) struct MDSCredentialsSigner {
    endpoint: String,
    client_email: Arc<RwLock<String>>,
    inner: Credentials,
}

impl MDSCredentialsSigner {
    pub(crate) fn new(endpoint: String, inner: Credentials) -> Self {
        Self {
            endpoint,
            client_email: Arc::new(RwLock::new(String::new())),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl SigningProvider for MDSCredentialsSigner {
    async fn client_email(&self) -> Result<String> {
        let mut client_email = self
            .client_email
            .try_write()
            .map_err(|_e| SigningError::transport("failed to obtain lock to read client email"))?;

        if client_email.is_empty() {
            let client = Client::new();

            let request = client
                .get(format!("{}{}/email", self.endpoint, MDS_DEFAULT_URI))
                .header(
                    METADATA_FLAVOR,
                    HeaderValue::from_static(METADATA_FLAVOR_VALUE),
                );

            let response = request.send().await.map_err(SigningError::transport)?;
            let email = response.text().await.map_err(SigningError::transport)?;

            *client_email = email.clone();
        }

        Ok(client_email.clone())
    }

    async fn sign(&self, content: &[u8]) -> Result<String> {
        let client_email = self.client_email().await?;

        let signer = crate::signer::CredentialsSigner {
            client_email,
            inner: self.inner.clone(),
        };

        signer.sign(content).await
    }
}

#[cfg(test)]
mod tests {}
