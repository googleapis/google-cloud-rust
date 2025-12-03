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

use crate::credentials::{CacheableResource, Credentials};
use crate::signer::{Result, SigningError, dynamic::SigningProvider};
use http::Extensions;
use reqwest::Client;

// Implements Signer using IAM signBlob API and reusing using existing [Credentials] to
// authenticate to it.
#[derive(Clone, Debug)]
pub(crate) struct IamSigner {
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
impl SigningProvider for IamSigner {
    async fn client_email(&self) -> Result<String> {
        Ok(self.client_email.clone())
    }

    async fn sign(&self, content: &[u8]) -> Result<String> {
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
            .map_err(SigningError::transport)?;

        if !response.status().is_success() {
            let err_text = response.text().await.map_err(SigningError::transport)?;
            return Err(SigningError::transport(format!("err status: {err_text:?}")));
        }

        let res = response
            .json::<SignBlobResponse>()
            .await
            .map_err(SigningError::transport)?;

        let signature = BASE64_STANDARD
            .decode(res.signed_blob)
            .map_err(SigningError::transport)?;

        let signature = hex::encode(signature);

        Ok(signature)
    }
}
