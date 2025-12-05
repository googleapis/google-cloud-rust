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

use crate::credentials::service_account::ServiceAccountKey;
use crate::signer::{Result, SigningError, dynamic::SigningProvider};

// Implements a local Signer using Service Account private key.
#[derive(Clone, Debug)]
pub(crate) struct ServiceAccountSigner {
    service_account_key: ServiceAccountKey,
    client_email: String,
}

impl ServiceAccountSigner {
    pub(crate) fn new(service_account_key: ServiceAccountKey) -> Self {
        Self {
            service_account_key: service_account_key.clone(),
            client_email: service_account_key.client_email.clone(),
        }
    }
    pub(crate) fn from_impersonated_service_account(
        service_account_key: ServiceAccountKey,
        client_email: String,
    ) -> Self {
        Self {
            service_account_key,
            client_email,
        }
    }
}

#[async_trait::async_trait]
impl SigningProvider for ServiceAccountSigner {
    async fn client_email(&self) -> Result<String> {
        Ok(self.client_email.clone())
    }

    async fn sign(&self, content: &[u8]) -> Result<bytes::Bytes> {
        let signer = self
            .service_account_key
            .signer()
            .map_err(SigningError::parsing)?;

        let signature = signer.sign(content).map_err(SigningError::sign)?;

        Ok(bytes::Bytes::from(signature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::tests::PKCS8_PK;
    use serde_json::{Value, json};

    type TestResult = anyhow::Result<()>;

    fn get_mock_service_key() -> Value {
        json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": "",
            "project_id": "test-project-id",
        })
    }

    #[tokio::test]
    async fn test_service_account_signer_success() -> TestResult {
        let mut service_account_key = get_mock_service_key();
        service_account_key["private_key"] = Value::from(PKCS8_PK.clone());
        let service_account_key =
            serde_json::from_value::<ServiceAccountKey>(service_account_key.clone())?;

        let signer = ServiceAccountSigner::new(service_account_key.clone());

        let client_email = signer.client_email().await?;
        assert_eq!(client_email, service_account_key.client_email);

        let result = signer.sign(b"test").await?;

        let inner_signer = service_account_key.signer().unwrap();
        let inner_result = inner_signer.sign(b"test")?;
        assert_eq!(result.as_ref(), inner_result);
        Ok(())
    }
}
