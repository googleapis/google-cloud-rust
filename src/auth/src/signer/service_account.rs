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

use crate::credentials::service_account::ServiceAccountKey;
use crate::signer::{Result, SigningError, SigningProvider};

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

    async fn sign(&self, content: &[u8]) -> Result<String> {
        let signer = self
            .service_account_key
            .signer()
            .map_err(SigningError::parsing)?;

        let signature = signer.sign(content).map_err(SigningError::sign)?;

        let signature = hex::encode(signature);

        Ok(signature)
    }
}

#[cfg(test)]
mod tests {}
