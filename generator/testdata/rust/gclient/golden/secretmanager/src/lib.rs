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

/// The messages and enums that are part of this client library.
pub mod model;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientRef>,
}

#[derive(Debug)]
struct ClientRef {
    http_client: reqwest::Client,
    token: String,
}

impl Client {
    pub fn new(tok: String) -> Self {
        let client = reqwest::Client::builder().build().unwrap();
        let inner = ClientRef {
            http_client: client,
            token: tok,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Secret Manager Service
    ///
    /// Manages secrets and operations using those secrets. Implements a REST
    /// model with the following objects:
    ///
    /// * [Secret][google.cloud.secretmanager.v1.Secret]
    /// * [SecretVersion][google.cloud.secretmanager.v1.SecretVersion]
    pub fn secret_manager_service(&self) -> SecretManagerService {
        SecretManagerService {
            client: self.clone(),
            base_path: "https://secretmanager.googleapis.com/".to_string(),
        }
    }
}

/// Secret Manager Service
///
/// Manages secrets and operations using those secrets. Implements a REST
/// model with the following objects:
///
/// * [Secret][google.cloud.secretmanager.v1.Secret]
/// * [SecretVersion][google.cloud.secretmanager.v1.SecretVersion]
#[derive(Debug)]
pub struct SecretManagerService {
    client: Client,
    base_path: String,
}

impl SecretManagerService {
    /// Lists [Secrets][google.cloud.secretmanager.v1.Secret].
    pub async fn list_secrets(
        &self,
        req: crate::model::ListSecretsRequest,
    ) -> Result<crate::model::ListSecretsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [
            gax::query_parameter::format("pageSize", &req.page_size)?,
            gax::query_parameter::format("pageToken", &req.page_token)?,
            gax::query_parameter::format("filter", &req.filter)?,
        ];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!("{}/v1/{}/secrets", self.base_path, req.parent,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::ListSecretsResponse>().await?;
        Ok(response)
    }

    /// Creates a new [Secret][google.cloud.secretmanager.v1.Secret] containing no
    /// [SecretVersions][google.cloud.secretmanager.v1.SecretVersion].
    pub async fn create_secret(
        &self,
        req: crate::model::CreateSecretRequest,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format("secretId", &req.secret_id)?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!("{}/v1/{}/secrets", self.base_path, req.parent,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req.secret)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Creates a new [SecretVersion][google.cloud.secretmanager.v1.SecretVersion]
    /// containing secret data and attaches it to an existing
    /// [Secret][google.cloud.secretmanager.v1.Secret].
    pub async fn add_secret_version(
        &self,
        req: crate::model::AddSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!("{}/v1/{}:addVersion", self.base_path, req.parent,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Gets metadata for a given [Secret][google.cloud.secretmanager.v1.Secret].
    pub async fn get_secret(
        &self,
        req: crate::model::GetSecretRequest,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!("{}/v1/{}", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Updates metadata of an existing
    /// [Secret][google.cloud.secretmanager.v1.Secret].
    pub async fn update_secret(
        &self,
        req: crate::model::UpdateSecretRequest,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format(
            "updateMask",
            &req.update_mask,
        )?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .patch(format!(
                "{}/v1/{}",
                self.base_path,
                gax::path_parameter::PathParameter::required(&req.secret, "secret")?.name,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req.secret)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Deletes a [Secret][google.cloud.secretmanager.v1.Secret].
    pub async fn delete_secret(
        &self,
        req: crate::model::DeleteSecretRequest,
    ) -> Result<wkt::Empty, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format("etag", &req.etag)?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .delete(format!("{}/v1/{}", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<wkt::Empty>().await?;
        Ok(response)
    }

    /// Lists [SecretVersions][google.cloud.secretmanager.v1.SecretVersion]. This
    /// call does not return secret data.
    pub async fn list_secret_versions(
        &self,
        req: crate::model::ListSecretVersionsRequest,
    ) -> Result<crate::model::ListSecretVersionsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [
            gax::query_parameter::format("pageSize", &req.page_size)?,
            gax::query_parameter::format("pageToken", &req.page_token)?,
            gax::query_parameter::format("filter", &req.filter)?,
        ];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!("{}/v1/{}/versions", self.base_path, req.parent,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res
            .json::<crate::model::ListSecretVersionsResponse>()
            .await?;
        Ok(response)
    }

    /// Gets metadata for a
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// `projects/*/secrets/*/versions/latest` is an alias to the most recently
    /// created [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    pub async fn get_secret_version(
        &self,
        req: crate::model::GetSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!("{}/v1/{}", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Accesses a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    /// This call returns the secret data.
    ///
    /// `projects/*/secrets/*/versions/latest` is an alias to the most recently
    /// created [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    pub async fn access_secret_version(
        &self,
        req: crate::model::AccessSecretVersionRequest,
    ) -> Result<crate::model::AccessSecretVersionResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!("{}/v1/{}:access", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res
            .json::<crate::model::AccessSecretVersionResponse>()
            .await?;
        Ok(response)
    }

    /// Disables a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// Sets the [state][google.cloud.secretmanager.v1.SecretVersion.state] of the
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion] to
    /// [DISABLED][google.cloud.secretmanager.v1.SecretVersion.State.DISABLED].
    pub async fn disable_secret_version(
        &self,
        req: crate::model::DisableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!("{}/v1/{}:disable", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Enables a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// Sets the [state][google.cloud.secretmanager.v1.SecretVersion.state] of the
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion] to
    /// [ENABLED][google.cloud.secretmanager.v1.SecretVersion.State.ENABLED].
    pub async fn enable_secret_version(
        &self,
        req: crate::model::EnableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!("{}/v1/{}:enable", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Destroys a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// Sets the [state][google.cloud.secretmanager.v1.SecretVersion.state] of the
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion] to
    /// [DESTROYED][google.cloud.secretmanager.v1.SecretVersion.State.DESTROYED]
    /// and irrevocably destroys the secret data.
    pub async fn destroy_secret_version(
        &self,
        req: crate::model::DestroySecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!("{}/v1/{}:destroy", self.base_path, req.name,))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Sets the access control policy on the specified secret. Replaces any
    /// existing policy.
    ///
    /// Permissions on
    /// [SecretVersions][google.cloud.secretmanager.v1.SecretVersion] are enforced
    /// according to the policy set on the associated
    /// [Secret][google.cloud.secretmanager.v1.Secret].
    pub async fn set_iam_policy(
        &self,
        req: iam::model::SetIamPolicyRequest,
    ) -> Result<iam::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/{}:setIamPolicy",
                self.base_path, req.resource,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<iam::model::Policy>().await?;
        Ok(response)
    }

    /// Gets the access control policy for a secret.
    /// Returns empty policy if the secret exists and does not have a policy set.
    pub async fn get_iam_policy(
        &self,
        req: iam::model::GetIamPolicyRequest,
    ) -> Result<iam::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/{}:getIamPolicy",
                self.base_path, req.resource,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<iam::model::Policy>().await?;
        Ok(response)
    }

    /// Returns permissions that a caller has for the specified secret.
    /// If the secret does not exist, this call returns an empty set of
    /// permissions, not a NOT_FOUND error.
    ///
    /// Note: This operation is designed to be used for building permission-aware
    /// UIs and command-line tools, not for authorization checking. This operation
    /// may "fail open" without warning.
    pub async fn test_iam_permissions(
        &self,
        req: iam::model::TestIamPermissionsRequest,
    ) -> Result<iam::model::TestIamPermissionsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/{}:testIamPermissions",
                self.base_path, req.resource,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<iam::model::TestIamPermissionsResponse>().await?;
        Ok(response)
    }
}
