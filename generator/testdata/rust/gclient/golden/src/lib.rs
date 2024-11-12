#![allow(dead_code)]

use std::sync::Arc;

pub mod model;

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
        req: model::ListSecretsRequest,
    ) -> Result<model::ListSecretsResponse, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::ListSecretsResponse>().await?;
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
        req: model::AddSecretVersionRequest,
    ) -> Result<model::SecretVersion, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::SecretVersion>().await?;
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
        req: model::UpdateSecretRequest,
    ) -> Result<model::Secret, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::Secret>().await?;
        Ok(response)
    }

    /// Lists [SecretVersions][google.cloud.secretmanager.v1.SecretVersion]. This
    /// call does not return secret data.
    pub async fn list_secret_versions(
        &self,
        req: model::ListSecretVersionsRequest,
    ) -> Result<model::ListSecretVersionsResponse, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::ListSecretVersionsResponse>().await?;
        Ok(response)
    }

    /// Gets metadata for a
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// `projects/*/secrets/*/versions/latest` is an alias to the most recently
    /// created [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    pub async fn get_secret_version(
        &self,
        req: model::GetSecretVersionRequest,
    ) -> Result<model::SecretVersion, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::SecretVersion>().await?;
        Ok(response)
    }

    /// Accesses a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    /// This call returns the secret data.
    ///
    /// `projects/*/secrets/*/versions/latest` is an alias to the most recently
    /// created [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    pub async fn access_secret_version(
        &self,
        req: model::AccessSecretVersionRequest,
    ) -> Result<model::AccessSecretVersionResponse, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::AccessSecretVersionResponse>().await?;
        Ok(response)
    }

    /// Disables a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// Sets the [state][google.cloud.secretmanager.v1.SecretVersion.state] of the
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion] to
    /// [DISABLED][google.cloud.secretmanager.v1.SecretVersion.State.DISABLED].
    pub async fn disable_secret_version(
        &self,
        req: model::DisableSecretVersionRequest,
    ) -> Result<model::SecretVersion, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::SecretVersion>().await?;
        Ok(response)
    }

    /// Enables a [SecretVersion][google.cloud.secretmanager.v1.SecretVersion].
    ///
    /// Sets the [state][google.cloud.secretmanager.v1.SecretVersion.state] of the
    /// [SecretVersion][google.cloud.secretmanager.v1.SecretVersion] to
    /// [ENABLED][google.cloud.secretmanager.v1.SecretVersion.State.ENABLED].
    pub async fn enable_secret_version(
        &self,
        req: model::EnableSecretVersionRequest,
    ) -> Result<model::SecretVersion, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::SecretVersion>().await?;
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
        req: model::DestroySecretVersionRequest,
    ) -> Result<model::SecretVersion, Box<dyn std::error::Error>> {
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
        let response = res.json::<model::SecretVersion>().await?;
        Ok(response)
    }
}
