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

    /// Stores sensitive data such as API keys, passwords, and certificates.
    /// Provides convenience while improving security.
    pub fn google_cloud_secretmanager_v_1_secret_manager_service(
        &self,
    ) -> GoogleCloudSecretmanagerV1SecretManagerService {
        GoogleCloudSecretmanagerV1SecretManagerService {
            client: self.clone(),
            base_path: "https://https://secretmanager.googleapis.com/".to_string(),
        }
    }
}

/// Stores sensitive data such as API keys, passwords, and certificates.
/// Provides convenience while improving security.
#[derive(Debug)]
pub struct GoogleCloudSecretmanagerV1SecretManagerService {
    client: Client,
    base_path: String,
}

impl GoogleCloudSecretmanagerV1SecretManagerService {
    /// Lists information about the supported locations for this service.
    pub async fn list_locations(
        &self,
        req: crate::model::ListLocationsRequest,
    ) -> Result<crate::model::ListLocationsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [
            gax::query_parameter::format("filter", &req.filter)?,
            gax::query_parameter::format("pageSize", &req.page_size)?,
            gax::query_parameter::format("pageToken", &req.page_token)?,
        ];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations",
                self.base_path, req.project,
            ))
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
        let response = res.json::<crate::model::ListLocationsResponse>().await?;
        Ok(response)
    }

    /// Gets information about a location.
    pub async fn get_location(
        &self,
        req: crate::model::GetLocationRequest,
    ) -> Result<crate::model::Location, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}",
                self.base_path, req.project, req.location,
            ))
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
        let response = res.json::<crate::model::Location>().await?;
        Ok(response)
    }

    /// Lists Secrets.
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
            .get(format!(
                "{}/v1/projects/{}/secrets",
                self.base_path, req.project,
            ))
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

    /// Creates a new Secret containing no SecretVersions.
    pub async fn create_secret(
        &self,
        req: crate::model::Secret,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format("secretId", &req.secret_id)?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets",
                self.base_path, req.project,
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
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Lists Secrets.
    pub async fn list_secrets_by_project_and_location(
        &self,
        req: crate::model::ListSecretsByProjectAndLocationRequest,
    ) -> Result<crate::model::ListSecretsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [
            gax::query_parameter::format("pageSize", &req.page_size)?,
            gax::query_parameter::format("pageToken", &req.page_token)?,
            gax::query_parameter::format("filter", &req.filter)?,
        ];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets",
                self.base_path, req.project, req.location,
            ))
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

    /// Creates a new Secret containing no SecretVersions.
    pub async fn create_secret_by_project_and_location(
        &self,
        req: crate::model::Secret,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format("secretId", &req.secret_id)?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets",
                self.base_path, req.project, req.location,
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
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Creates a new SecretVersion containing secret data and attaches
    /// it to an existing Secret.
    pub async fn add_secret_version(
        &self,
        req: crate::model::AddSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}:addVersion",
                self.base_path, req.project, req.secret,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Creates a new SecretVersion containing secret data and attaches
    /// it to an existing Secret.
    pub async fn add_secret_version_by_project_and_location_and_secret(
        &self,
        req: crate::model::AddSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:addVersion",
                self.base_path, req.project, req.location, req.secret,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Gets metadata for a given Secret.
    pub async fn get_secret(
        &self,
        req: crate::model::GetSecretRequest,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}",
                self.base_path, req.project, req.secret,
            ))
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

    /// Deletes a Secret.
    pub async fn delete_secret(
        &self,
        req: crate::model::DeleteSecretRequest,
    ) -> Result<crate::model::Empty, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format("etag", &req.etag)?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .delete(format!(
                "{}/v1/projects/{}/secrets/{}",
                self.base_path, req.project, req.secret,
            ))
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
        let response = res.json::<crate::model::Empty>().await?;
        Ok(response)
    }

    /// Updates metadata of an existing Secret.
    pub async fn update_secret(
        &self,
        req: crate::model::Secret,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format(
            "updateMask",
            &req.update_mask,
        )?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .patch(format!(
                "{}/v1/projects/{}/secrets/{}",
                self.base_path, req.project, req.secret,
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
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Gets metadata for a given Secret.
    pub async fn get_secret_by_project_and_location_and_secret(
        &self,
        req: crate::model::GetSecretByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}",
                self.base_path, req.project, req.location, req.secret,
            ))
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

    /// Deletes a Secret.
    pub async fn delete_secret_by_project_and_location_and_secret(
        &self,
        req: crate::model::DeleteSecretByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Empty, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format("etag", &req.etag)?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .delete(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}",
                self.base_path, req.project, req.location, req.secret,
            ))
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
        let response = res.json::<crate::model::Empty>().await?;
        Ok(response)
    }

    /// Updates metadata of an existing Secret.
    pub async fn update_secret_by_project_and_location_and_secret(
        &self,
        req: crate::model::Secret,
    ) -> Result<crate::model::Secret, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format(
            "updateMask",
            &req.update_mask,
        )?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .patch(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}",
                self.base_path, req.project, req.location, req.secret,
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
        let response = res.json::<crate::model::Secret>().await?;
        Ok(response)
    }

    /// Lists SecretVersions. This call does not return secret
    /// data.
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
            .get(format!(
                "{}/v1/projects/{}/secrets/{}/versions",
                self.base_path, req.project, req.secret,
            ))
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

    /// Lists SecretVersions. This call does not return secret
    /// data.
    pub async fn list_secret_versions_by_project_and_location_and_secret(
        &self,
        req: crate::model::ListSecretVersionsByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::ListSecretVersionsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [
            gax::query_parameter::format("pageSize", &req.page_size)?,
            gax::query_parameter::format("pageToken", &req.page_token)?,
            gax::query_parameter::format("filter", &req.filter)?,
        ];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions",
                self.base_path, req.project, req.location, req.secret,
            ))
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

    /// Gets metadata for a SecretVersion.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn get_secret_version(
        &self,
        req: crate::model::GetSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}",
                self.base_path, req.project, req.secret, req.version,
            ))
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

    /// Gets metadata for a SecretVersion.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn get_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::GetSecretVersionByProjectAndLocationAndSecretAndVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
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

    /// Accesses a SecretVersion. This call returns the secret data.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn access_secret_version(
        &self,
        req: crate::model::AccessSecretVersionRequest,
    ) -> Result<crate::model::AccessSecretVersionResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:access",
                self.base_path, req.project, req.secret, req.version,
            ))
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

    /// Accesses a SecretVersion. This call returns the secret data.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn access_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::AccessSecretVersionByProjectAndLocationAndSecretAndVersionRequest,
    ) -> Result<crate::model::AccessSecretVersionResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:access",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
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

    /// Disables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// DISABLED.
    pub async fn disable_secret_version(
        &self,
        req: crate::model::DisableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:disable",
                self.base_path, req.project, req.secret, req.version,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Disables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// DISABLED.
    pub async fn disable_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::DisableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:disable",
                self.base_path, req.project, req.location, req.secret, req.version,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Enables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// ENABLED.
    pub async fn enable_secret_version(
        &self,
        req: crate::model::EnableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:enable",
                self.base_path, req.project, req.secret, req.version,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Enables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// ENABLED.
    pub async fn enable_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::EnableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:enable",
                self.base_path, req.project, req.location, req.secret, req.version,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Destroys a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// DESTROYED and irrevocably destroys the
    /// secret data.
    pub async fn destroy_secret_version(
        &self,
        req: crate::model::DestroySecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:destroy",
                self.base_path, req.project, req.secret, req.version,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Destroys a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// DESTROYED and irrevocably destroys the
    /// secret data.
    pub async fn destroy_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::DestroySecretVersionRequest,
    ) -> Result<crate::model::SecretVersion, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:destroy",
                self.base_path, req.project, req.location, req.secret, req.version,
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
        let response = res.json::<crate::model::SecretVersion>().await?;
        Ok(response)
    }

    /// Sets the access control policy on the specified secret. Replaces any
    /// existing policy.
    ///
    /// Permissions on SecretVersions are enforced according
    /// to the policy set on the associated Secret.
    pub async fn set_iam_policy(
        &self,
        req: crate::model::SetIamPolicyRequest,
    ) -> Result<crate::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}:setIamPolicy",
                self.base_path, req.project, req.secret,
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
        let response = res.json::<crate::model::Policy>().await?;
        Ok(response)
    }

    /// Sets the access control policy on the specified secret. Replaces any
    /// existing policy.
    ///
    /// Permissions on SecretVersions are enforced according
    /// to the policy set on the associated Secret.
    pub async fn set_iam_policy_by_project_and_location_and_secret(
        &self,
        req: crate::model::SetIamPolicyRequest,
    ) -> Result<crate::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:setIamPolicy",
                self.base_path, req.project, req.location, req.secret,
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
        let response = res.json::<crate::model::Policy>().await?;
        Ok(response)
    }

    /// Gets the access control policy for a secret.
    /// Returns empty policy if the secret exists and does not have a policy set.
    pub async fn get_iam_policy(
        &self,
        req: crate::model::GetIamPolicyRequest,
    ) -> Result<crate::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format(
            "options.requestedPolicyVersion",
            &req.options_requested_policy_version,
        )?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}:getIamPolicy",
                self.base_path, req.project, req.secret,
            ))
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
        let response = res.json::<crate::model::Policy>().await?;
        Ok(response)
    }

    /// Gets the access control policy for a secret.
    /// Returns empty policy if the secret exists and does not have a policy set.
    pub async fn get_iam_policy_by_project_and_location_and_secret(
        &self,
        req: crate::model::GetIamPolicyByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [gax::query_parameter::format(
            "options.requestedPolicyVersion",
            &req.options_requested_policy_version,
        )?];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:getIamPolicy",
                self.base_path, req.project, req.location, req.secret,
            ))
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
        let response = res.json::<crate::model::Policy>().await?;
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
        req: crate::model::TestIamPermissionsRequest,
    ) -> Result<crate::model::TestIamPermissionsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}:testIamPermissions",
                self.base_path, req.project, req.secret,
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
        let response = res
            .json::<crate::model::TestIamPermissionsResponse>()
            .await?;
        Ok(response)
    }

    /// Returns permissions that a caller has for the specified secret.
    /// If the secret does not exist, this call returns an empty set of
    /// permissions, not a NOT_FOUND error.
    ///
    /// Note: This operation is designed to be used for building permission-aware
    /// UIs and command-line tools, not for authorization checking. This operation
    /// may "fail open" without warning.
    pub async fn test_iam_permissions_by_project_and_location_and_secret(
        &self,
        req: crate::model::TestIamPermissionsRequest,
    ) -> Result<crate::model::TestIamPermissionsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:testIamPermissions",
                self.base_path, req.project, req.location, req.secret,
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
        let response = res
            .json::<crate::model::TestIamPermissionsResponse>()
            .await?;
        Ok(response)
    }
}
