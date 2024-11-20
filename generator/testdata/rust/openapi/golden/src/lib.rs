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

use gax::error::{Error, HttpError};
use std::sync::Arc;

/// A `Result` alias where the `Err` case is an [Error].
pub type Result<T> = std::result::Result<T, Error>;

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
            base_path: "https://secretmanager.googleapis.com/".to_string(),
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
    ) -> Result<crate::model::ListLocationsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations",
                self.base_path, req.project,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "filter", &req.filter).map_err(Error::other)?;
        let builder =
            gax::query_parameter::add(builder, "pageSize", &req.page_size).map_err(Error::other)?;
        let builder = gax::query_parameter::add(builder, "pageToken", &req.page_token)
            .map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::ListLocationsResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets information about a location.
    pub async fn get_location(
        &self,
        req: crate::model::GetLocationRequest,
    ) -> Result<crate::model::Location> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}",
                self.base_path, req.project, req.location,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Location>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Lists Secrets.
    pub async fn list_secrets(
        &self,
        req: crate::model::ListSecretsRequest,
    ) -> Result<crate::model::ListSecretsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets",
                self.base_path, req.project,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "pageSize", &req.page_size).map_err(Error::other)?;
        let builder = gax::query_parameter::add(builder, "pageToken", &req.page_token)
            .map_err(Error::other)?;
        let builder =
            gax::query_parameter::add(builder, "filter", &req.filter).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::ListSecretsResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Creates a new Secret containing no SecretVersions.
    pub async fn create_secret(
        &self,
        req: crate::model::CreateSecretRequest,
    ) -> Result<crate::model::Secret> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets",
                self.base_path, req.project,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "secretId", &req.secret_id).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .json(&req.request_body)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Secret>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Lists Secrets.
    pub async fn list_secrets_by_project_and_location(
        &self,
        req: crate::model::ListSecretsByProjectAndLocationRequest,
    ) -> Result<crate::model::ListSecretsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets",
                self.base_path, req.project, req.location,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "pageSize", &req.page_size).map_err(Error::other)?;
        let builder = gax::query_parameter::add(builder, "pageToken", &req.page_token)
            .map_err(Error::other)?;
        let builder =
            gax::query_parameter::add(builder, "filter", &req.filter).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::ListSecretsResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Creates a new Secret containing no SecretVersions.
    pub async fn create_secret_by_project_and_location(
        &self,
        req: crate::model::CreateSecretByProjectAndLocationRequest,
    ) -> Result<crate::model::Secret> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets",
                self.base_path, req.project, req.location,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "secretId", &req.secret_id).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .json(&req.request_body)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Secret>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Creates a new SecretVersion containing secret data and attaches
    /// it to an existing Secret.
    pub async fn add_secret_version(
        &self,
        req: crate::model::AddSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}:addVersion",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Creates a new SecretVersion containing secret data and attaches
    /// it to an existing Secret.
    pub async fn add_secret_version_by_project_and_location_and_secret(
        &self,
        req: crate::model::AddSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:addVersion",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets metadata for a given Secret.
    pub async fn get_secret(
        &self,
        req: crate::model::GetSecretRequest,
    ) -> Result<crate::model::Secret> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Secret>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Deletes a Secret.
    pub async fn delete_secret(
        &self,
        req: crate::model::DeleteSecretRequest,
    ) -> Result<crate::model::Empty> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .delete(format!(
                "{}/v1/projects/{}/secrets/{}",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "etag", &req.etag).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Empty>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Updates metadata of an existing Secret.
    pub async fn update_secret(
        &self,
        req: crate::model::UpdateSecretRequest,
    ) -> Result<crate::model::Secret> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .patch(format!(
                "{}/v1/projects/{}/secrets/{}",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder = gax::query_parameter::add(
            builder,
            "updateMask",
            &serde_json::to_value(&req.update_mask).map_err(Error::serde)?,
        )
        .map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .json(&req.request_body)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Secret>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets metadata for a given Secret.
    pub async fn get_secret_by_project_and_location_and_secret(
        &self,
        req: crate::model::GetSecretByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Secret> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Secret>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Deletes a Secret.
    pub async fn delete_secret_by_project_and_location_and_secret(
        &self,
        req: crate::model::DeleteSecretByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Empty> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .delete(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "etag", &req.etag).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Empty>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Updates metadata of an existing Secret.
    pub async fn update_secret_by_project_and_location_and_secret(
        &self,
        req: crate::model::UpdateSecretByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Secret> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .patch(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder = gax::query_parameter::add(
            builder,
            "updateMask",
            &serde_json::to_value(&req.update_mask).map_err(Error::serde)?,
        )
        .map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .json(&req.request_body)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Secret>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Lists SecretVersions. This call does not return secret
    /// data.
    pub async fn list_secret_versions(
        &self,
        req: crate::model::ListSecretVersionsRequest,
    ) -> Result<crate::model::ListSecretVersionsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}/versions",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "pageSize", &req.page_size).map_err(Error::other)?;
        let builder = gax::query_parameter::add(builder, "pageToken", &req.page_token)
            .map_err(Error::other)?;
        let builder =
            gax::query_parameter::add(builder, "filter", &req.filter).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::ListSecretVersionsResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Lists SecretVersions. This call does not return secret
    /// data.
    pub async fn list_secret_versions_by_project_and_location_and_secret(
        &self,
        req: crate::model::ListSecretVersionsByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::ListSecretVersionsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder =
            gax::query_parameter::add(builder, "pageSize", &req.page_size).map_err(Error::other)?;
        let builder = gax::query_parameter::add(builder, "pageToken", &req.page_token)
            .map_err(Error::other)?;
        let builder =
            gax::query_parameter::add(builder, "filter", &req.filter).map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::ListSecretVersionsResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets metadata for a SecretVersion.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn get_secret_version(
        &self,
        req: crate::model::GetSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}",
                self.base_path, req.project, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets metadata for a SecretVersion.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn get_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::GetSecretVersionByProjectAndLocationAndSecretAndVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Accesses a SecretVersion. This call returns the secret data.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn access_secret_version(
        &self,
        req: crate::model::AccessSecretVersionRequest,
    ) -> Result<crate::model::AccessSecretVersionResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:access",
                self.base_path, req.project, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::AccessSecretVersionResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Accesses a SecretVersion. This call returns the secret data.
    ///
    /// `projects/_*_/secrets/_*_/versions/latest` is an alias to the most recently
    /// created SecretVersion.
    pub async fn access_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::AccessSecretVersionByProjectAndLocationAndSecretAndVersionRequest,
    ) -> Result<crate::model::AccessSecretVersionResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:access",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::AccessSecretVersionResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Disables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// DISABLED.
    pub async fn disable_secret_version(
        &self,
        req: crate::model::DisableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:disable",
                self.base_path, req.project, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Disables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// DISABLED.
    pub async fn disable_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::DisableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:disable",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Enables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// ENABLED.
    pub async fn enable_secret_version(
        &self,
        req: crate::model::EnableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:enable",
                self.base_path, req.project, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Enables a SecretVersion.
    ///
    /// Sets the state of the SecretVersion to
    /// ENABLED.
    pub async fn enable_secret_version_by_project_and_location_and_secret_and_version(
        &self,
        req: crate::model::EnableSecretVersionRequest,
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:enable",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
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
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}/versions/{}:destroy",
                self.base_path, req.project, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
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
    ) -> Result<crate::model::SecretVersion> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}/versions/{}:destroy",
                self.base_path, req.project, req.location, req.secret, req.version,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::SecretVersion>()
            .await
            .map_err(Error::serde)?;
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
    ) -> Result<crate::model::Policy> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}:setIamPolicy",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Policy>()
            .await
            .map_err(Error::serde)?;
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
    ) -> Result<crate::model::Policy> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:setIamPolicy",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Policy>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets the access control policy for a secret.
    /// Returns empty policy if the secret exists and does not have a policy set.
    pub async fn get_iam_policy(
        &self,
        req: crate::model::GetIamPolicyRequest,
    ) -> Result<crate::model::Policy> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/secrets/{}:getIamPolicy",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder = gax::query_parameter::add(
            builder,
            "options.requestedPolicyVersion",
            &req.options_requested_policy_version,
        )
        .map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Policy>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }

    /// Gets the access control policy for a secret.
    /// Returns empty policy if the secret exists and does not have a policy set.
    pub async fn get_iam_policy_by_project_and_location_and_secret(
        &self,
        req: crate::model::GetIamPolicyByProjectAndLocationAndSecretRequest,
    ) -> Result<crate::model::Policy> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .get(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:getIamPolicy",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let builder = gax::query_parameter::add(
            builder,
            "options.requestedPolicyVersion",
            &req.options_requested_policy_version,
        )
        .map_err(Error::other)?;
        let res = builder
            .bearer_auth(&client.token)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::Policy>()
            .await
            .map_err(Error::serde)?;
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
    ) -> Result<crate::model::TestIamPermissionsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/secrets/{}:testIamPermissions",
                self.base_path, req.project, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::TestIamPermissionsResponse>()
            .await
            .map_err(Error::serde)?;
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
    ) -> Result<crate::model::TestIamPermissionsResponse> {
        let client = self.client.inner.clone();
        let builder = client
            .http_client
            .post(format!(
                "{}/v1/projects/{}/locations/{}/secrets/{}:testIamPermissions",
                self.base_path, req.project, req.location, req.secret,
            ))
            .query(&[("alt", "json")]);
        let res = builder
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await
            .map_err(Error::io)?;
        if !res.status().is_success() {
            let status = res.status().as_u16();
            let headers = gax::error::convert_headers(res.headers());
            let body = res.bytes().await.map_err(Error::io)?;
            return Err(HttpError::new(status, headers, Some(body)).into());
        }
        let response = res
            .json::<crate::model::TestIamPermissionsResponse>()
            .await
            .map_err(Error::serde)?;
        Ok(response)
    }
}
