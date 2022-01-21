// Copyright 2022 Google LLC
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

#![allow(dead_code)]

use google_cloud_auth::{Credential, CredentialConfig};
use serde::Deserialize;
use std::error::Error as StdError;
use std::sync::Arc;

mod bytes;
pub use crate::bytes::BytesReader;
pub mod model;

const BASE_PATH: &str = "https:/storage.googleapis.com/storage/v1/";
const MTLS_BASE_PATH: &str = "https:/storage.mtls.googleapis.com/storage/v1/";

fn default_scopes() -> Vec<String> {
    vec![
        "https://www.googleapis.com/auth/cloud-platform".to_string(),
        "https://www.googleapis.com/auth/cloud-platform.read-only".to_string(),
        "https://www.googleapis.com/auth/devstorage.full_control".to_string(),
        "https://www.googleapis.com/auth/devstorage.read_only".to_string(),
        "https://www.googleapis.com/auth/devstorage.read_write".to_string(),
    ]
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientRef>,
}

struct ClientRef {
    http_client: reqwest::Client,
    base_path: String,
    cred: Credential,
}

impl std::fmt::Debug for ClientRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRef")
            .field("http_client", &self.http_client)
            .field("base_path", &self.base_path)
            .finish()
    }
}

impl Default for ClientRef {
    fn default() -> Self {
        let mut headers = http::HeaderMap::with_capacity(1);
        headers.insert("User-Agent", "gcloud-rust/0.1".parse().unwrap());
        headers.insert(
            "x-goog-api-client",
            format!(
                "gl-rust/{}  gdcl/0.1",
                rustc_version_runtime::version().to_string()
            )
            .parse()
            .unwrap(),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();
        Self {
            http_client: client,
            base_path: BASE_PATH.into(),
            cred: Credential::default(),
        }
    }
}

impl Client {
    pub async fn new() -> Result<Client> {
        let cc = CredentialConfig::builder()
            .scopes(default_scopes())
            .build()
            .map_err(Error::wrap)?;
        let cred = Credential::find_default(cc).await.map_err(Error::wrap)?;
        let mut headers = http::HeaderMap::with_capacity(1);
        headers.insert("User-Agent", "gcloud-rust/0.1".parse().unwrap());
        headers.insert(
            "x-goog-api-client",
            format!(
                "gl-rust/{}  gdcl/0.1",
                rustc_version_runtime::version().to_string()
            )
            .parse()
            .unwrap(),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();
        let inner = ClientRef {
            base_path: BASE_PATH.into(),
            http_client: client,
            cred,
        };
        Ok(Client {
            inner: Arc::new(inner),
        })
    }
    pub fn bucket_access_controls_service(&self) -> BucketAccessControlsService {
        BucketAccessControlsService {
            client: self.clone(),
        }
    }
    pub fn buckets_service(&self) -> BucketsService {
        BucketsService {
            client: self.clone(),
        }
    }
    pub fn channels_service(&self) -> ChannelsService {
        ChannelsService {
            client: self.clone(),
        }
    }
    pub fn default_object_access_controls_service(&self) -> DefaultObjectAccessControlsService {
        DefaultObjectAccessControlsService {
            client: self.clone(),
        }
    }
    pub fn notifications_service(&self) -> NotificationsService {
        NotificationsService {
            client: self.clone(),
        }
    }
    pub fn object_access_controls_service(&self) -> ObjectAccessControlsService {
        ObjectAccessControlsService {
            client: self.clone(),
        }
    }
    pub fn objects_service(&self) -> ObjectsService {
        ObjectsService {
            client: self.clone(),
        }
    }
    pub fn projects_hmac_keys_service(&self) -> ProjectsHmacKeysService {
        ProjectsHmacKeysService {
            client: self.clone(),
        }
    }
    pub fn projects_service_account_service(&self) -> ProjectsServiceAccountService {
        ProjectsServiceAccountService {
            client: self.clone(),
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Self {
            inner: Arc::new(ClientRef::default()),
        }
    }
}

#[derive(Debug)]
pub struct BucketAccessControlsService {
    client: Client,
}

#[derive(Debug)]
pub struct BucketsService {
    client: Client,
}

#[derive(Debug)]
pub struct ChannelsService {
    client: Client,
}

#[derive(Debug)]
pub struct DefaultObjectAccessControlsService {
    client: Client,
}

#[derive(Debug)]
pub struct NotificationsService {
    client: Client,
}

#[derive(Debug)]
pub struct ObjectAccessControlsService {
    client: Client,
}

#[derive(Debug)]
pub struct ObjectsService {
    client: Client,
}

#[derive(Debug)]
pub struct ProjectsHmacKeysService {
    client: Client,
}

#[derive(Debug)]
pub struct ProjectsServiceAccountService {
    client: Client,
}

impl BucketAccessControlsService {
    /// Permanently deletes the ACL entry for the specified entity on the
    /// specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn delete(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
    ) -> BucketAccessControlsDeleteCall {
        let mut c = BucketAccessControlsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c
    }
    /// Returns the ACL entry for the specified entity on the specified
    /// bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn get(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
    ) -> BucketAccessControlsGetCall {
        let mut c = BucketAccessControlsGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c
    }
    /// Creates a new ACL entry on the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn insert(
        &self,
        bucket: impl Into<String>,
        request: model::BucketAccessControl,
    ) -> BucketAccessControlsInsertCall {
        let mut c = BucketAccessControlsInsertCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
    /// Retrieves ACL entries on the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn list(&self, bucket: impl Into<String>) -> BucketAccessControlsListCall {
        let mut c = BucketAccessControlsListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
    /// Patches an ACL entry on the specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn patch(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
        request: model::BucketAccessControl,
    ) -> BucketAccessControlsPatchCall {
        let mut c = BucketAccessControlsPatchCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c.request = request;
        c
    }
    /// Updates an ACL entry on the specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn update(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
        request: model::BucketAccessControl,
    ) -> BucketAccessControlsUpdateCall {
        let mut c = BucketAccessControlsUpdateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct BucketAccessControlsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl BucketAccessControlsDeleteCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!(
                "{}b/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BucketAccessControlsGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl BucketAccessControlsGetCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::BucketAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::BucketAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketAccessControlsInsertCall {
    client: Client,
    request: model::BucketAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketAccessControlsInsertCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::BucketAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/acl",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::BucketAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketAccessControlsListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketAccessControlsListCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::BucketAccessControls> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/acl",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::BucketAccessControls = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketAccessControlsPatchCall {
    client: Client,
    request: model::BucketAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl BucketAccessControlsPatchCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::BucketAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .patch(format!(
                "{}b/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::BucketAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketAccessControlsUpdateCall {
    client: Client,
    request: model::BucketAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl BucketAccessControlsUpdateCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::BucketAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}b/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::BucketAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl BucketsService {
    /// Permanently deletes an empty bucket.
    /// - bucket: Name of a bucket.
    pub fn delete(&self, bucket: impl Into<String>) -> BucketsDeleteCall {
        let mut c = BucketsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
    /// Returns metadata for the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn get(&self, bucket: impl Into<String>) -> BucketsGetCall {
        let mut c = BucketsGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
    /// Returns an IAM policy for the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn get_iam_policy(&self, bucket: impl Into<String>) -> BucketsGetIamPolicyCall {
        let mut c = BucketsGetIamPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
    /// Creates a new bucket.
    /// - project: A valid API project identifier.
    pub fn insert(&self, project: impl Into<String>, request: model::Bucket) -> BucketsInsertCall {
        let mut c = BucketsInsertCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.url_params.insert("project".into(), vec![project.into()]);
        c.request = request;
        c
    }
    /// Retrieves a list of buckets for a given project.
    /// - project: A valid API project identifier.
    pub fn list(&self, project: impl Into<String>) -> BucketsListCall {
        let mut c = BucketsListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.url_params.insert("project".into(), vec![project.into()]);
        c
    }
    /// Locks retention policy on a bucket.
    /// - bucket: Name of a bucket.
    /// - if_metageneration_match: Makes the operation conditional on whether
    ///   bucket's current metageneration matches the given value.
    pub fn lock_retention_policy(
        &self,
        bucket: impl Into<String>,
        if_metageneration_match: impl Into<String>,
    ) -> BucketsLockRetentionPolicyCall {
        let mut c = BucketsLockRetentionPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.url_params.insert(
            "ifMetagenerationMatch".into(),
            vec![if_metageneration_match.into()],
        );
        c
    }
    /// Patches a bucket. Changes to the bucket will be readable immediately
    /// after writing, but configuration changes may take time to propagate.
    /// - bucket: Name of a bucket.
    pub fn patch(&self, bucket: impl Into<String>, request: model::Bucket) -> BucketsPatchCall {
        let mut c = BucketsPatchCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
    /// Updates an IAM policy for the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn set_iam_policy(
        &self,
        bucket: impl Into<String>,
        request: model::Policy,
    ) -> BucketsSetIamPolicyCall {
        let mut c = BucketsSetIamPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
    /// Tests a set of permissions on the given bucket to see which, if any,
    /// are held by the caller.
    /// - bucket: Name of a bucket.
    /// - permissions: Permissions to test.
    pub fn test_iam_permissions(
        &self,
        bucket: impl Into<String>,
        permissions: Vec<String>,
    ) -> BucketsTestIamPermissionsCall {
        let mut c = BucketsTestIamPermissionsCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.url_params.insert("permissions".into(), permissions);
        c
    }
    /// Updates a bucket. Changes to the bucket will be readable immediately
    /// after writing, but configuration changes may take time to propagate.
    /// - bucket: Name of a bucket.
    pub fn update(&self, bucket: impl Into<String>, request: model::Bucket) -> BucketsUpdateCall {
        let mut c = BucketsUpdateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct BucketsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsDeleteCall {
    /// If set, only deletes the bucket if its metageneration matches this
    /// value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// If set, only deletes the bucket if its metageneration does not match
    /// this value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!("{}b/{}", client.base_path, self.bucket.unwrap()))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BucketsGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsGetCall {
    /// Makes the return of the bucket metadata conditional on whether the
    /// bucket's current metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the return of the bucket metadata conditional on whether the
    /// bucket's current metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Bucket> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!("{}b/{}", client.base_path, self.bucket.unwrap()))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Bucket = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsGetIamPolicyCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsGetIamPolicyCall {
    /// The IAM policy format version to be returned. If the
    /// optionsRequestedPolicyVersion is for an older version that doesn't
    /// support part of the requested IAM policy, the request fails.
    pub fn options_requested_policy_version(mut self, value: i64) -> Self {
        self.url_params.insert(
            "options_requested_policy_version".into(),
            vec![value.to_string()],
        );
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Policy> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/iam",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Policy = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsInsertCall {
    client: Client,
    request: model::Bucket,
    url_params: std::collections::HashMap<String, Vec<String>>,
}

impl BucketsInsertCall {
    /// Apply a predefined set of access controls to this bucket.
    pub fn predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of default object access controls to this
    /// bucket.
    pub fn predefined_default_object_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_default_object_acl".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl, unless the bucket
    /// resource specifies acl or defaultObjectAcl properties, when it
    /// defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Bucket> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!("{}b", client.base_path))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Bucket = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
}

impl BucketsListCall {
    /// Maximum number of buckets to return in a single response. The service
    /// will use this parameter or 1,000 items, whichever is smaller.
    pub fn max_results(mut self, value: i64) -> Self {
        self.url_params
            .insert("max_results".into(), vec![value.to_string()]);
        self
    }
    /// A previously-returned page token representing part of the larger set
    /// of results to view.
    pub fn page_token(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("page_token".into(), vec![value.into()]);
        self
    }
    /// Filter results to buckets whose names begin with this prefix.
    pub fn prefix(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert("prefix".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Buckets> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!("{}b", client.base_path))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Buckets = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsLockRetentionPolicyCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsLockRetentionPolicyCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Bucket> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/lockRetentionPolicy",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Bucket = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsPatchCall {
    client: Client,
    request: model::Bucket,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsPatchCall {
    /// Makes the return of the bucket metadata conditional on whether the
    /// bucket's current metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the return of the bucket metadata conditional on whether the
    /// bucket's current metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to this bucket.
    pub fn predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of default object access controls to this
    /// bucket.
    pub fn predefined_default_object_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_default_object_acl".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Bucket> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .patch(format!("{}b/{}", client.base_path, self.bucket.unwrap()))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Bucket = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsSetIamPolicyCall {
    client: Client,
    request: model::Policy,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsSetIamPolicyCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Policy> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}b/{}/iam",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Policy = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsTestIamPermissionsCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsTestIamPermissionsCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::TestIamPermissionsResponse> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/iam/testPermissions",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::TestIamPermissionsResponse = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct BucketsUpdateCall {
    client: Client,
    request: model::Bucket,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsUpdateCall {
    /// Makes the return of the bucket metadata conditional on whether the
    /// bucket's current metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the return of the bucket metadata conditional on whether the
    /// bucket's current metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to this bucket.
    pub fn predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of default object access controls to this
    /// bucket.
    pub fn predefined_default_object_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_default_object_acl".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Bucket> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!("{}b/{}", client.base_path, self.bucket.unwrap()))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Bucket = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl ChannelsService {
    /// Stop watching resources through this channel
    pub fn stop(&self, request: model::Channel) -> ChannelsStopCall {
        let mut c = ChannelsStopCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct ChannelsStopCall {
    client: Client,
    request: model::Channel,
}

impl ChannelsStopCall {
    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!("{}channels/stop", client.base_path))
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

impl DefaultObjectAccessControlsService {
    /// Permanently deletes the default object ACL entry for the specified
    /// entity on the specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn delete(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
    ) -> DefaultObjectAccessControlsDeleteCall {
        let mut c = DefaultObjectAccessControlsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c
    }
    /// Returns the default object ACL entry for the specified entity on the
    /// specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn get(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
    ) -> DefaultObjectAccessControlsGetCall {
        let mut c = DefaultObjectAccessControlsGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c
    }
    /// Creates a new default object ACL entry on the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn insert(
        &self,
        bucket: impl Into<String>,
        request: model::ObjectAccessControl,
    ) -> DefaultObjectAccessControlsInsertCall {
        let mut c = DefaultObjectAccessControlsInsertCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
    /// Retrieves default object ACL entries on the specified bucket.
    /// - bucket: Name of a bucket.
    pub fn list(&self, bucket: impl Into<String>) -> DefaultObjectAccessControlsListCall {
        let mut c = DefaultObjectAccessControlsListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
    /// Patches a default object ACL entry on the specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn patch(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
        request: model::ObjectAccessControl,
    ) -> DefaultObjectAccessControlsPatchCall {
        let mut c = DefaultObjectAccessControlsPatchCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c.request = request;
        c
    }
    /// Updates a default object ACL entry on the specified bucket.
    /// - bucket: Name of a bucket.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn update(
        &self,
        bucket: impl Into<String>,
        entity: impl Into<String>,
        request: model::ObjectAccessControl,
    ) -> DefaultObjectAccessControlsUpdateCall {
        let mut c = DefaultObjectAccessControlsUpdateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.entity = Some(entity.into());
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct DefaultObjectAccessControlsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl DefaultObjectAccessControlsDeleteCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!(
                "{}b/{}/defaultObjectAcl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DefaultObjectAccessControlsGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl DefaultObjectAccessControlsGetCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/defaultObjectAcl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct DefaultObjectAccessControlsInsertCall {
    client: Client,
    request: model::ObjectAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl DefaultObjectAccessControlsInsertCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/defaultObjectAcl",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct DefaultObjectAccessControlsListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl DefaultObjectAccessControlsListCall {
    /// If present, only return default ACL listing if the bucket's current
    /// metageneration matches this value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// If present, only return default ACL listing if the bucket's current
    /// metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControls> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/defaultObjectAcl",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControls = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct DefaultObjectAccessControlsPatchCall {
    client: Client,
    request: model::ObjectAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl DefaultObjectAccessControlsPatchCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .patch(format!(
                "{}b/{}/defaultObjectAcl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct DefaultObjectAccessControlsUpdateCall {
    client: Client,
    request: model::ObjectAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
}

impl DefaultObjectAccessControlsUpdateCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}b/{}/defaultObjectAcl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl NotificationsService {
    /// Permanently deletes a notification subscription.
    /// - bucket: The parent bucket of the notification.
    /// - notification: ID of the notification to delete.
    pub fn delete(
        &self,
        bucket: impl Into<String>,
        notification: impl Into<String>,
    ) -> NotificationsDeleteCall {
        let mut c = NotificationsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.notification = Some(notification.into());
        c
    }
    /// View a notification configuration.
    /// - bucket: The parent bucket of the notification.
    /// - notification: Notification ID
    pub fn get(
        &self,
        bucket: impl Into<String>,
        notification: impl Into<String>,
    ) -> NotificationsGetCall {
        let mut c = NotificationsGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.notification = Some(notification.into());
        c
    }
    /// Creates a notification subscription for a given bucket.
    /// - bucket: The parent bucket of the notification.
    pub fn insert(
        &self,
        bucket: impl Into<String>,
        request: model::Notification,
    ) -> NotificationsInsertCall {
        let mut c = NotificationsInsertCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
    /// Retrieves a list of notification subscriptions for a given bucket.
    /// - bucket: Name of a Google Cloud Storage bucket.
    pub fn list(&self, bucket: impl Into<String>) -> NotificationsListCall {
        let mut c = NotificationsListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
}
#[derive(Debug, Default)]
pub struct NotificationsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    notification: Option<String>,
}

impl NotificationsDeleteCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!(
                "{}b/{}/notificationConfigs/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.notification.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NotificationsGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    notification: Option<String>,
}

impl NotificationsGetCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Notification> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/notificationConfigs/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.notification.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Notification = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct NotificationsInsertCall {
    client: Client,
    request: model::Notification,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl NotificationsInsertCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Notification> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/notificationConfigs",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Notification = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct NotificationsListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl NotificationsListCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Notifications> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/notificationConfigs",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Notifications = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl ObjectAccessControlsService {
    /// Permanently deletes the ACL entry for the specified entity on the
    /// specified object.
    /// - bucket: Name of a bucket.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn delete(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        entity: impl Into<String>,
    ) -> ObjectAccessControlsDeleteCall {
        let mut c = ObjectAccessControlsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.entity = Some(entity.into());
        c
    }
    /// Returns the ACL entry for the specified entity on the specified
    /// object.
    /// - bucket: Name of a bucket.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn get(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        entity: impl Into<String>,
    ) -> ObjectAccessControlsGetCall {
        let mut c = ObjectAccessControlsGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.entity = Some(entity.into());
        c
    }
    /// Creates a new ACL entry on the specified object.
    /// - bucket: Name of a bucket.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn insert(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        request: model::ObjectAccessControl,
    ) -> ObjectAccessControlsInsertCall {
        let mut c = ObjectAccessControlsInsertCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.request = request;
        c
    }
    /// Retrieves ACL entries on the specified object.
    /// - bucket: Name of a bucket.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn list(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
    ) -> ObjectAccessControlsListCall {
        let mut c = ObjectAccessControlsListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c
    }
    /// Patches an ACL entry on the specified object.
    /// - bucket: Name of a bucket.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn patch(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        entity: impl Into<String>,
        request: model::ObjectAccessControl,
    ) -> ObjectAccessControlsPatchCall {
        let mut c = ObjectAccessControlsPatchCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.entity = Some(entity.into());
        c.request = request;
        c
    }
    /// Updates an ACL entry on the specified object.
    /// - bucket: Name of a bucket.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    /// - entity: The entity holding the permission. Can be user-userId,
    ///   user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    ///   allAuthenticatedUsers.
    pub fn update(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        entity: impl Into<String>,
        request: model::ObjectAccessControl,
    ) -> ObjectAccessControlsUpdateCall {
        let mut c = ObjectAccessControlsUpdateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.entity = Some(entity.into());
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct ObjectAccessControlsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
    object: Option<String>,
}

impl ObjectAccessControlsDeleteCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!(
                "{}b/{}/o/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ObjectAccessControlsGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
    object: Option<String>,
}

impl ObjectAccessControlsGetCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/o/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectAccessControlsInsertCall {
    client: Client,
    request: model::ObjectAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectAccessControlsInsertCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/o/{}/acl",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectAccessControlsListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectAccessControlsListCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControls> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/o/{}/acl",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControls = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectAccessControlsPatchCall {
    client: Client,
    request: model::ObjectAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
    object: Option<String>,
}

impl ObjectAccessControlsPatchCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .patch(format!(
                "{}b/{}/o/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectAccessControlsUpdateCall {
    client: Client,
    request: model::ObjectAccessControl,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    entity: Option<String>,
    object: Option<String>,
}

impl ObjectAccessControlsUpdateCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ObjectAccessControl> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}b/{}/o/{}/acl/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap(),
                self.entity.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ObjectAccessControl = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl ObjectsService {
    /// Concatenates a list of existing objects into a new object in the same
    /// bucket.
    /// - destination_bucket: Name of the bucket containing the source
    ///   objects. The destination object is stored in this bucket.
    /// - destination_object: Name of the new object. For information about
    ///   how to URL encode object names to be path safe, see Encoding URI
    ///   Path Parts.
    pub fn compose(
        &self,
        destination_bucket: impl Into<String>,
        destination_object: impl Into<String>,
        request: model::ComposeRequest,
    ) -> ObjectsComposeCall {
        let mut c = ObjectsComposeCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.destination_bucket = Some(destination_bucket.into());
        c.destination_object = Some(destination_object.into());
        c.request = request;
        c
    }
    /// Copies a source object to a destination object. Optionally overrides
    /// metadata.
    /// - source_bucket: Name of the bucket in which to find the source
    ///   object.
    /// - source_object: Name of the source object. For information about how
    ///   to URL encode object names to be path safe, see Encoding URI Path
    ///   Parts.
    /// - destination_bucket: Name of the bucket in which to store the new
    ///   object. Overrides the provided object metadata's bucket value, if
    ///   any.For information about how to URL encode object names to be path
    ///   safe, see Encoding URI Path Parts.
    /// - destination_object: Name of the new object. Required when the
    ///   object metadata is not otherwise provided. Overrides the object
    ///   metadata's name value, if any.
    pub fn copy(
        &self,
        source_bucket: impl Into<String>,
        source_object: impl Into<String>,
        destination_bucket: impl Into<String>,
        destination_object: impl Into<String>,
        request: model::Object,
    ) -> ObjectsCopyCall {
        let mut c = ObjectsCopyCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.source_bucket = Some(source_bucket.into());
        c.source_object = Some(source_object.into());
        c.destination_bucket = Some(destination_bucket.into());
        c.destination_object = Some(destination_object.into());
        c.request = request;
        c
    }
    /// Deletes an object and its metadata. Deletions are permanent if
    /// versioning is not enabled for the bucket, or if the generation
    /// parameter is used.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn delete(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
    ) -> ObjectsDeleteCall {
        let mut c = ObjectsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c
    }
    /// Retrieves an object or its metadata.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn get(&self, bucket: impl Into<String>, object: impl Into<String>) -> ObjectsGetCall {
        let mut c = ObjectsGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c
    }
    /// Returns an IAM policy for the specified object.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn get_iam_policy(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
    ) -> ObjectsGetIamPolicyCall {
        let mut c = ObjectsGetIamPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c
    }
    /// Stores a new object and metadata.
    /// - bucket: Name of the bucket in which to store the new object.
    ///   Overrides the provided object metadata's bucket value, if any.
    pub fn insert(&self, bucket: impl Into<String>, request: model::Object) -> ObjectsInsertCall {
        let mut c = ObjectsInsertCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
    /// Retrieves a list of objects matching the criteria.
    /// - bucket: Name of the bucket in which to look for objects.
    pub fn list(&self, bucket: impl Into<String>) -> ObjectsListCall {
        let mut c = ObjectsListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c
    }
    /// Patches an object's metadata.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn patch(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        request: model::Object,
    ) -> ObjectsPatchCall {
        let mut c = ObjectsPatchCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.request = request;
        c
    }
    /// Rewrites a source object to a destination object. Optionally
    /// overrides metadata.
    /// - source_bucket: Name of the bucket in which to find the source
    ///   object.
    /// - source_object: Name of the source object. For information about how
    ///   to URL encode object names to be path safe, see Encoding URI Path
    ///   Parts.
    /// - destination_bucket: Name of the bucket in which to store the new
    ///   object. Overrides the provided object metadata's bucket value, if
    ///   any.
    /// - destination_object: Name of the new object. Required when the
    ///   object metadata is not otherwise provided. Overrides the object
    ///   metadata's name value, if any. For information about how to URL
    ///   encode object names to be path safe, see Encoding URI Path Parts.
    pub fn rewrite(
        &self,
        source_bucket: impl Into<String>,
        source_object: impl Into<String>,
        destination_bucket: impl Into<String>,
        destination_object: impl Into<String>,
        request: model::Object,
    ) -> ObjectsRewriteCall {
        let mut c = ObjectsRewriteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.source_bucket = Some(source_bucket.into());
        c.source_object = Some(source_object.into());
        c.destination_bucket = Some(destination_bucket.into());
        c.destination_object = Some(destination_object.into());
        c.request = request;
        c
    }
    /// Updates an IAM policy for the specified object.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn set_iam_policy(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        request: model::Policy,
    ) -> ObjectsSetIamPolicyCall {
        let mut c = ObjectsSetIamPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.request = request;
        c
    }
    /// Tests a set of permissions on the given object to see which, if any,
    /// are held by the caller.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    /// - permissions: Permissions to test.
    pub fn test_iam_permissions(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        permissions: Vec<String>,
    ) -> ObjectsTestIamPermissionsCall {
        let mut c = ObjectsTestIamPermissionsCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.url_params.insert("permissions".into(), permissions);
        c
    }
    /// Updates an object's metadata.
    /// - bucket: Name of the bucket in which the object resides.
    /// - object: Name of the object. For information about how to URL encode
    ///   object names to be path safe, see Encoding URI Path Parts.
    pub fn update(
        &self,
        bucket: impl Into<String>,
        object: impl Into<String>,
        request: model::Object,
    ) -> ObjectsUpdateCall {
        let mut c = ObjectsUpdateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.object = Some(object.into());
        c.request = request;
        c
    }
    /// Watch for changes on all objects in a bucket.
    /// - bucket: Name of the bucket in which to look for objects.
    pub fn watch_all(
        &self,
        bucket: impl Into<String>,
        request: model::Channel,
    ) -> ObjectsWatchAllCall {
        let mut c = ObjectsWatchAllCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.bucket = Some(bucket.into());
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct ObjectsComposeCall {
    client: Client,
    request: model::ComposeRequest,
    url_params: std::collections::HashMap<String, Vec<String>>,
    destination_bucket: Option<String>,
    destination_object: Option<String>,
}

impl ObjectsComposeCall {
    /// Apply a predefined set of access controls to the destination object.
    pub fn destination_predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Resource name of the Cloud KMS key, of the form
    /// projects/my-project/locations/global/keyRings/my-kr/cryptoKeys/my-key,
    ///  that will be used to encrypt the object. Overrides the object
    /// metadata's kms_key_name value, if any.
    pub fn kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("kms_key_name".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/o/{}/compose",
                client.base_path,
                self.destination_bucket.unwrap(),
                self.destination_object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Object = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsCopyCall {
    client: Client,
    request: model::Object,
    url_params: std::collections::HashMap<String, Vec<String>>,
    destination_bucket: Option<String>,
    destination_object: Option<String>,
    source_bucket: Option<String>,
    source_object: Option<String>,
}

impl ObjectsCopyCall {
    /// Resource name of the Cloud KMS key, of the form
    /// projects/my-project/locations/global/keyRings/my-kr/cryptoKeys/my-key,
    ///  that will be used to encrypt the object. Overrides the object
    /// metadata's kms_key_name value, if any.
    pub fn destination_kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_kms_key_name".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to the destination object.
    pub fn destination_predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the destination object's
    /// current generation matches the given value. Setting to 0 makes the
    /// operation succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the destination object's
    /// current generation does not match the given value. If no live object
    /// exists, the precondition fails. Setting to 0 makes the operation
    /// succeed only if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the destination object's
    /// current metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the destination object's
    /// current metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current generation matches the given value.
    pub fn if_source_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_source_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current generation does not match the given value.
    pub fn if_source_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_source_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current metageneration matches the given value.
    pub fn if_source_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_source_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current metageneration does not match the given value.
    pub fn if_source_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert(
            "if_source_metageneration_not_match".into(),
            vec![value.into()],
        );
        self
    }
    /// Set of properties to return. Defaults to noAcl, unless the object
    /// resource specifies the acl property, when it defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// If present, selects a specific revision of the source object (as
    /// opposed to the latest version, the default).
    pub fn source_generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("source_generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/o/{}/copyTo/b/{}/o/{}",
                client.base_path,
                self.source_bucket.unwrap(),
                self.source_object.unwrap(),
                self.destination_bucket.unwrap(),
                self.destination_object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Object = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsDeleteCall {
    /// If present, permanently deletes a specific revision of this object
    /// (as opposed to the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation does not match the given value. If no live object exists,
    /// the precondition fails. Setting to 0 makes the operation succeed only
    /// if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!(
                "{}b/{}/o/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ObjectsGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsGetCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation does not match the given value. If no live object exists,
    /// the precondition fails. Setting to 0 makes the operation succeed only
    /// if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/o/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Object = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }

    pub async fn download(self) -> Result<Vec<u8>> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/o/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "media")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(res.bytes().await.map_err(Error::wrap)?.to_vec())
    }
}

#[derive(Debug, Default)]
pub struct ObjectsGetIamPolicyCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsGetIamPolicyCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Policy> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/o/{}/iam",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Policy = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsInsertCall {
    client: Client,
    request: model::Object,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    media_content_type: Option<String>,
}

impl ObjectsInsertCall {
    /// If set, sets the contentEncoding property of the final object to this
    /// value. Setting this parameter is equivalent to setting the
    /// contentEncoding metadata property. This can be useful when uploading
    /// an object with uploadType=media to indicate the encoding of the
    /// content being uploaded.
    pub fn content_encoding(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("content_encoding".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation does not match the given value. If no live object exists,
    /// the precondition fails. Setting to 0 makes the operation succeed only
    /// if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Resource name of the Cloud KMS key, of the form
    /// projects/my-project/locations/global/keyRings/my-kr/cryptoKeys/my-key,
    ///  that will be used to encrypt the object. Overrides the object
    /// metadata's kms_key_name value, if any.
    pub fn kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("kms_key_name".into(), vec![value.into()]);
        self
    }
    /// Name of the object. Required when the object metadata is not
    /// otherwise provided. Overrides the object metadata's name value, if
    /// any. For information about how to URL encode object names to be path
    /// safe, see Encoding URI Path Parts.
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert("name".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to this object.
    pub fn predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl, unless the object
    /// resource specifies the acl property, when it defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }
    /// Explicitly sets the content type of the media being uploaded.
    pub fn media_content_type(mut self, value: impl Into<String>) -> Self {
        self.media_content_type = Some(value.into());
        self
    }

    pub async fn upload(mut self, media: impl Into<BytesReader>) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let body = serde_json::to_vec(&self.request).map_err(Error::wrap)?;
        let mut media_part =
            reqwest::multipart::Part::bytes(media.into().read_all().await?.as_ref().to_owned());
        if let Some(media_content_type) = self.media_content_type {
            media_part = media_part
                .mime_str(&media_content_type)
                .map_err(Error::wrap)?;
        }
        let form = reqwest::multipart::Form::new()
            .part(
                "body",
                reqwest::multipart::Part::bytes(body)
                    .mime_str("application/json")
                    .map_err(Error::wrap)?,
            )
            .part("media", media_part);
        self.url_params
            .insert("uploadType".into(), vec!["multipart".into()]);

        let res = client
            .http_client
            .post(set_path(
                &client.base_path,
                &format!("/upload/storage/v1/b/{}/o", self.bucket.unwrap()),
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .multipart(form)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Object = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl ObjectsListCall {
    /// Returns results in a directory-like mode. items will contain only
    /// objects whose names, aside from the prefix, do not contain delimiter.
    /// Objects whose names, aside from the prefix, contain delimiter will
    /// have their name, truncated after the delimiter, returned in prefixes.
    /// Duplicate prefixes are omitted.
    pub fn delimiter(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("delimiter".into(), vec![value.into()]);
        self
    }
    /// Filter results to objects whose names are lexicographically before
    /// endOffset. If startOffset is also set, the objects listed will have
    /// names between startOffset (inclusive) and endOffset (exclusive).
    pub fn end_offset(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("end_offset".into(), vec![value.into()]);
        self
    }
    /// If true, objects that end in exactly one instance of delimiter will
    /// have their metadata included in items in addition to prefixes.
    pub fn include_trailing_delimiter(mut self, value: bool) -> Self {
        self.url_params
            .insert("include_trailing_delimiter".into(), vec![value.to_string()]);
        self
    }
    /// Maximum number of items plus prefixes to return in a single page of
    /// responses. As duplicate prefixes are omitted, fewer total results may
    /// be returned than requested. The service will use this parameter or
    /// 1,000 items, whichever is smaller.
    pub fn max_results(mut self, value: i64) -> Self {
        self.url_params
            .insert("max_results".into(), vec![value.to_string()]);
        self
    }
    /// A previously-returned page token representing part of the larger set
    /// of results to view.
    pub fn page_token(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("page_token".into(), vec![value.into()]);
        self
    }
    /// Filter results to objects whose names begin with this prefix.
    pub fn prefix(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert("prefix".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// Filter results to objects whose names are lexicographically equal to
    /// or after startOffset. If endOffset is also set, the objects listed
    /// will have names between startOffset (inclusive) and endOffset
    /// (exclusive).
    pub fn start_offset(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("start_offset".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }
    /// If true, lists all versions of an object as distinct results. The
    /// default is false. For more information, see Object Versioning.
    pub fn versions(mut self, value: bool) -> Self {
        self.url_params
            .insert("versions".into(), vec![value.to_string()]);
        self
    }

    pub async fn execute(self) -> Result<model::Objects> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!("{}b/{}/o", client.base_path, self.bucket.unwrap()))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Objects = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsPatchCall {
    client: Client,
    request: model::Object,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsPatchCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation does not match the given value. If no live object exists,
    /// the precondition fails. Setting to 0 makes the operation succeed only
    /// if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to this object.
    pub fn predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request, for Requester Pays
    /// buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .patch(format!(
                "{}b/{}/o/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Object = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsRewriteCall {
    client: Client,
    request: model::Object,
    url_params: std::collections::HashMap<String, Vec<String>>,
    destination_bucket: Option<String>,
    destination_object: Option<String>,
    source_bucket: Option<String>,
    source_object: Option<String>,
}

impl ObjectsRewriteCall {
    /// Resource name of the Cloud KMS key, of the form
    /// projects/my-project/locations/global/keyRings/my-kr/cryptoKeys/my-key,
    ///  that will be used to encrypt the object. Overrides the object
    /// metadata's kms_key_name value, if any.
    pub fn destination_kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_kms_key_name".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to the destination object.
    pub fn destination_predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation does not match the given value. If no live object exists,
    /// the precondition fails. Setting to 0 makes the operation succeed only
    /// if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the destination object's
    /// current metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the destination object's
    /// current metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current generation matches the given value.
    pub fn if_source_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_source_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current generation does not match the given value.
    pub fn if_source_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_source_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current metageneration matches the given value.
    pub fn if_source_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_source_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the source object's
    /// current metageneration does not match the given value.
    pub fn if_source_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert(
            "if_source_metageneration_not_match".into(),
            vec![value.into()],
        );
        self
    }
    /// The maximum number of bytes that will be rewritten per rewrite
    /// request. Most callers shouldn't need to specify this parameter - it
    /// is primarily in place to support testing. If specified the value must
    /// be an integral multiple of 1 MiB (1048576). Also, this only applies
    /// to requests where the source and destination span locations and/or
    /// storage classes. Finally, this value must not change across rewrite
    /// calls else you'll get an error that the rewriteToken is invalid.
    pub fn max_bytes_rewritten_per_call(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("max_bytes_rewritten_per_call".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl, unless the object
    /// resource specifies the acl property, when it defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// Include this field (from the previous rewrite response) on each
    /// rewrite request after the first one, until the rewrite response
    /// 'done' flag is true. Calls that provide a rewriteToken can omit all
    /// other request fields, but if included those fields must match the
    /// values provided in the first rewrite request.
    pub fn rewrite_token(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("rewrite_token".into(), vec![value.into()]);
        self
    }
    /// If present, selects a specific revision of the source object (as
    /// opposed to the latest version, the default).
    pub fn source_generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("source_generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::RewriteResponse> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/o/{}/rewriteTo/b/{}/o/{}",
                client.base_path,
                self.source_bucket.unwrap(),
                self.source_object.unwrap(),
                self.destination_bucket.unwrap(),
                self.destination_object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::RewriteResponse = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsSetIamPolicyCall {
    client: Client,
    request: model::Policy,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsSetIamPolicyCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Policy> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}b/{}/o/{}/iam",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Policy = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsTestIamPermissionsCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsTestIamPermissionsCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::TestIamPermissionsResponse> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}b/{}/o/{}/iam/testPermissions",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::TestIamPermissionsResponse = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsUpdateCall {
    client: Client,
    request: model::Object,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
    object: Option<String>,
}

impl ObjectsUpdateCall {
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation matches the given value. Setting to 0 makes the operation
    /// succeed only if there are no live versions of the object.
    pub fn if_generation_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// generation does not match the given value. If no live object exists,
    /// the precondition fails. Setting to 0 makes the operation succeed only
    /// if there is a live version of the object.
    pub fn if_generation_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_generation_not_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
        self
    }
    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn if_metageneration_not_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_not_match".into(), vec![value.into()]);
        self
    }
    /// Apply a predefined set of access controls to this object.
    pub fn predefined_acl(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("predefined_acl".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to full.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}b/{}/o/{}",
                client.base_path,
                self.bucket.unwrap(),
                self.object.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Object = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ObjectsWatchAllCall {
    client: Client,
    request: model::Channel,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl ObjectsWatchAllCall {
    /// Returns results in a directory-like mode. items will contain only
    /// objects whose names, aside from the prefix, do not contain delimiter.
    /// Objects whose names, aside from the prefix, contain delimiter will
    /// have their name, truncated after the delimiter, returned in prefixes.
    /// Duplicate prefixes are omitted.
    pub fn delimiter(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("delimiter".into(), vec![value.into()]);
        self
    }
    /// Filter results to objects whose names are lexicographically before
    /// endOffset. If startOffset is also set, the objects listed will have
    /// names between startOffset (inclusive) and endOffset (exclusive).
    pub fn end_offset(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("end_offset".into(), vec![value.into()]);
        self
    }
    /// If true, objects that end in exactly one instance of delimiter will
    /// have their metadata included in items in addition to prefixes.
    pub fn include_trailing_delimiter(mut self, value: bool) -> Self {
        self.url_params
            .insert("include_trailing_delimiter".into(), vec![value.to_string()]);
        self
    }
    /// Maximum number of items plus prefixes to return in a single page of
    /// responses. As duplicate prefixes are omitted, fewer total results may
    /// be returned than requested. The service will use this parameter or
    /// 1,000 items, whichever is smaller.
    pub fn max_results(mut self, value: i64) -> Self {
        self.url_params
            .insert("max_results".into(), vec![value.to_string()]);
        self
    }
    /// A previously-returned page token representing part of the larger set
    /// of results to view.
    pub fn page_token(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("page_token".into(), vec![value.into()]);
        self
    }
    /// Filter results to objects whose names begin with this prefix.
    pub fn prefix(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert("prefix".into(), vec![value.into()]);
        self
    }
    /// Set of properties to return. Defaults to noAcl.
    pub fn projection(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("projection".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// Filter results to objects whose names are lexicographically equal to
    /// or after startOffset. If endOffset is also set, the objects listed
    /// will have names between startOffset (inclusive) and endOffset
    /// (exclusive).
    pub fn start_offset(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("start_offset".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request. Required for Requester
    /// Pays buckets.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }
    /// If true, lists all versions of an object as distinct results. The
    /// default is false. For more information, see Object Versioning.
    pub fn versions(mut self, value: bool) -> Self {
        self.url_params
            .insert("versions".into(), vec![value.to_string()]);
        self
    }

    pub async fn execute(self) -> Result<model::Channel> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}b/{}/o/watch",
                client.base_path,
                self.bucket.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::Channel = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl ProjectsHmacKeysService {
    /// Creates a new HMAC key for the specified service account.
    /// - project_id: Project ID owning the service account.
    /// - service_account_email: Email address of the service account.
    pub fn create(
        &self,
        project_id: impl Into<String>,
        service_account_email: impl Into<String>,
    ) -> ProjectsHmacKeysCreateCall {
        let mut c = ProjectsHmacKeysCreateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.project_id = Some(project_id.into());
        c.url_params.insert(
            "serviceAccountEmail".into(),
            vec![service_account_email.into()],
        );
        c
    }
    /// Deletes an HMAC key.
    /// - project_id: Project ID owning the requested key
    /// - access_id: Name of the HMAC key to be deleted.
    pub fn delete(
        &self,
        project_id: impl Into<String>,
        access_id: impl Into<String>,
    ) -> ProjectsHmacKeysDeleteCall {
        let mut c = ProjectsHmacKeysDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.project_id = Some(project_id.into());
        c.access_id = Some(access_id.into());
        c
    }
    /// Retrieves an HMAC key's metadata
    /// - project_id: Project ID owning the service account of the requested
    ///   key.
    /// - access_id: Name of the HMAC key.
    pub fn get(
        &self,
        project_id: impl Into<String>,
        access_id: impl Into<String>,
    ) -> ProjectsHmacKeysGetCall {
        let mut c = ProjectsHmacKeysGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.project_id = Some(project_id.into());
        c.access_id = Some(access_id.into());
        c
    }
    /// Retrieves a list of HMAC keys matching the criteria.
    /// - project_id: Name of the project in which to look for HMAC keys.
    pub fn list(&self, project_id: impl Into<String>) -> ProjectsHmacKeysListCall {
        let mut c = ProjectsHmacKeysListCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.project_id = Some(project_id.into());
        c
    }
    /// Updates the state of an HMAC key. See the HMAC Key resource
    /// descriptor for valid states.
    /// - project_id: Project ID owning the service account of the updated
    ///   key.
    /// - access_id: Name of the HMAC key being updated.
    pub fn update(
        &self,
        project_id: impl Into<String>,
        access_id: impl Into<String>,
        request: model::HmacKeyMetadata,
    ) -> ProjectsHmacKeysUpdateCall {
        let mut c = ProjectsHmacKeysUpdateCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.project_id = Some(project_id.into());
        c.access_id = Some(access_id.into());
        c.request = request;
        c
    }
}
#[derive(Debug, Default)]
pub struct ProjectsHmacKeysCreateCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    project_id: Option<String>,
}

impl ProjectsHmacKeysCreateCall {
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::HmacKey> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .post(format!(
                "{}projects/{}/hmacKeys",
                client.base_path,
                self.project_id.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::HmacKey = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ProjectsHmacKeysDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    access_id: Option<String>,
    project_id: Option<String>,
}

impl ProjectsHmacKeysDeleteCall {
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<()> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .delete(format!(
                "{}projects/{}/hmacKeys/{}",
                client.base_path,
                self.project_id.unwrap(),
                self.access_id.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ProjectsHmacKeysGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    access_id: Option<String>,
    project_id: Option<String>,
}

impl ProjectsHmacKeysGetCall {
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::HmacKeyMetadata> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}projects/{}/hmacKeys/{}",
                client.base_path,
                self.project_id.unwrap(),
                self.access_id.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::HmacKeyMetadata = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ProjectsHmacKeysListCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    project_id: Option<String>,
}

impl ProjectsHmacKeysListCall {
    /// Maximum number of items to return in a single page of responses. The
    /// service uses this parameter or 250 items, whichever is smaller. The
    /// max number of items per page will also be limited by the number of
    /// distinct service accounts in the response. If the number of service
    /// accounts in a single response is too high, the page will truncated
    /// and a next page token will be returned.
    pub fn max_results(mut self, value: i64) -> Self {
        self.url_params
            .insert("max_results".into(), vec![value.to_string()]);
        self
    }
    /// A previously-returned page token representing part of the larger set
    /// of results to view.
    pub fn page_token(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("page_token".into(), vec![value.into()]);
        self
    }
    /// If present, only keys for the given service account are returned.
    pub fn service_account_email(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("service_account_email".into(), vec![value.into()]);
        self
    }
    /// Whether or not to show keys in the DELETED state.
    pub fn show_deleted_keys(mut self, value: bool) -> Self {
        self.url_params
            .insert("show_deleted_keys".into(), vec![value.to_string()]);
        self
    }
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::HmacKeysMetadata> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}projects/{}/hmacKeys",
                client.base_path,
                self.project_id.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::HmacKeysMetadata = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct ProjectsHmacKeysUpdateCall {
    client: Client,
    request: model::HmacKeyMetadata,
    url_params: std::collections::HashMap<String, Vec<String>>,
    access_id: Option<String>,
    project_id: Option<String>,
}

impl ProjectsHmacKeysUpdateCall {
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::HmacKeyMetadata> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .put(format!(
                "{}projects/{}/hmacKeys/{}",
                client.base_path,
                self.project_id.unwrap(),
                self.access_id.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .json(&self.request)
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::HmacKeyMetadata = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

impl ProjectsServiceAccountService {
    /// Get the email address of this project's Google Cloud Storage service
    /// account.
    /// - project_id: Project ID
    pub fn get(&self, project_id: impl Into<String>) -> ProjectsServiceAccountGetCall {
        let mut c = ProjectsServiceAccountGetCall {
            client: self.client.clone(),
            ..Default::default()
        };
        c.project_id = Some(project_id.into());
        c
    }
}
#[derive(Debug, Default)]
pub struct ProjectsServiceAccountGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    project_id: Option<String>,
}

impl ProjectsServiceAccountGetCall {
    /// The project to be billed for this request if the target bucket is
    /// requester-pays bucket.
    pub fn provisional_user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("provisional_user_project".into(), vec![value.into()]);
        self
    }
    /// The project to be billed for this request.
    pub fn user_project(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("user_project".into(), vec![value.into()]);
        self
    }

    pub async fn execute(self) -> Result<model::ServiceAccount> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let res = client
            .http_client
            .get(format!(
                "{}projects/{}/serviceAccount",
                client.base_path,
                self.project_id.unwrap()
            ))
            .query(
                &self
                    .url_params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v[0].as_str()))
                    .collect::<Vec<(&str, &str)>>(),
            )
            .query(&[("alt", "json")])
            .query(&[("prettyPrint", "false")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }
        let res: model::ServiceAccount = res.json().await.map_err(Error::wrap)?;
        Ok(res)
    }
}

fn set_path(base: &str, path: &str) -> String {
    let mut url = reqwest::Url::parse(base).unwrap();
    url.set_path(path);
    url.to_string()
}

#[derive(Debug)]
pub struct Error {
    inner_error: Option<Box<dyn StdError + Send + Sync>>,
    message: Option<String>,
}

impl Error {
    fn new(msg: impl Into<String>) -> Self {
        Self {
            inner_error: None,
            message: Some(msg.into()),
        }
    }

    fn wrap<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner_error: Some(Box::new(error)),
            message: None,
        }
    }

    /// Returns a reference to the inner error wrapped if, if there is one.
    pub fn get_ref(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        match &self.inner_error {
            Some(err) => Some(err.as_ref()),
            None => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(inner_error) = &self.inner_error {
            inner_error.fmt(f)
        } else if let Some(msg) = &self.message {
            write!(f, "{}", msg)
        } else {
            write!(f, "unknown error")
        }
    }
}

impl StdError for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
struct ApiErrorReply {
    error: ApiError,
}

impl ApiErrorReply {
    fn into_inner(self) -> ApiError {
        self.error
    }
}

#[derive(Clone, Debug, Deserialize)]
#[non_exhaustive]
pub struct ApiError {
    pub code: i32,
    pub message: String,
    #[serde(flatten)]
    extra: serde_json::value::Value,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}: {}",
            self.code,
            self.message,
            self.extra.to_string()
        )
    }
}

impl StdError for ApiError {}
