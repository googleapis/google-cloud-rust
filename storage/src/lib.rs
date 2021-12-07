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
    pub fn delete(&self) -> BucketAccessControlsDeleteCall {
        BucketAccessControlsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Returns the ACL entry for the specified entity on the specified
    /// bucket.
    pub fn get(&self) -> BucketAccessControlsGetCall {
        BucketAccessControlsGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Creates a new ACL entry on the specified bucket.
    pub fn insert(&self, request: model::BucketAccessControl) -> BucketAccessControlsInsertCall {
        BucketAccessControlsInsertCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Retrieves ACL entries on the specified bucket.
    pub fn list(&self) -> BucketAccessControlsListCall {
        BucketAccessControlsListCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Patches an ACL entry on the specified bucket.
    pub fn patch(&self, request: model::BucketAccessControl) -> BucketAccessControlsPatchCall {
        BucketAccessControlsPatchCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Updates an ACL entry on the specified bucket.
    pub fn update(&self, request: model::BucketAccessControl) -> BucketAccessControlsUpdateCall {
        BucketAccessControlsUpdateCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    pub fn delete(&self) -> BucketsDeleteCall {
        BucketsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Returns metadata for the specified bucket.
    pub fn get(&self) -> BucketsGetCall {
        BucketsGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Returns an IAM policy for the specified bucket.
    pub fn get_iam_policy(&self) -> BucketsGetIamPolicyCall {
        BucketsGetIamPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Creates a new bucket.
    pub fn insert(&self, request: model::Bucket) -> BucketsInsertCall {
        BucketsInsertCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Retrieves a list of buckets for a given project.
    pub fn list(&self) -> BucketsListCall {
        BucketsListCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Locks retention policy on a bucket.
    pub fn lock_retention_policy(&self) -> BucketsLockRetentionPolicyCall {
        BucketsLockRetentionPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Patches a bucket. Changes to the bucket will be readable immediately
    /// after writing, but configuration changes may take time to propagate.
    pub fn patch(&self, request: model::Bucket) -> BucketsPatchCall {
        BucketsPatchCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Updates an IAM policy for the specified bucket.
    pub fn set_iam_policy(&self, request: model::Policy) -> BucketsSetIamPolicyCall {
        BucketsSetIamPolicyCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Tests a set of permissions on the given bucket to see which, if any,
    /// are held by the caller.
    pub fn test_iam_permissions(&self) -> BucketsTestIamPermissionsCall {
        BucketsTestIamPermissionsCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Updates a bucket. Changes to the bucket will be readable immediately
    /// after writing, but configuration changes may take time to propagate.
    pub fn update(&self, request: model::Bucket) -> BucketsUpdateCall {
        BucketsUpdateCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
}
#[derive(Debug, Default)]
pub struct BucketsDeleteCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    bucket: Option<String>,
}

impl BucketsDeleteCall {
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// A valid API project identifier.
    pub fn project(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert("project".into(), vec![value.into()]);
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
    /// A valid API project identifier.
    pub fn project(mut self, value: impl Into<String>) -> Self {
        self.url_params.insert("project".into(), vec![value.into()]);
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// Makes the operation conditional on whether bucket's current
    /// metageneration matches the given value.
    pub fn if_metageneration_match(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("if_metageneration_match".into(), vec![value.into()]);
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// Permissions to test.
    pub fn permissions(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("permissions".into(), vec![value.into()]);
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
        ChannelsStopCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
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
    pub fn delete(&self) -> DefaultObjectAccessControlsDeleteCall {
        DefaultObjectAccessControlsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Returns the default object ACL entry for the specified entity on the
    /// specified bucket.
    pub fn get(&self) -> DefaultObjectAccessControlsGetCall {
        DefaultObjectAccessControlsGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Creates a new default object ACL entry on the specified bucket.
    pub fn insert(
        &self,
        request: model::ObjectAccessControl,
    ) -> DefaultObjectAccessControlsInsertCall {
        DefaultObjectAccessControlsInsertCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Retrieves default object ACL entries on the specified bucket.
    pub fn list(&self) -> DefaultObjectAccessControlsListCall {
        DefaultObjectAccessControlsListCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Patches a default object ACL entry on the specified bucket.
    pub fn patch(
        &self,
        request: model::ObjectAccessControl,
    ) -> DefaultObjectAccessControlsPatchCall {
        DefaultObjectAccessControlsPatchCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Updates a default object ACL entry on the specified bucket.
    pub fn update(
        &self,
        request: model::ObjectAccessControl,
    ) -> DefaultObjectAccessControlsUpdateCall {
        DefaultObjectAccessControlsUpdateCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
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
    pub fn delete(&self) -> NotificationsDeleteCall {
        NotificationsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// View a notification configuration.
    pub fn get(&self) -> NotificationsGetCall {
        NotificationsGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Creates a notification subscription for a given bucket.
    pub fn insert(&self, request: model::Notification) -> NotificationsInsertCall {
        NotificationsInsertCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Retrieves a list of notification subscriptions for a given bucket.
    pub fn list(&self) -> NotificationsListCall {
        NotificationsListCall {
            client: self.client.clone(),
            ..Default::default()
        }
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
    /// The parent bucket of the notification.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// ID of the notification to delete.
    pub fn notification(mut self, value: impl Into<String>) -> Self {
        self.notification = Some(value.into());
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
    /// The parent bucket of the notification.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// Notification ID
    pub fn notification(mut self, value: impl Into<String>) -> Self {
        self.notification = Some(value.into());
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
    /// The parent bucket of the notification.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
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
    /// Name of a Google Cloud Storage bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
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
    pub fn delete(&self) -> ObjectAccessControlsDeleteCall {
        ObjectAccessControlsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Returns the ACL entry for the specified entity on the specified
    /// object.
    pub fn get(&self) -> ObjectAccessControlsGetCall {
        ObjectAccessControlsGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Creates a new ACL entry on the specified object.
    pub fn insert(&self, request: model::ObjectAccessControl) -> ObjectAccessControlsInsertCall {
        ObjectAccessControlsInsertCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Retrieves ACL entries on the specified object.
    pub fn list(&self) -> ObjectAccessControlsListCall {
        ObjectAccessControlsListCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Patches an ACL entry on the specified object.
    pub fn patch(&self, request: model::ObjectAccessControl) -> ObjectAccessControlsPatchCall {
        ObjectAccessControlsPatchCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Updates an ACL entry on the specified object.
    pub fn update(&self, request: model::ObjectAccessControl) -> ObjectAccessControlsUpdateCall {
        ObjectAccessControlsUpdateCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of a bucket.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// The entity holding the permission. Can be user-userId,
    /// user-emailAddress, group-groupId, group-emailAddress, allUsers, or
    /// allAuthenticatedUsers.
    pub fn entity(mut self, value: impl Into<String>) -> Self {
        self.entity = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    pub fn compose(&self, request: model::ComposeRequest) -> ObjectsComposeCall {
        ObjectsComposeCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Copies a source object to a destination object. Optionally overrides
    /// metadata.
    pub fn copy(&self, request: model::Object) -> ObjectsCopyCall {
        ObjectsCopyCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Deletes an object and its metadata. Deletions are permanent if
    /// versioning is not enabled for the bucket, or if the generation
    /// parameter is used.
    pub fn delete(&self) -> ObjectsDeleteCall {
        ObjectsDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Retrieves an object or its metadata.
    pub fn get(&self) -> ObjectsGetCall {
        ObjectsGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Returns an IAM policy for the specified object.
    pub fn get_iam_policy(&self) -> ObjectsGetIamPolicyCall {
        ObjectsGetIamPolicyCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Stores a new object and metadata.
    pub fn insert(&self, request: model::Object) -> ObjectsInsertCall {
        ObjectsInsertCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Retrieves a list of objects matching the criteria.
    pub fn list(&self) -> ObjectsListCall {
        ObjectsListCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Patches an object's metadata.
    pub fn patch(&self, request: model::Object) -> ObjectsPatchCall {
        ObjectsPatchCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Rewrites a source object to a destination object. Optionally
    /// overrides metadata.
    pub fn rewrite(&self, request: model::Object) -> ObjectsRewriteCall {
        ObjectsRewriteCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Updates an IAM policy for the specified object.
    pub fn set_iam_policy(&self, request: model::Policy) -> ObjectsSetIamPolicyCall {
        ObjectsSetIamPolicyCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Tests a set of permissions on the given object to see which, if any,
    /// are held by the caller.
    pub fn test_iam_permissions(&self) -> ObjectsTestIamPermissionsCall {
        ObjectsTestIamPermissionsCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Updates an object's metadata.
    pub fn update(&self, request: model::Object) -> ObjectsUpdateCall {
        ObjectsUpdateCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
    /// Watch for changes on all objects in a bucket.
    pub fn watch_all(&self, request: model::Channel) -> ObjectsWatchAllCall {
        ObjectsWatchAllCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
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
    /// Name of the bucket containing the source objects. The destination
    /// object is stored in this bucket.
    pub fn destination_bucket(mut self, value: impl Into<String>) -> Self {
        self.destination_bucket = Some(value.into());
        self
    }
    /// Name of the new object. For information about how to URL encode
    /// object names to be path safe, see Encoding URI Path Parts.
    pub fn destination_object(mut self, value: impl Into<String>) -> Self {
        self.destination_object = Some(value.into());
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
    /// Name of the bucket in which to store the new object. Overrides the
    /// provided object metadata's bucket value, if any.For information about
    /// how to URL encode object names to be path safe, see Encoding URI Path
    /// Parts.
    pub fn destination_bucket(mut self, value: impl Into<String>) -> Self {
        self.destination_bucket = Some(value.into());
        self
    }
    /// Resource name of the Cloud KMS key, of the form
    /// projects/my-project/locations/global/keyRings/my-kr/cryptoKeys/my-key,
    ///  that will be used to encrypt the object. Overrides the object
    /// metadata's kms_key_name value, if any.
    pub fn destination_kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_kms_key_name".into(), vec![value.into()]);
        self
    }
    /// Name of the new object. Required when the object metadata is not
    /// otherwise provided. Overrides the object metadata's name value, if
    /// any.
    pub fn destination_object(mut self, value: impl Into<String>) -> Self {
        self.destination_object = Some(value.into());
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
    /// Name of the bucket in which to find the source object.
    pub fn source_bucket(mut self, value: impl Into<String>) -> Self {
        self.source_bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of the source object (as
    /// opposed to the latest version, the default).
    pub fn source_generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("source_generation".into(), vec![value.into()]);
        self
    }
    /// Name of the source object. For information about how to URL encode
    /// object names to be path safe, see Encoding URI Path Parts.
    pub fn source_object(mut self, value: impl Into<String>) -> Self {
        self.source_object = Some(value.into());
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
}

impl ObjectsInsertCall {
    /// Name of the bucket in which to store the new object. Overrides the
    /// provided object metadata's bucket value, if any.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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

    pub async fn upload(
        mut self,
        media: impl Into<BytesReader>,
        media_mime_type: impl Into<std::string::String>,
    ) -> Result<model::Object> {
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let body = serde_json::to_vec(&self.request).map_err(Error::wrap)?;
        let form = reqwest::multipart::Form::new()
            .part(
                "body",
                reqwest::multipart::Part::bytes(body)
                    .mime_str("application/json")
                    .map_err(Error::wrap)?,
            )
            .part(
                "media",
                reqwest::multipart::Part::bytes(media.into().read_all().await?.as_ref().to_owned())
                    .mime_str(media_mime_type.into().as_str())
                    .map_err(Error::wrap)?,
            );
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
    /// Name of the bucket in which to look for objects.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of the bucket in which to store the new object. Overrides the
    /// provided object metadata's bucket value, if any.
    pub fn destination_bucket(mut self, value: impl Into<String>) -> Self {
        self.destination_bucket = Some(value.into());
        self
    }
    /// Resource name of the Cloud KMS key, of the form
    /// projects/my-project/locations/global/keyRings/my-kr/cryptoKeys/my-key,
    ///  that will be used to encrypt the object. Overrides the object
    /// metadata's kms_key_name value, if any.
    pub fn destination_kms_key_name(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("destination_kms_key_name".into(), vec![value.into()]);
        self
    }
    /// Name of the new object. Required when the object metadata is not
    /// otherwise provided. Overrides the object metadata's name value, if
    /// any. For information about how to URL encode object names to be path
    /// safe, see Encoding URI Path Parts.
    pub fn destination_object(mut self, value: impl Into<String>) -> Self {
        self.destination_object = Some(value.into());
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
    /// Name of the bucket in which to find the source object.
    pub fn source_bucket(mut self, value: impl Into<String>) -> Self {
        self.source_bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of the source object (as
    /// opposed to the latest version, the default).
    pub fn source_generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("source_generation".into(), vec![value.into()]);
        self
    }
    /// Name of the source object. For information about how to URL encode
    /// object names to be path safe, see Encoding URI Path Parts.
    pub fn source_object(mut self, value: impl Into<String>) -> Self {
        self.source_object = Some(value.into());
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    /// If present, selects a specific revision of this object (as opposed to
    /// the latest version, the default).
    pub fn generation(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("generation".into(), vec![value.into()]);
        self
    }
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
        self
    }
    /// Permissions to test.
    pub fn permissions(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("permissions".into(), vec![value.into()]);
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
    /// Name of the bucket in which the object resides.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    /// Name of the object. For information about how to URL encode object
    /// names to be path safe, see Encoding URI Path Parts.
    pub fn object(mut self, value: impl Into<String>) -> Self {
        self.object = Some(value.into());
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
    /// Name of the bucket in which to look for objects.
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
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
    pub fn create(&self) -> ProjectsHmacKeysCreateCall {
        ProjectsHmacKeysCreateCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Deletes an HMAC key.
    pub fn delete(&self) -> ProjectsHmacKeysDeleteCall {
        ProjectsHmacKeysDeleteCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Retrieves an HMAC key's metadata
    pub fn get(&self) -> ProjectsHmacKeysGetCall {
        ProjectsHmacKeysGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Retrieves a list of HMAC keys matching the criteria.
    pub fn list(&self) -> ProjectsHmacKeysListCall {
        ProjectsHmacKeysListCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
    /// Updates the state of an HMAC key. See the HMAC Key resource
    /// descriptor for valid states.
    pub fn update(&self, request: model::HmacKeyMetadata) -> ProjectsHmacKeysUpdateCall {
        ProjectsHmacKeysUpdateCall {
            client: self.client.clone(),
            request,
            ..Default::default()
        }
    }
}
#[derive(Debug, Default)]
pub struct ProjectsHmacKeysCreateCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    project_id: Option<String>,
}

impl ProjectsHmacKeysCreateCall {
    /// Project ID owning the service account.
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
        self
    }
    /// Email address of the service account.
    pub fn service_account_email(mut self, value: impl Into<String>) -> Self {
        self.url_params
            .insert("service_account_email".into(), vec![value.into()]);
        self
    }
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
    /// Name of the HMAC key to be deleted.
    pub fn access_id(mut self, value: impl Into<String>) -> Self {
        self.access_id = Some(value.into());
        self
    }
    /// Project ID owning the requested key
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
        self
    }
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
    /// Name of the HMAC key.
    pub fn access_id(mut self, value: impl Into<String>) -> Self {
        self.access_id = Some(value.into());
        self
    }
    /// Project ID owning the service account of the requested key.
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
        self
    }
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
    /// Name of the project in which to look for HMAC keys.
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
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
    /// Name of the HMAC key being updated.
    pub fn access_id(mut self, value: impl Into<String>) -> Self {
        self.access_id = Some(value.into());
        self
    }
    /// Project ID owning the service account of the updated key.
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
        self
    }
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
    pub fn get(&self) -> ProjectsServiceAccountGetCall {
        ProjectsServiceAccountGetCall {
            client: self.client.clone(),
            ..Default::default()
        }
    }
}
#[derive(Debug, Default)]
pub struct ProjectsServiceAccountGetCall {
    client: Client,
    url_params: std::collections::HashMap<String, Vec<String>>,
    project_id: Option<String>,
}

impl ProjectsServiceAccountGetCall {
    /// Project ID
    pub fn project_id(mut self, value: impl Into<String>) -> Self {
        self.project_id = Some(value.into());
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
