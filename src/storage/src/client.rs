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

pub use crate::Error;
pub use crate::Result;
pub use control::model::Object;
use http::Extensions;

/// Implements a client for the Cloud Storage API.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::client::Storage;
/// let client = Storage::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # gax::Result::<()>::Ok(()) });
/// ```
///
/// # Configuration
///
/// To configure `Storage` use the `with_*` methods in the type returned
/// by [builder()][Storage::builder]. The default configuration should
/// work for most applications. Common configuration changes include
///
/// * [with_endpoint()]: by default this client uses the global default endpoint
///   (`https://storage.googleapis.com`). Applications using regional
///   endpoints or running in restricted networks (e.g. a network configured
///   with [Private Google Access with VPC Service Controls]) may want to
///   override this default.
/// * [with_credentials()]: by default this client uses
///   [Application Default Credentials]. Applications using custom
///   authentication may need to override this default.
///
/// # Pooling and Cloning
///
/// `Storage` holds a connection pool internally, it is advised to
/// create one and then reuse it.  You do not need to wrap `Storage` in
/// an [Rc](std::rc::Rc) or [Arc](std::sync::Arc) to reuse it, because it
/// already uses an `Arc` internally.
///
/// # Service Description
///
/// The Cloud Storage API allows applications to read and write data through
/// the abstractions of buckets and objects. For a description of these
/// abstractions please see <https://cloud.google.com/storage/docs>.
///
/// Resources are named as follows:
///
/// - Projects are referred to as they are defined by the Resource Manager API,
///   using strings like `projects/123456` or `projects/my-string-id`.
///
/// - Buckets are named using string names of the form:
///   `projects/{project}/buckets/{bucket}`
///   For globally unique buckets, `_` may be substituted for the project.
///
/// - Objects are uniquely identified by their name along with the name of the
///   bucket they belong to, as separate strings in this API. For example:
///   ```no_rust
///   bucket = "projects/_/buckets/my-bucket"
///   object = "my-object/with/a/folder-like/name"
///   ```
///   Note that object names can contain `/` characters, which are treated as
///   any other character (no special directory semantics).
///
/// [with_endpoint()]: ClientBuilder::with_endpoint
/// [with_credentials()]: ClientBuilder::with_credentials
/// [Private Google Access with VPC Service Controls]: https://cloud.google.com/vpc-service-controls/docs/private-connectivity
/// [Application Default Credentials]: https://cloud.google.com/docs/authentication#adc
#[derive(Clone, Debug)]
pub struct Storage {
    inner: reqwest::Client,
    cred: auth::credentials::Credentials,
    endpoint: String,
}

impl Storage {
    /// Returns a builder for [Storage].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// let client = Storage::builder().build().await?;
    /// # gax::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// A simple upload from a buffer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let response = client
    ///         .insert_object(
    ///             "projects/_/buckets/my-bucket",
    ///             "my-object",
    ///             "the quick brown fox jumped over the lazy dog",
    ///         )
    ///         .await?;
    ///     println!("response details={response:?}");
    ///     Ok(())
    /// }
    /// ```
    pub async fn insert_object<B, O, P>(
        &self,
        bucket: B,
        object: O,
        payload: P,
    ) -> crate::Result<Object>
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<bytes::Bytes>,
    {
        let bucket: String = bucket.into();
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::other(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = object.into();
        let builder = self
            .inner
            .request(
                reqwest::Method::POST,
                format!("{}upload/storage/v1/b/{bucket_id}/o", &self.endpoint),
            )
            .query(&[("uploadType", "media")])
            .query(&[("name", &object)])
            .header("content-type", "application/octet-stream")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );
        let auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;
        let builder = auth_headers
            .iter()
            .fold(builder, |b, (k, v)| b.header(k, v));
        let builder = builder.body(payload.into());

        tracing::info!("builder={builder:?}");
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.json::<v1::Object>().await.map_err(Error::io)?;

        Ok(Object::from(response))
    }

    /// A simple download into a buffer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let contents = client
    ///         .read_object(
    ///             "projects/_/buckets/my-bucket",
    ///             "my-object",
    ///         )
    ///         .await?;
    ///     println!("object contents={contents:?}");
    ///     Ok(())
    /// }
    /// ```
    pub async fn read_object<B, O>(&self, bucket: B, object: O) -> crate::Result<bytes::Bytes>
    where
        B: Into<String>,
        O: Into<String>,
    {
        let bucket: String = bucket.into();
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::other(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = object.into();
        let builder = self
            .inner
            .request(
                reqwest::Method::GET,
                format!("{}storage/v1/b/{bucket_id}/o/{object}", &self.endpoint),
            )
            .query(&[("alt", "media")])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );
        let auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;
        let builder = auth_headers
            .iter()
            .fold(builder, |b, (k, v)| b.header(k, v));
        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.bytes().await.map_err(Error::io)?;

        Ok(response)
    }

    pub(crate) async fn new(config: gaxi::options::ClientConfig) -> crate::Result<Self> {
        let inner = reqwest::Client::new();
        let cred = if let Some(c) = config.cred.clone() {
            c
        } else {
            auth::credentials::Builder::default()
                .build()
                .map_err(Error::authentication)?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| self::DEFAULT_HOST.to_string());
        Ok(Self {
            inner,
            cred,
            endpoint,
        })
    }
}

/// A builder for [Storage].
///
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::*;
/// # use client::ClientBuilder;
/// # use client::Storage;
/// let builder : ClientBuilder = Storage::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build().await?;
/// # gax::Result::<()>::Ok(()) });
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::Storage;
    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = Storage;
        type Credentials = gaxi::options::Credentials;
        async fn build(self, config: gaxi::options::ClientConfig) -> gax::Result<Self::Client> {
            Self::Client::new(config).await
        }
    }
}

/// The default host used by the service.
const DEFAULT_HOST: &str = "https://storage.googleapis.com/";

pub(crate) mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gaxi::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gaxi::api_header::GCCL,
            };
            ac.grpc_header_value()
        };
    }
}

mod v1 {
    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    // See http://cloud.google.com/storage/docs/json_api/v1/objects#resource for API reference.
    pub struct Object {
        id: String,
        name: String,
        bucket: String,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        generation: i64,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        metageneration: i64,
        content_type: String,
        storage_class: String,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        size: u64,
        component_count: i32,
        kms_key_name: String,
        etag: String,
        restore_token: Option<String>,
        content_encoding: String,
        content_disposition: String,
        content_language: String,
        cache_control: String,
        temporary_hold: bool,
        event_based_hold: Option<bool>,
        soft_delete_time: Option<wkt::Timestamp>,
        hard_delete_time: Option<wkt::Timestamp>,
        retention_expiration_time: Option<wkt::Timestamp>,
        time_created: wkt::Timestamp,
        time_finalized: wkt::Timestamp,
        time_deleted: Option<wkt::Timestamp>,
        time_storage_class_updated: wkt::Timestamp,
        updated: wkt::Timestamp,
        custom_time: Option<wkt::Timestamp>,
        acl: Vec<ObjectAccessControl>,
        // The following are excluded from the protos, so we don't really need to parse them.
        media_link: String,
        self_link: String,
        // TODO(#2039) - add all the other fields:
        //     "md5Hash": string,
        //     "crc32c": string,
        //     "retention": {
        //       "retainUntilTime": "datetime",
        //       "mode": string
        //     }
        //     "metadata": {
        //       (key): string
        //     },
        //     "owner": {
        //       "entity": string,
        //       "entityId": string
        //     },
        //     "customerEncryption": {
        //       "encryptionAlgorithm": string,
        //       "keySha256": string
        //     }
    }

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct ObjectAccessControl {
        id: String,
        entity: String,
        role: String,
        email: String,
        domain: String,
        entity_id: String,
        etag: String,
        project_team: Option<ProjectTeam>,
    }

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct ProjectTeam {
        project_number: String,
        team: String,
    }

    impl std::convert::From<ObjectAccessControl> for control::model::ObjectAccessControl {
        fn from(value: ObjectAccessControl) -> Self {
            Self::new()
                .set_id(value.id)
                .set_entity(value.entity)
                .set_role(value.role)
                .set_email(value.email)
                .set_domain(value.domain)
                .set_entity_id(value.entity_id)
                .set_etag(value.etag)
                .set_or_clear_project_team::<control::model::ProjectTeam>(
                    value.project_team.map(|x| x.into()),
                )
        }
    }

    impl std::convert::From<ProjectTeam> for control::model::ProjectTeam {
        fn from(p: ProjectTeam) -> Self {
            control::model::ProjectTeam::new()
                .set_project_number(p.project_number)
                .set_team(p.team)
        }
    }

    impl std::convert::From<Object> for control::model::Object {
        fn from(value: Object) -> Self {
            Self::new()
                .set_name(value.name)
                .set_bucket(format!("projects/_/buckets/{}", value.bucket))
                .set_generation(value.generation)
                .set_metageneration(value.metageneration)
                .set_content_type(value.content_type)
                .set_storage_class(value.storage_class)
                .set_size(value.size as i64)
                .set_kms_key(value.kms_key_name)
                .set_etag(value.etag)
                .set_or_clear_restore_token(value.restore_token)
                .set_content_encoding(value.content_encoding)
                .set_content_disposition(value.content_disposition)
                .set_content_language(value.content_language)
                .set_cache_control(value.cache_control)
                .set_temporary_hold(value.temporary_hold)
                .set_or_clear_event_based_hold(value.event_based_hold)
                .set_component_count(value.component_count)
                .set_or_clear_soft_delete_time(value.soft_delete_time)
                .set_or_clear_hard_delete_time(value.hard_delete_time)
                .set_or_clear_retention_expire_time(value.retention_expiration_time)
                .set_create_time(value.time_created)
                .set_finalize_time(value.time_finalized)
                .set_or_clear_delete_time(value.time_deleted)
                .set_update_storage_class_time(value.time_storage_class_updated)
                .set_or_clear_custom_time(value.custom_time)
                .set_update_time(value.updated)
                .set_acl(value.acl)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use test_case::test_case;

        #[test]
        fn test_deserialize_object() {
            let json = serde_json::json!({
                // string fields:
                "id": "obj1",
                "name": "test-object.txt",
                "bucket": "my-bucket",
                "contentType": "text/plain",
                "storageClass": "STANDARD",
                // i64 and u64 fields:
                "generation": "123",
                "metageneration": "456",
                "size": "789",
                // boolean fields:
                "temporaryHold": true,
                // number fields:
                "componentCount": 5,
                // datetime fields:
                "timeCreated": "2025-05-13T10:30:00Z",
                // list fields:
                "acl": [{"id": "acl-id","unknownField": 5, "projectTeam": {"projectNumber": "123456", "team": "myteam"}}]
            });
            let object: Object = serde_json::from_value(json)
                .expect("json value in object test should be deserializable");

            let want = Object {
                // string fields:
                id: "obj1".to_string(),
                name: "test-object.txt".to_string(),
                bucket: "my-bucket".to_string(),
                content_type: "text/plain".to_string(),
                storage_class: "STANDARD".to_string(),
                // i64 and u64 fields:
                generation: 123,
                metageneration: 456,
                size: 789,
                // boolean fields:
                temporary_hold: true,
                // number fields:
                component_count: 5,
                // datetime fields:
                time_created: wkt::Timestamp::clamp(1747132200, 0),
                acl: vec![ObjectAccessControl {
                    id: "acl-id".to_string(),
                    project_team: Some(ProjectTeam {
                        project_number: "123456".to_string(),
                        team: "myteam".to_string(),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            };

            assert_eq!(object, want);
        }

        #[test_case(Object::default(); "default fields")]
        #[test_case(Object {
            // string fields:
            id: "obj1".to_string(),
            name: "test-object.txt".to_string(),
            bucket: "my-bucket".to_string(),
            content_type: "text/plain".to_string(),
            storage_class: "STANDARD".to_string(),
            // i64 and u64 fields:
            generation: 123,
            metageneration: 456,
            size: 789,
            // boolean fields:
            temporary_hold: true,
            // number fields:
            component_count: 5,
            // datetime fields:
            time_created: wkt::Timestamp::clamp(1747132200, 0),
            // list fields:
            acl: vec![
                ObjectAccessControl {
                    id: "acl1".to_string(),
                    ..Default::default()
                },
                ObjectAccessControl {
                    id: "acl2".to_string(),
                    ..Default::default()
                },
            ],
            // unused in control::model
            media_link: "my-media-link".to_string(),
            ..Default::default()
        }; "some fields set")]
        #[test_case(Object {
            id: "obj1".to_string(),
            name: "test-object.txt".to_string(),
            bucket: "my-bucket".to_string(),
            generation: 123,
            metageneration: 456,
            content_type: "text/plain".to_string(),
            storage_class: "STANDARD".to_string(),
            size: 789,
            component_count: 101112,
            kms_key_name: "my-kms-key".to_string(),
            etag: "etag1".to_string(),
            restore_token: Some("restore-token1".to_string()),
            content_encoding: "content-encoding".to_string(),
            content_disposition: "content-disposition1".to_string(),
            content_language: "content-language1".to_string(),
            cache_control: "cache-control1".to_string(),
            temporary_hold: true,
            event_based_hold: Some(false),
            soft_delete_time: Some(wkt::Timestamp::clamp(1747132200, 1)),
            hard_delete_time: Some(wkt::Timestamp::clamp(1747132200, 2)),
            retention_expiration_time: Some(wkt::Timestamp::clamp(1747132200, 3)),
            time_created: wkt::Timestamp::clamp(1747132200, 4),
            time_finalized: wkt::Timestamp::clamp(1747132200, 5),
            time_deleted: Some(wkt::Timestamp::clamp(1747132200, 6)),
            time_storage_class_updated: wkt::Timestamp::clamp(1747132200, 7),
            updated: wkt::Timestamp::clamp(1747132200, 8),
            custom_time: Some(wkt::Timestamp::clamp(1747132200, 9)),
            acl: vec![
                ObjectAccessControl {
                    id: "acl1".to_string(),
                    ..Default::default()
                },
                ObjectAccessControl {
                    id: "acl2".to_string(),
                    ..Default::default()
                },
            ],
            // unused in control::model
            media_link: "my-media-link".to_string(),
            self_link: "my-self-link".to_string(),
        }; "all fields set")]
        // Tests for acl values.
        #[test_case(Object { acl: Vec::new(), ..Default::default()}; "empty acl")]
        #[test_case(Object {acl: vec![ObjectAccessControl::default(), object_acl_with_some_fields(), object_acl_with_all_fields()], ..Default::default()}; "acls with different fields")]
        fn test_convert_object_to_control_model(object: Object) {
            let got = control::model::Object::from(object.clone());
            assert_eq_object(object, got);
        }

        fn assert_eq_object(object: Object, got: control::model::Object) {
            assert_eq!(got.name, object.name);
            assert_eq!(got.bucket, format!("projects/_/buckets/{}", object.bucket));
            assert_eq!(got.etag, object.etag);
            assert_eq!(got.generation, object.generation);
            assert_eq!(got.restore_token, object.restore_token);
            assert_eq!(got.metageneration, object.metageneration);
            assert_eq!(got.storage_class, object.storage_class);
            assert_eq!(got.size, object.size as i64);
            assert_eq!(got.content_encoding, object.content_encoding);
            assert_eq!(got.content_disposition, object.content_disposition);
            assert_eq!(got.cache_control, object.cache_control);
            got.acl
                .iter()
                .zip(object.acl)
                .for_each(|a| assert_eq_object_access_control(a.0, &a.1));
            assert_eq!(got.content_language, object.content_language);
            assert_eq!(got.delete_time, object.time_deleted);
            assert_eq!(
                got.finalize_time.expect("finalize time is set"),
                object.time_finalized
            );
            assert_eq!(got.content_type, object.content_type);
            assert_eq!(
                got.create_time.expect("create time is set"),
                object.time_created
            );
            assert_eq!(got.component_count, object.component_count);
            assert_eq!(got.update_time.expect("update time is set"), object.updated);
            assert_eq!(got.kms_key, object.kms_key_name);
            assert_eq!(
                got.update_storage_class_time
                    .expect("update storage class time is set"),
                object.time_storage_class_updated
            );
            assert_eq!(got.temporary_hold, object.temporary_hold);
            assert_eq!(got.retention_expire_time, object.retention_expiration_time);
            assert_eq!(got.event_based_hold, object.event_based_hold);
            assert_eq!(got.custom_time, object.custom_time);
            assert_eq!(got.soft_delete_time, object.soft_delete_time);
            assert_eq!(got.hard_delete_time, object.hard_delete_time);
            // TODO(#2039): assert_eq!(got.checksums, object.checksums);
            // TODO(#2039): assert_eq!(got.metadata, object.metadata);
            // TODO(#2039): assert_eq!(got.owner, object.owner);
            // TODO(#2039): assert_eq!(got.customer_encryption, object.customer_encryption);
        }

        fn object_acl_with_all_fields() -> ObjectAccessControl {
            ObjectAccessControl {
                id: "acl1".to_string(),
                entity: "entity1".to_string(),
                role: "role1".to_string(),
                email: "email1".to_string(),
                domain: "domain1".to_string(),
                entity_id: "entity1".to_string(),
                etag: "etag1".to_string(),
                project_team: Some(ProjectTeam {
                    project_number: "123456".to_string(),
                    team: "team1".to_string(),
                }),
            }
        }

        fn object_acl_with_some_fields() -> ObjectAccessControl {
            ObjectAccessControl {
                id: "acl1".to_string(),
                entity: "entity1".to_string(),
                role: "role1".to_string(),
                project_team: Some(ProjectTeam {
                    project_number: "123456".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }

        #[test_case(ObjectAccessControl::default(); "default fields")]
        #[test_case(object_acl_with_all_fields(); "all fields have values")]
        #[test_case(object_acl_with_some_fields(); "some fields have values" )]
        fn test_object_access_control(from: ObjectAccessControl) {
            let got = control::model::ObjectAccessControl::from(from.clone());
            assert_eq_object_access_control(&got, &from);
        }

        fn assert_eq_object_access_control(
            got: &control::model::ObjectAccessControl,
            from: &ObjectAccessControl,
        ) {
            assert_eq!(got.id, from.id);
            assert_eq!(got.entity, from.entity);
            assert_eq!(got.role, from.role);
            assert_eq!(got.email, from.email);
            assert_eq!(got.domain, from.domain);
            assert_eq!(got.entity_id, from.entity_id);
            assert_eq!(got.etag, from.etag);
            if let Some(got) = &got.project_team {
                if let Some(from) = &from.project_team {
                    assert_eq!(got.project_number, from.project_number);
                    assert_eq!(got.team, from.team);
                } else {
                    panic!("expected None, got {:?}", got); // false, lets get the error.
                }
            } else {
                assert_eq!(from.project_team, None);
            }
        }
    }
}
