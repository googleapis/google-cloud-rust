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
//
// Code generated by sidekick. DO NOT EDIT.

use crate::Result;
#[allow(unused_imports)]
use gax::error::Error;

/// Implements [ApiKeys](super::stub::ApiKeys) using a [gaxi::http::ReqwestClient].
#[derive(Clone)]
pub struct ApiKeys {
    inner: gaxi::http::ReqwestClient,
}

impl std::fmt::Debug for ApiKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("ApiKeys")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ApiKeys {
    pub async fn new(config: gaxi::options::ClientConfig) -> gax::client_builder::Result<Self> {
        let inner = gaxi::http::ReqwestClient::new(config, crate::DEFAULT_HOST).await?;
        Ok(Self { inner })
    }
}

impl super::stub::ApiKeys for ApiKeys {
    async fn create_key(
        &self,
        req: crate::model::CreateKeyRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::Operation>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}/keys",
                    try_match(
                        Some(&req).map(|m| &m.parent).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::POST, path);
                let builder = builder.query(&[("keyId", &req.key_id)]);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::POST)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.parent).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                        ],
                        "parent",
                        "projects/*/locations/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner.execute(builder, Some(req.key), options).await
    }

    async fn list_keys(
        &self,
        req: crate::model::ListKeysRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::ListKeysResponse>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}/keys",
                    try_match(
                        Some(&req).map(|m| &m.parent).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::GET, path);
                let builder = builder.query(&[("pageSize", &req.page_size)]);
                let builder = builder.query(&[("pageToken", &req.page_token)]);
                let builder = builder.query(&[("showDeleted", &req.show_deleted)]);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::GET)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.parent).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                        ],
                        "parent",
                        "projects/*/locations/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner
            .execute(builder, gaxi::http::NoBody::new(&method), options)
            .await
    }

    async fn get_key(
        &self,
        req: crate::model::GetKeyRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::Key>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}",
                    try_match(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::GET, path);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::GET)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard,
                        ],
                        "name",
                        "projects/*/locations/*/keys/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner
            .execute(builder, gaxi::http::NoBody::new(&method), options)
            .await
    }

    async fn get_key_string(
        &self,
        req: crate::model::GetKeyStringRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::GetKeyStringResponse>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}/keyString",
                    try_match(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::GET, path);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::GET)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard,
                        ],
                        "name",
                        "projects/*/locations/*/keys/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner
            .execute(builder, gaxi::http::NoBody::new(&method), options)
            .await
    }

    async fn update_key(
        &self,
        req: crate::model::UpdateKeyRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::Operation>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}",
                    try_match(
                        Some(&req)
                            .and_then(|m| m.key.as_ref())
                            .map(|m| &m.name)
                            .map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::PATCH, path);
                let builder = (|| {
                    let builder = req
                        .update_mask
                        .as_ref()
                        .map(|p| serde_json::to_value(p).map_err(Error::ser))
                        .transpose()?
                        .into_iter()
                        .fold(builder, |builder, v| {
                            use gaxi::query_parameter::QueryParameter;
                            v.add(builder, "updateMask")
                        });
                    Ok(builder)
                })();
                Some(builder.map(|b| (b, reqwest::Method::PATCH)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req)
                            .and_then(|m| m.key.as_ref())
                            .map(|m| &m.name)
                            .map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard,
                        ],
                        "key.name",
                        "projects/*/locations/*/keys/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner.execute(builder, Some(req.key), options).await
    }

    async fn delete_key(
        &self,
        req: crate::model::DeleteKeyRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::Operation>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}",
                    try_match(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::DELETE, path);
                let builder = builder.query(&[("etag", &req.etag)]);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::DELETE)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard,
                        ],
                        "name",
                        "projects/*/locations/*/keys/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner
            .execute(builder, gaxi::http::NoBody::new(&method), options)
            .await
    }

    async fn undelete_key(
        &self,
        req: crate::model::UndeleteKeyRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::Operation>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}:undelete",
                    try_match(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard
                        ]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::POST, path);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::POST)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[
                            Segment::Literal("projects/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/locations/"),
                            Segment::SingleWildcard,
                            Segment::Literal("/keys/"),
                            Segment::SingleWildcard,
                        ],
                        "name",
                        "projects/*/locations/*/keys/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner.execute(builder, Some(req), options).await
    }

    async fn lookup_key(
        &self,
        req: crate::model::LookupKeyRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::LookupKeyResponse>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        let (builder, method) = None
            .or_else(|| {
                let path = "/v2/keys:lookupKey".to_string();

                let builder = self.inner.builder(reqwest::Method::GET, path);
                let builder = builder.query(&[("keyString", &req.key_string)]);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::GET)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner
            .execute(builder, gaxi::http::NoBody::new(&method), options)
            .await
    }

    async fn get_operation(
        &self,
        req: longrunning::model::GetOperationRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::Operation>> {
        use gax::error::binding::BindingError;
        use gaxi::path_parameter::PathMismatchBuilder;
        use gaxi::path_parameter::try_match;
        use gaxi::routing_parameter::Segment;
        let (builder, method) = None
            .or_else(|| {
                let path = format!(
                    "/v2/{}",
                    try_match(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[Segment::Literal("operations/"), Segment::SingleWildcard]
                    )?,
                );

                let builder = self.inner.builder(reqwest::Method::GET, path);
                let builder = Ok(builder);
                Some(builder.map(|b| (b, reqwest::Method::GET)))
            })
            .ok_or_else(|| {
                let mut paths = Vec::new();
                {
                    let builder = PathMismatchBuilder::default();
                    let builder = builder.maybe_add(
                        Some(&req).map(|m| &m.name).map(|s| s.as_str()),
                        &[Segment::Literal("operations/"), Segment::SingleWildcard],
                        "name",
                        "operations/*",
                    );
                    paths.push(builder.build());
                }
                gax::error::Error::binding(BindingError { paths })
            })??;
        let options = gax::options::internal::set_default_idempotency(
            options,
            gaxi::http::default_idempotency(&method),
        );
        let builder = builder.query(&[("$alt", "json;enum-encoding=int")]).header(
            "x-goog-api-client",
            reqwest::header::HeaderValue::from_static(&crate::info::X_GOOG_API_CLIENT_HEADER),
        );
        self.inner
            .execute(builder, gaxi::http::NoBody::new(&method), options)
            .await
    }

    fn get_polling_error_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> std::sync::Arc<dyn gax::polling_error_policy::PollingErrorPolicy> {
        self.inner.get_polling_error_policy(options)
    }

    fn get_polling_backoff_policy(
        &self,
        options: &gax::options::RequestOptions,
    ) -> std::sync::Arc<dyn gax::polling_backoff_policy::PollingBackoffPolicy> {
        self.inner.get_polling_backoff_policy(options)
    }
}
