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

//! Headers utility functions to work with Google Cloud authentication [Credentials].
//!
//! [Credentials]: https://cloud.google.com/docs/authentication#credentials

use crate::Result;
use crate::constants::TRUST_BOUNDARY_HEADER;
use crate::credentials::{CacheableResource, QUOTA_PROJECT_KEY};
use crate::errors;
use crate::token::Token;
use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};

mod build_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/build_env.rs"));
}

/// The name of the telemetry header.
pub(crate) const X_GOOG_API_CLIENT: &str = "x-goog-api-client";

/// Access token request type.
pub(crate) const ACCESS_TOKEN_REQUEST_TYPE: &str = "at";

#[cfg(feature = "idtoken")]
/// ID token request type.
pub(crate) const ID_TOKEN_REQUEST_TYPE: &str = "it";

/// Format the struct as needed for the `x-goog-api-client` header.
pub(crate) fn metrics_header_value(request_type: &str, cred_type: &str) -> String {
    let rustc_version = build_info::RUSTC_VERSION;
    let auth_version = build_info::PKG_VERSION;

    format!(
        "gl-rust/{rustc_version} auth/{auth_version} auth-request-type/{request_type} cred-type/{cred_type}"
    )
}

const API_KEY_HEADER_KEY: &str = "x-goog-api-key";

/// Known auth headers currently supported
#[derive(Debug)]
pub(crate) struct AuthHeadersBuilder {
    token: CacheableResource<Token>,
    quota_project_id: Option<String>,
    access_boundary: Option<String>,
    header_type: HeaderType,
}

impl AuthHeadersBuilder {
    // Build bearer token headers
    pub(crate) fn new(token: CacheableResource<Token>) -> AuthHeadersBuilder {
        AuthHeadersBuilder {
            token,
            header_type: HeaderType::Bearer,
            quota_project_id: None,
            access_boundary: None,
        }
    }

    // Build API Key headers
    pub(crate) fn for_api_key(token: CacheableResource<Token>) -> AuthHeadersBuilder {
        AuthHeadersBuilder {
            token,
            header_type: HeaderType::ApiKey,
            quota_project_id: None,
            access_boundary: None,
        }
    }

    pub(crate) fn with_quota_project_id<S: Into<String>>(mut self, quota_project_id: S) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_access_boundary<S: Into<String>>(mut self, access_boundary: S) -> Self {
        self.access_boundary = Some(access_boundary.into());
        self
    }

    pub(crate) fn build(&self) -> Result<CacheableResource<HeaderMap>> {
        match &self.token {
            CacheableResource::NotModified => Ok(CacheableResource::NotModified),
            CacheableResource::New { entity_tag, data } => {
                let data = &AuthHeaderData {
                    token: data.to_owned(),
                    quota_project_id: self.quota_project_id.clone(),
                    access_boundary: self.access_boundary.clone(),
                    header_type: self.header_type.clone(),
                };
                Ok(CacheableResource::New {
                    entity_tag: entity_tag.clone(),
                    data: data.as_headers()?,
                })
            }
        }
    }
}

struct AuthHeaderData {
    token: Token,
    quota_project_id: Option<String>,
    access_boundary: Option<String>,
    header_type: HeaderType,
}

#[derive(Debug, Clone)]
enum HeaderType {
    Bearer,
    ApiKey,
}

impl AuthHeaderData {
    fn header_name(&self) -> HeaderName {
        match self.header_type {
            HeaderType::Bearer => AUTHORIZATION,
            HeaderType::ApiKey => HeaderName::from_static(API_KEY_HEADER_KEY),
        }
    }

    fn header_value(&self) -> Result<HeaderValue> {
        match self.header_type {
            HeaderType::Bearer => {
                HeaderValue::from_str(&format!("{} {}", self.token.token_type, self.token.token))
                    .map_err(errors::non_retryable)
            }
            HeaderType::ApiKey => {
                HeaderValue::from_str(&self.token.token).map_err(errors::non_retryable)
            }
        }
    }

    fn as_headers(&self) -> Result<HeaderMap> {
        let mut value = self.header_value()?;
        value.set_sensitive(true);

        let header_name = self.header_name();
        let mut header_map = HeaderMap::new();
        header_map.insert(header_name, value);

        if let Some(project) = self.quota_project_id.clone() {
            header_map.insert(
                HeaderName::from_static(QUOTA_PROJECT_KEY),
                HeaderValue::from_str(&project).map_err(errors::non_retryable)?,
            );
        }

        if let Some(access_boundary) = self.access_boundary.clone() {
            header_map.insert(
                HeaderName::from_static(TRUST_BOUNDARY_HEADER),
                HeaderValue::from_str(&access_boundary).map_err(errors::non_retryable)?,
            );
        }

        Ok(header_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{credentials::EntityTag, token::Token};
    use std::error::Error as _;

    // Helper to create test tokens
    fn create_test_token(token: &str, token_type: &str) -> Token {
        Token {
            token: token.to_string(),
            token_type: token_type.to_string(),
            expires_at: None,
            metadata: None,
        }
    }

    #[test]
    fn build_cacheable_headers_basic_success() {
        let token = create_test_token("test_token", "Bearer");
        let cacheable_token = CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: token,
        };

        let result = AuthHeadersBuilder::new(cacheable_token).build();

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        let headers = match cached_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };

        assert_eq!(headers.len(), 1, "{headers:?}");
        let value = headers
            .get(HeaderName::from_static("authorization"))
            .unwrap();

        assert_eq!(value, HeaderValue::from_static("Bearer test_token"));
        assert!(value.is_sensitive());
    }

    #[test]
    fn build_cacheable_headers_basic_not_modified() {
        let cacheable_token = CacheableResource::NotModified;

        let result = AuthHeadersBuilder::new(cacheable_token).build();

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<Token>::NotModified,
        };
    }

    #[test]
    fn build_cacheable_headers_with_quota_project_success() {
        let token = create_test_token("test_token", "Bearer");
        let cacheable_token = CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: token,
        };

        let quota_project_id = "test-project-123".to_string();
        let result = AuthHeadersBuilder::new(cacheable_token)
            .with_quota_project_id(quota_project_id)
            .build();

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        let headers = match cached_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        assert_eq!(headers.len(), 2, "{headers:?}");

        let token = headers
            .get(HeaderName::from_static("authorization"))
            .unwrap();
        assert_eq!(token, HeaderValue::from_static("Bearer test_token"));
        assert!(token.is_sensitive());

        let quota_project = headers
            .get(HeaderName::from_static(QUOTA_PROJECT_KEY))
            .unwrap();
        assert_eq!(quota_project, HeaderValue::from_static("test-project-123"));
    }

    #[test]
    fn build_cacheable_headers_with_trust_boundary_success() {
        let token = create_test_token("test_token", "Bearer");
        let cacheable_token = CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: token,
        };

        let trust_boundary = "test-trust-boundary".to_string();
        let result = AuthHeadersBuilder::new(cacheable_token)
            .with_access_boundary(trust_boundary)
            .build();

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        let headers = match cached_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        assert_eq!(headers.len(), 2, "{headers:?}");

        let token = headers
            .get(HeaderName::from_static("authorization"))
            .unwrap();
        assert_eq!(token, HeaderValue::from_static("Bearer test_token"));
        assert!(token.is_sensitive());

        let trust_boundary = headers
            .get(HeaderName::from_static(TRUST_BOUNDARY_HEADER))
            .unwrap();
        assert_eq!(
            trust_boundary,
            HeaderValue::from_static("test-trust-boundary")
        );
    }

    #[test]
    fn build_bearer_headers_different_token_type() {
        let token = create_test_token("special_token", "MAC");

        let data = AuthHeaderData {
            token,
            header_type: HeaderType::Bearer,
            quota_project_id: None,
            access_boundary: None,
        };
        let result = data.as_headers();

        assert!(result.is_ok());
        let headers = result.unwrap();

        assert_eq!(headers.len(), 1, "{headers:?}");

        let token = headers
            .get(HeaderName::from_static("authorization"))
            .unwrap();

        assert_eq!(token, HeaderValue::from_static("MAC special_token"));
        assert!(token.is_sensitive());
    }

    #[test]
    fn build_bearer_headers_invalid_token() {
        let token = create_test_token("token with \n invalid chars", "Bearer");

        let data = AuthHeaderData {
            token,
            header_type: HeaderType::Bearer,
            quota_project_id: None,
            access_boundary: None,
        };
        let result = data.as_headers();

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(!error.is_transient(), "{error:?}");
        let source = error
            .source()
            .and_then(|e| e.downcast_ref::<http::header::InvalidHeaderValue>());
        assert!(
            matches!(source, Some(http::header::InvalidHeaderValue { .. })),
            "{error:?}"
        );
    }

    #[test]
    fn build_cacheable_api_key_headers_basic_success() {
        let token = create_test_token("api_key_12345", "Bearer");
        let cacheable_token = CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: token,
        };

        let result = AuthHeadersBuilder::for_api_key(cacheable_token).build();

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        let headers = match cached_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };

        assert_eq!(headers.len(), 1, "{headers:?}");
        let api_key = headers
            .get(HeaderName::from_static(API_KEY_HEADER_KEY))
            .unwrap();

        assert_eq!(api_key, HeaderValue::from_static("api_key_12345"));
        assert!(api_key.is_sensitive());
    }

    #[test]
    fn build_cacheable_api_key_headers_basic_not_modified() {
        let cacheable_token = CacheableResource::NotModified;

        let result = AuthHeadersBuilder::for_api_key(cacheable_token).build();

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<Token>::NotModified,
        };
    }

    #[test]
    fn build_api_key_headers_invalid_token() {
        let token = create_test_token("api_key with \n invalid chars", "Bearer");
        let data = AuthHeaderData {
            token,
            header_type: HeaderType::ApiKey,
            quota_project_id: None,
            access_boundary: None,
        };
        let result = data.as_headers();

        let error = result.unwrap_err();
        assert!(!error.is_transient(), "{error:?}");
        let source = error
            .source()
            .and_then(|e| e.downcast_ref::<http::header::InvalidHeaderValue>());
        assert!(
            matches!(source, Some(http::header::InvalidHeaderValue { .. })),
            "{error:?}"
        );
    }

    #[test]
    fn test_metrics_header_value() {
        let header = metrics_header_value("at", "u");
        let rustc_version = build_info::RUSTC_VERSION;
        let expected = format!(
            "gl-rust/{} auth/{} auth-request-type/at cred-type/u",
            rustc_version,
            build_info::PKG_VERSION
        );
        assert_eq!(header, expected);
    }
}
