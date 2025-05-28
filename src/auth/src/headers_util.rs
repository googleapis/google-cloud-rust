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

use crate::Result;
use crate::credentials::{CacheableResource, QUOTA_PROJECT_KEY};
use crate::errors;
use crate::token::Token;

use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};

const API_KEY_HEADER_KEY: &str = "x-goog-api-key";

/// A utility function to create cacheable headers.
pub(crate) fn build_cacheable_headers(
    cached_token: &CacheableResource<Token>,
    quota_project_id: &Option<String>,
) -> Result<CacheableResource<HeaderMap>> {
    match cached_token {
        CacheableResource::NotModified => Ok(CacheableResource::NotModified),
        CacheableResource::New { entity_tag, data } => {
            let headers = build_bearer_headers(data, quota_project_id)?;
            Ok(CacheableResource::New {
                entity_tag: entity_tag.clone(),
                data: headers,
            })
        }
    }
}

/// A utility function to create bearer headers.
fn build_bearer_headers(
    token: &crate::token::Token,
    quota_project_id: &Option<String>,
) -> Result<HeaderMap> {
    build_headers(token, quota_project_id, AUTHORIZATION, |token| {
        HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(errors::non_retryable)
    })
}

pub(crate) fn build_cacheable_api_key_headers(
    cached_token: &CacheableResource<Token>,
    quota_project_id: &Option<String>,
) -> Result<CacheableResource<HeaderMap>> {
    match cached_token {
        CacheableResource::NotModified => Ok(CacheableResource::NotModified),
        CacheableResource::New { entity_tag, data } => {
            let headers = build_api_key_headers(data, quota_project_id)?;
            Ok(CacheableResource::New {
                entity_tag: entity_tag.clone(),
                data: headers,
            })
        }
    }
}

/// A utility function to create API key headers.
fn build_api_key_headers(
    token: &crate::token::Token,
    quota_project_id: &Option<String>,
) -> Result<HeaderMap> {
    build_headers(
        token,
        quota_project_id,
        HeaderName::from_static(API_KEY_HEADER_KEY),
        |token| HeaderValue::from_str(&token.token).map_err(errors::non_retryable),
    )
}

/// A helper to create auth headers.
fn build_headers(
    token: &crate::token::Token,
    quota_project_id: &Option<String>,
    header_name: HeaderName,
    build_header_value: impl FnOnce(&crate::token::Token) -> Result<HeaderValue>,
) -> Result<HeaderMap> {
    let mut value = build_header_value(token)?;
    value.set_sensitive(true);

    let mut header_map = HeaderMap::new();
    header_map.insert(header_name, value);

    if let Some(project) = quota_project_id {
        header_map.insert(
            HeaderName::from_static(QUOTA_PROJECT_KEY),
            HeaderValue::from_str(project).map_err(errors::non_retryable)?,
        );
    }

    Ok(header_map)
}

#[cfg(test)]
mod test {
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

        let result = build_cacheable_headers(&cacheable_token, &None);

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

        let result = build_cacheable_headers(&cacheable_token, &None);

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

        let quota_project_id = Some("test-project-123".to_string());
        let result = build_cacheable_headers(&cacheable_token, &quota_project_id);

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
    fn build_bearer_headers_different_token_type() {
        let token = create_test_token("special_token", "MAC");

        let result = build_bearer_headers(&token, &None);

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

        let result = build_bearer_headers(&token, &None);

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

        let result = build_cacheable_api_key_headers(&cacheable_token, &None);

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

        let result = build_cacheable_api_key_headers(&cacheable_token, &None);

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting new headers"),
            CacheableResource::NotModified => CacheableResource::<Token>::NotModified,
        };
    }

    #[test]
    fn build_cacheable_api_key_headers_with_quota_project() {
        let token = create_test_token("api_key_12345", "Bearer");
        let cacheable_token = CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: token,
        };

        let quota_project_id = Some("test-project-456".to_string());
        let result = build_cacheable_api_key_headers(&cacheable_token, &quota_project_id);

        assert!(result.is_ok());
        let cached_headers = result.unwrap();
        let headers = match cached_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };

        assert_eq!(headers.len(), 2, "{headers:?}");

        let api_key = headers
            .get(HeaderName::from_static(API_KEY_HEADER_KEY))
            .unwrap();

        assert_eq!(api_key, HeaderValue::from_static("api_key_12345"));
        assert!(api_key.is_sensitive());

        let quota_project = headers
            .get(HeaderName::from_static(QUOTA_PROJECT_KEY))
            .unwrap();
        assert_eq!(quota_project, HeaderValue::from_static("test-project-456"));
    }

    #[test]
    fn build_api_key_headers_invalid_token() {
        let token = create_test_token("api_key with \n invalid chars", "Bearer");
        let result = build_api_key_headers(&token, &None);
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
    fn build_api_key_headers_invalid_quota_project() {
        let token = create_test_token("api_key_12345", "Bearer");

        let invalid_quota_project = Some("project with \n invalid chars".to_string());
        let result = build_api_key_headers(&token, &invalid_quota_project);

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
}
