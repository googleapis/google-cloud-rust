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
use crate::credentials::QUOTA_PROJECT_KEY;
use crate::errors;

use http::header::{AUTHORIZATION, HeaderName, HeaderValue};

const API_KEY_HEADER_KEY: &str = "x-goog-api-key";

/// A utility function to create bearer headers.
pub(crate) fn build_bearer_headers(
    token: &crate::token::Token,
    quota_project_id: &Option<String>,
) -> Result<Vec<(HeaderName, HeaderValue)>> {
    build_headers(
        token,
        quota_project_id,
        AUTHORIZATION,
        |token| HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
            .map_err(errors::non_retryable)
    )
}

/// A utility function to create API key headers.
pub(crate) fn build_api_key_headers(
    token: &crate::token::Token,
    quota_project_id: &Option<String>,
) -> Result<Vec<(HeaderName, HeaderValue)>> {
    build_headers(
        token,
        quota_project_id,
        HeaderName::from_static(API_KEY_HEADER_KEY),
        |token| HeaderValue::from_str(&token.token)
            .map_err(errors::non_retryable)
    )
}

/// A helper to create auth headers.
fn build_headers(
    token: &crate::token::Token,
    quota_project_id: &Option<String>,
    header_name: HeaderName,
    build_header_value: impl FnOnce(&crate::token::Token) -> Result<HeaderValue>
) -> Result<Vec<(HeaderName, HeaderValue)>> {
    let mut value = build_header_value(token)?;
    value.set_sensitive(true);

    let mut headers = vec![(header_name, value)];

    if let Some(project) = quota_project_id {
        headers.push((
            HeaderName::from_static(QUOTA_PROJECT_KEY),
            HeaderValue::from_str(project).map_err(errors::non_retryable)?
        ));
    }

    Ok(headers)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::token::Token;

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
    fn build_bearer_headers_basic_success() {
        let token = create_test_token("test_token", "Bearer");

        let result = build_bearer_headers(&token, &None);

        assert!(result.is_ok());
        let headers = result.unwrap();
        
        assert_eq!(headers.len(), 1);
        
        assert_eq!(headers[0].0, HeaderName::from_static("authorization"));
        assert_eq!(headers[0].1, HeaderValue::from_str("Bearer test_token").unwrap());
        assert!(headers[0].1.is_sensitive());
    }

    #[test]
    fn build_bearer_headers_with_quota_project_success() {
        let token = create_test_token("test_token", "Bearer");
        
        let quota_project_id = Some("test-project-123".to_string());
        let result = build_bearer_headers(&token, &quota_project_id);

        assert!(result.is_ok());
        let headers = result.unwrap();
        assert_eq!(headers.len(), 2);
        
        assert_eq!(headers[0].0, HeaderName::from_static("authorization"));
        assert_eq!(headers[0].1, HeaderValue::from_str("Bearer test_token").unwrap());
        assert!(headers[0].1.is_sensitive());
        
        assert_eq!(headers[1].0, HeaderName::from_static(QUOTA_PROJECT_KEY));
        assert_eq!(headers[1].1, HeaderValue::from_str("test-project-123").unwrap());
    }

    #[test]
    fn build_bearer_headers_different_token_type() {
        let token = create_test_token("special_token", "MAC");
        
        let result = build_bearer_headers(&token, &None);

        assert!(result.is_ok());
        let headers = result.unwrap();
        
        assert_eq!(headers.len(), 1);
        
        assert_eq!(headers[0].0, HeaderName::from_static("authorization"));
        assert_eq!(headers[0].1, HeaderValue::from_str("MAC special_token").unwrap());
        assert!(headers[0].1.is_sensitive());
    }

    #[test]
    fn build_bearer_headers_invalid_token() {
        let token = create_test_token("token with \n invalid chars", "Bearer");
        
        let result = build_bearer_headers(&token, &None);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("fail"));
    }

    #[test]
    fn build_api_key_headers_basic_success() {
        let token = create_test_token("api_key_12345", "Bearer");

        let result = build_api_key_headers(&token, &None);

        assert!(result.is_ok());
        let headers = result.unwrap();
        
        assert_eq!(headers.len(), 1);
        
        assert_eq!(headers[0].0, HeaderName::from_static(API_KEY_HEADER_KEY));
        assert_eq!(headers[0].1, HeaderValue::from_str("api_key_12345").unwrap());
        assert!(headers[0].1.is_sensitive());
    }

    #[test]
    fn build_api_key_headers_with_quota_project() {
        let token = create_test_token("api_key_12345", "Bearer");
        
        let quota_project_id = Some("test-project-456".to_string());
        let result = build_api_key_headers(&token, &quota_project_id);

        assert!(result.is_ok());
        let headers = result.unwrap();
        
        assert_eq!(headers.len(), 2);
        
        assert_eq!(headers[0].0, HeaderName::from_static(API_KEY_HEADER_KEY));
        assert_eq!(headers[0].1, HeaderValue::from_str("api_key_12345").unwrap());
        assert!(headers[0].1.is_sensitive());
        
        assert_eq!(headers[1].0, HeaderName::from_static(QUOTA_PROJECT_KEY));
        assert_eq!(headers[1].1, HeaderValue::from_str("test-project-456").unwrap());
    }

    #[test]
    fn build_api_key_headers_invalid_token() {
        let token = create_test_token("api_key with \n invalid chars", "Bearer");
        
        let result = build_api_key_headers(&token, &None);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("fail"));
    }

    #[test]
    fn build_api_key_headers_invalid_quota_project() {
        let token = create_test_token("api_key_12345", "Bearer");
        
        let invalid_quota_project = Some("project with \n invalid chars".to_string());
        let result = build_api_key_headers(&token, &invalid_quota_project);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("fail"));
    }
}
