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

use http::header::{HeaderName, HeaderValue};

/// A utility function to create auth headers.
pub(crate) fn build_headers(
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

    #[test]
    fn build_headers_basic_success() {
        // Create a token for testing
        let token = Token {
            token: "test_token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        // Test with just the authorization header (no quota project)
        let result = build_headers(
            &token,
            &None,
            HeaderName::from_static("authorization"),
            |token| {
                Ok(
                    HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
                        .unwrap(),
                )
            },
        );

        assert!(result.is_ok());
        let headers = result.unwrap();
        assert_eq!(headers.len(), 1);

        // Check authorization header
        assert_eq!(headers[0].0, HeaderName::from_static("authorization"));
        assert_eq!(
            headers[0].1,
            HeaderValue::from_str("Bearer test_token").unwrap()
        );
        assert!(headers[0].1.is_sensitive());
    }

    #[test]
    fn build_headers_with_quota_project_success() {
        let token = Token {
            token: "test_token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        // Test with both authorization and quota project headers
        let quota_project_id = Some("test-project-123".to_string());
        let result = build_headers(
            &token,
            &quota_project_id,
            HeaderName::from_static("authorization"),
            |token| {
                Ok(
                    HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
                        .unwrap(),
                )
            },
        );

        assert!(result.is_ok());
        let headers = result.unwrap();
        assert_eq!(headers.len(), 2);

        // Check authorization header
        assert_eq!(headers[0].0, HeaderName::from_static("authorization"));
        assert_eq!(
            headers[0].1,
            HeaderValue::from_str("Bearer test_token").unwrap()
        );
        assert!(headers[0].1.is_sensitive());

        // Check quota project header
        assert_eq!(headers[1].0, HeaderName::from_static(QUOTA_PROJECT_KEY));
        assert_eq!(
            headers[1].1,
            HeaderValue::from_str("test-project-123").unwrap()
        );
    }

    #[test]
    fn build_headers_custom_header() {
        let token = Token {
            token: "api_key_12345".to_string(),
            token_type: "API-Key".to_string(),
            expires_at: None,
            metadata: None,
        };

        // Test with a custom header (e.g. API_KEY_HEADER_KEY)
        let result = build_headers(
            &token,
            &None,
            HeaderName::from_static("x-api-key"),
            |token| Ok(HeaderValue::from_str(&token.token).unwrap()),
        );

        assert!(result.is_ok());
        let headers = result.unwrap();
        assert_eq!(headers.len(), 1);

        // Check custom header
        assert_eq!(headers[0].0, HeaderName::from_static("x-api-key"));
        assert_eq!(
            headers[0].1,
            HeaderValue::from_str("api_key_12345").unwrap()
        );
        assert!(headers[0].1.is_sensitive());
    }

    #[test]
    fn build_headers_header_value_failure() {
        let token = Token {
            token: "token with invalid chars \n\r".to_string(), // Contains invalid header value characters
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        // Test error handling when creating header value fails
        let result = build_headers(
            &token,
            &None,
            HeaderName::from_static("authorization"),
            |token| {
                HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
                    .map_err(errors::non_retryable)
            },
        );

        assert!(result.is_err());

        // Check that the error is properly propagated
        let error = result.unwrap_err();
        assert!(error.to_string().contains("fail"));
    }

    #[test]
    fn build_headers_quota_project_failure() {
        let token = Token {
            token: "valid_token".to_string(),
            token_type: "Bearer".to_string(),
            expires_at: None,
            metadata: None,
        };

        // Test error handling when quota project has invalid characters
        let invalid_project = Some("project with invalid chars \n\r".to_string());

        let result = build_headers(
            &token,
            &invalid_project,
            HeaderName::from_static("authorization"),
            |token| {
                Ok(
                    HeaderValue::from_str(&format!("{} {}", token.token_type, token.token))
                        .unwrap(),
                )
            },
        );

        assert!(result.is_err());

        // Check that the error is properly propagated
        let error = result.unwrap_err();
        assert!(error.to_string().contains("fail"));
    }
}
