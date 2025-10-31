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

use http::StatusCode;

use super::attributes::error_type_values::*;

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorType {
    HttpError {
        code: StatusCode,
        reason: Option<String>,
    },
    ClientTimeout,
    ClientConnectionError,
    ClientRequestError,
    ClientResponseDecodeError,
    ClientAuthenticationError,
    ClientRetryExhausted,
    Internal,
}

impl ErrorType {
    pub(crate) fn from_gax_error(err: &gax::error::Error) -> Self {
        match err {
            e if e.is_timeout() => ErrorType::ClientTimeout,
            e if e.is_exhausted() => ErrorType::ClientRetryExhausted,
            e if e.is_binding() => ErrorType::ClientRequestError,
            e if e.is_serialization() => ErrorType::ClientRequestError,
            e if e.is_deserialization() => ErrorType::ClientResponseDecodeError,
            e if e.is_authentication() => ErrorType::ClientAuthenticationError,
            e if e.is_io() || e.is_connect() => ErrorType::ClientConnectionError,
            e => {
                if let Some(status) = e.http_status_code() {
                    ErrorType::HttpError {
                        code: http::StatusCode::from_u16(status)
                            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR),
                        reason: None,
                    }
                } else {
                    ErrorType::Internal
                }
            }
        }
    }

    pub(crate) fn as_str(&self) -> String {
        match self {
            ErrorType::HttpError {
                reason: Some(r), ..
            } => r.clone(),
            ErrorType::HttpError { code, .. } => code.as_str().to_string(),
            ErrorType::ClientTimeout => CLIENT_TIMEOUT.to_string(),
            ErrorType::ClientConnectionError => CLIENT_CONNECTION_ERROR.to_string(),
            ErrorType::ClientRequestError => CLIENT_REQUEST_ERROR.to_string(),
            ErrorType::ClientResponseDecodeError => CLIENT_RESPONSE_DECODE_ERROR.to_string(),
            ErrorType::ClientAuthenticationError => CLIENT_AUTHENTICATION_ERROR.to_string(),
            ErrorType::ClientRetryExhausted => CLIENT_RETRY_EXHAUSTED.to_string(),
            ErrorType::Internal => INTERNAL.to_string(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use gax::error::Error;
    use http::{HeaderMap, StatusCode};
    use test_case::test_case;

    #[test_case(ErrorType::HttpError { code: StatusCode::OK, reason: None }, "200"; "OK")]
    #[test_case(ErrorType::HttpError { code: StatusCode::BAD_REQUEST, reason: None }, "400"; "Bad Request")]
    #[test_case(ErrorType::HttpError { code: StatusCode::UNAUTHORIZED, reason: None }, "401"; "Unauthorized")]
    #[test_case(ErrorType::HttpError { code: StatusCode::FORBIDDEN, reason: None }, "403"; "Forbidden")]
    #[test_case(ErrorType::HttpError { code: StatusCode::NOT_FOUND, reason: None }, "404"; "Not Found")]
    #[test_case(ErrorType::HttpError { code: StatusCode::CONFLICT, reason: None }, "409"; "Conflict")]
    #[test_case(ErrorType::HttpError { code: StatusCode::TOO_MANY_REQUESTS, reason: None }, "429"; "Too Many Requests")]
    #[test_case(ErrorType::HttpError { code: StatusCode::INTERNAL_SERVER_ERROR, reason: None }, "500"; "Internal Server Error")]
    #[test_case(ErrorType::HttpError { code: StatusCode::NOT_IMPLEMENTED, reason: None }, "501"; "Not Implemented")]
    #[test_case(ErrorType::HttpError { code: StatusCode::SERVICE_UNAVAILABLE, reason: None }, "503"; "Service Unavailable")]
    #[test_case(ErrorType::HttpError { code: StatusCode::GATEWAY_TIMEOUT, reason: None }, "504"; "Gateway Timeout")]
    #[test_case(ErrorType::HttpError { code: StatusCode::IM_A_TEAPOT, reason: None }, "418"; "I'm a teapot")]
    #[test_case(ErrorType::HttpError { code: StatusCode::CREATED, reason: None }, "201"; "Created")]
    #[test_case(ErrorType::HttpError { code: StatusCode::METHOD_NOT_ALLOWED, reason: None }, "405"; "Method Not Allowed")]
    #[test_case(ErrorType::HttpError { code: StatusCode::BAD_GATEWAY, reason: None }, "502"; "Bad Gateway")]
    #[test_case(ErrorType::HttpError { code: StatusCode::from_u16(499).unwrap(), reason: None }, "499"; "Client Closed Request")]
    #[test_case(ErrorType::HttpError { code: StatusCode::BAD_REQUEST, reason: Some("REASON".to_string()) }, "REASON"; "Bad Request with Reason")]
    #[test_case(ErrorType::ClientTimeout, CLIENT_TIMEOUT; "Client Timeout")]
    #[test_case(ErrorType::ClientConnectionError, CLIENT_CONNECTION_ERROR; "Client Connection Error")]
    #[test_case(ErrorType::ClientRequestError, CLIENT_REQUEST_ERROR; "Client Request Error")]
    #[test_case(ErrorType::ClientResponseDecodeError, CLIENT_RESPONSE_DECODE_ERROR; "Client Response Decode Error")]
    #[test_case(ErrorType::ClientAuthenticationError, CLIENT_AUTHENTICATION_ERROR; "Client Authentication Error")]
    #[test_case(ErrorType::ClientRetryExhausted, CLIENT_RETRY_EXHAUSTED; "Client Retry Exhausted")]
    #[test_case(ErrorType::Internal, INTERNAL; "Internal")]
    fn test_error_type_conversions(error_type: ErrorType, expected_as_str: &str) {
        assert_eq!(
            error_type.as_str(),
            expected_as_str,
            "expected as_str for {:?}",
            error_type
        );
    }

    #[test_case(Error::timeout("test"), CLIENT_TIMEOUT; "Timeout")]
    #[test_case(Error::exhausted("test"), CLIENT_RETRY_EXHAUSTED; "Exhausted")]
    #[test_case(Error::binding("test"), CLIENT_REQUEST_ERROR; "Binding")]
    #[test_case(Error::ser("test"), CLIENT_REQUEST_ERROR; "Serialization")]
    #[test_case(Error::deser("test"), CLIENT_RESPONSE_DECODE_ERROR; "Deserialization")]
    #[test_case(Error::authentication(gax::error::CredentialsError::from_msg(false, "test")), CLIENT_AUTHENTICATION_ERROR; "Authentication")]
    #[test_case(Error::io("test"), CLIENT_CONNECTION_ERROR; "IO")]
    #[test_case(Error::http(404, HeaderMap::new(), bytes::Bytes::new()), "404"; "HTTP 404")]
    #[test_case(Error::http(503, HeaderMap::new(), bytes::Bytes::new()), "503"; "HTTP 503")]
    #[test_case(Error::service(gax::error::rpc::Status::default()), INTERNAL; "Internal")]
    fn test_from_gax_error(err: Error, expected: &str) {
        assert_eq!(ErrorType::from_gax_error(&err).as_str(), expected);
    }
}
