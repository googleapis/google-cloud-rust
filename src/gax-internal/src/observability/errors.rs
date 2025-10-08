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

use gax::error::rpc::Code;
use http::StatusCode;

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorType {
    HttpError {
        code: StatusCode,
        reason: Option<String>,
    },
    ClientTimeout,
    ClientConnectionError,
    ClientRequestError,
    ClientRequestBodyError,
    ClientResponseDecodeError,
    ClientRedirectError,
    Internal,
}

// Trait to abstract reqwest::Error for testing
pub trait ReqwestErrorDetails {
    fn is_timeout(&self) -> bool;
    fn is_connect(&self) -> bool;
    fn is_request(&self) -> bool;
    fn is_body(&self) -> bool;
    fn is_decode(&self) -> bool;
    fn is_redirect(&self) -> bool;
}

impl ReqwestErrorDetails for reqwest::Error {
    fn is_timeout(&self) -> bool {
        self.is_timeout()
    }
    fn is_connect(&self) -> bool {
        self.is_connect()
    }
    fn is_request(&self) -> bool {
        self.is_request()
    }
    fn is_body(&self) -> bool {
        self.is_body()
    }
    fn is_decode(&self) -> bool {
        self.is_decode()
    }
    fn is_redirect(&self) -> bool {
        self.is_redirect()
    }
}

impl ErrorType {
    pub(crate) fn from_reqwest_error(err: &dyn ReqwestErrorDetails) -> Self {
        if err.is_timeout() {
            ErrorType::ClientTimeout
        } else if err.is_connect() {
            ErrorType::ClientConnectionError
        } else if err.is_request() {
            ErrorType::ClientRequestError
        } else if err.is_body() {
            ErrorType::ClientRequestBodyError
        } else if err.is_decode() {
            ErrorType::ClientResponseDecodeError
        } else if err.is_redirect() {
            ErrorType::ClientRedirectError
        } else {
            ErrorType::Internal
        }
    }

    pub(crate) fn as_str(&self) -> String {
        match self {
            ErrorType::HttpError {
                reason: Some(r), ..
            } => r.clone(),
            ErrorType::HttpError { code, .. } => code.as_str().to_string(),
            ErrorType::ClientTimeout => "CLIENT_TIMEOUT".to_string(),
            ErrorType::ClientConnectionError => "CLIENT_CONNECTION_ERROR".to_string(),
            ErrorType::ClientRequestError => "CLIENT_REQUEST_ERROR".to_string(),
            ErrorType::ClientRequestBodyError => "CLIENT_REQUEST_BODY_ERROR".to_string(),
            ErrorType::ClientResponseDecodeError => "CLIENT_RESPONSE_DECODE_ERROR".to_string(),
            ErrorType::ClientRedirectError => "CLIENT_REDIRECT_ERROR".to_string(),
            ErrorType::Internal => "INTERNAL".to_string(),
        }
    }

    pub(crate) fn grpc_code(&self) -> Code {
        match self {
            ErrorType::HttpError { code, .. } => match code.as_u16() {
                200 => Code::Ok,
                400 => Code::InvalidArgument,
                401 => Code::Unauthenticated,
                403 => Code::PermissionDenied,
                404 => Code::NotFound,
                409 => Code::Aborted,
                429 => Code::ResourceExhausted,
                499 => Code::Cancelled,
                501 => Code::Unimplemented,
                503 => Code::Unavailable,
                504 => Code::DeadlineExceeded,
                _ if code.is_success() => Code::Ok,
                _ if code.is_client_error() => Code::FailedPrecondition,
                _ if code.is_server_error() => Code::Internal,
                _ => Code::Unknown,
            },
            ErrorType::ClientTimeout => Code::DeadlineExceeded,
            ErrorType::ClientConnectionError => Code::Unavailable,
            ErrorType::ClientRequestError => Code::InvalidArgument,
            ErrorType::ClientRequestBodyError => Code::InvalidArgument,
            ErrorType::ClientResponseDecodeError => Code::Internal,
            ErrorType::ClientRedirectError => Code::Aborted,
            ErrorType::Internal => Code::Internal,
        }
    }

    pub(crate) fn grpc_status(&self) -> String {
        self.grpc_code().name().to_string()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use http::StatusCode;

    #[derive(Default)]
    pub struct MockReqwestError {
        pub is_timeout: bool,
        pub is_connect: bool,
        pub is_request: bool,
        pub is_body: bool,
        pub is_decode: bool,
        pub is_redirect: bool,
    }

    impl ReqwestErrorDetails for MockReqwestError {
        fn is_timeout(&self) -> bool {
            self.is_timeout
        }
        fn is_connect(&self) -> bool {
            self.is_connect
        }
        fn is_request(&self) -> bool {
            self.is_request
        }
        fn is_body(&self) -> bool {
            self.is_body
        }
        fn is_decode(&self) -> bool {
            self.is_decode
        }
        fn is_redirect(&self) -> bool {
            self.is_redirect
        }
    }

    fn http_error(http_code: StatusCode, rpc_code: Code) -> (ErrorType, String, Code) {
        (
            ErrorType::HttpError {
                code: http_code,
                reason: None,
            },
            http_code.as_str().to_string(),
            rpc_code,
        )
    }

    fn client_error(error_type: ErrorType, name: &str, code: Code) -> (ErrorType, String, Code) {
        (error_type, name.to_string(), code)
    }

    #[test]
    fn test_error_type_conversions() {
        // Table of test case tuples.
        // (error_type: ErrorType, expected_as_str: String, expected_grpc_code: Code)
        let test_cases = vec![
            http_error(StatusCode::OK, Code::Ok),
            http_error(StatusCode::BAD_REQUEST, Code::InvalidArgument),
            http_error(StatusCode::UNAUTHORIZED, Code::Unauthenticated),
            http_error(StatusCode::FORBIDDEN, Code::PermissionDenied),
            http_error(StatusCode::NOT_FOUND, Code::NotFound),
            http_error(StatusCode::CONFLICT, Code::Aborted),
            http_error(StatusCode::TOO_MANY_REQUESTS, Code::ResourceExhausted),
            http_error(StatusCode::INTERNAL_SERVER_ERROR, Code::Internal),
            http_error(StatusCode::NOT_IMPLEMENTED, Code::Unimplemented),
            http_error(StatusCode::SERVICE_UNAVAILABLE, Code::Unavailable),
            http_error(StatusCode::GATEWAY_TIMEOUT, Code::DeadlineExceeded),
            http_error(StatusCode::IM_A_TEAPOT, Code::FailedPrecondition),
            http_error(StatusCode::CREATED, Code::Ok),
            http_error(StatusCode::METHOD_NOT_ALLOWED, Code::FailedPrecondition),
            http_error(StatusCode::BAD_GATEWAY, Code::Internal),
            // Client closed request.
            http_error(StatusCode::from_u16(499).unwrap(), Code::Cancelled),
            (
                ErrorType::HttpError {
                    code: StatusCode::BAD_REQUEST,
                    reason: Some("REASON".to_string()),
                },
                "REASON".to_string(),
                Code::InvalidArgument,
            ),
            client_error(
                ErrorType::ClientTimeout,
                "CLIENT_TIMEOUT",
                Code::DeadlineExceeded,
            ),
            client_error(
                ErrorType::ClientConnectionError,
                "CLIENT_CONNECTION_ERROR",
                Code::Unavailable,
            ),
            client_error(
                ErrorType::ClientRequestError,
                "CLIENT_REQUEST_ERROR",
                Code::InvalidArgument,
            ),
            client_error(
                ErrorType::ClientRequestBodyError,
                "CLIENT_REQUEST_BODY_ERROR",
                Code::InvalidArgument,
            ),
            client_error(
                ErrorType::ClientResponseDecodeError,
                "CLIENT_RESPONSE_DECODE_ERROR",
                Code::Internal,
            ),
            client_error(
                ErrorType::ClientRedirectError,
                "CLIENT_REDIRECT_ERROR",
                Code::Aborted,
            ),
            client_error(ErrorType::Internal, "INTERNAL", Code::Internal),
        ];

        for (error_type, expected_as_str, expected_grpc_code) in test_cases {
            assert_eq!(
                error_type.as_str(),
                expected_as_str,
                "expected as_str for {:?}",
                error_type
            );
            assert_eq!(
                error_type.grpc_code(),
                expected_grpc_code,
                "grpc_code for {:?}",
                error_type
            );
            assert_eq!(
                error_type.grpc_status(),
                expected_grpc_code.name().to_string(),
                "grpc_status for {:?}",
                error_type
            );
        }
    }

    #[test]
    fn test_from_reqwest_error() {
        let test_cases = vec![
            (
                MockReqwestError {
                    is_timeout: true,
                    ..Default::default()
                },
                ErrorType::ClientTimeout,
            ),
            (
                MockReqwestError {
                    is_connect: true,
                    ..Default::default()
                },
                ErrorType::ClientConnectionError,
            ),
            (
                MockReqwestError {
                    is_request: true,
                    ..Default::default()
                },
                ErrorType::ClientRequestError,
            ),
            (
                MockReqwestError {
                    is_body: true,
                    ..Default::default()
                },
                ErrorType::ClientRequestBodyError,
            ),
            (
                MockReqwestError {
                    is_decode: true,
                    ..Default::default()
                },
                ErrorType::ClientResponseDecodeError,
            ),
            (
                MockReqwestError {
                    is_redirect: true,
                    ..Default::default()
                },
                ErrorType::ClientRedirectError,
            ),
            (
                MockReqwestError {
                    ..Default::default()
                },
                ErrorType::Internal,
            ),
        ];

        for (mock_err, expected_error_type) in test_cases {
            assert_eq!(
                ErrorType::from_reqwest_error(&mock_err),
                expected_error_type
            );
        }
    }
}
