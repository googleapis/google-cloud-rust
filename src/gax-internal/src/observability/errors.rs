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

use super::attributes as attr;

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
    pub(crate) fn from_reqwest_error<E>(err: &E) -> Self
    where
        E: ReqwestErrorDetails,
    {
        match err {
            e if e.is_timeout() => ErrorType::ClientTimeout,
            e if e.is_connect() => ErrorType::ClientConnectionError,
            e if e.is_request() => ErrorType::ClientRequestError,
            e if e.is_body() => ErrorType::ClientRequestBodyError,
            e if e.is_decode() => ErrorType::ClientResponseDecodeError,
            e if e.is_redirect() => ErrorType::ClientRedirectError,
            _ => ErrorType::Internal,
        }
    }

    pub(crate) fn as_str(&self) -> String {
        match self {
            ErrorType::HttpError {
                reason: Some(r), ..
            } => r.clone(),
            ErrorType::HttpError { code, .. } => code.as_str().to_string(),
            ErrorType::ClientTimeout => attr::ERROR_TYPE_CLIENT_TIMEOUT.to_string(),
            ErrorType::ClientConnectionError => {
                attr::ERROR_TYPE_CLIENT_CONNECTION_ERROR.to_string()
            }
            ErrorType::ClientRequestError => attr::ERROR_TYPE_CLIENT_REQUEST_ERROR.to_string(),
            ErrorType::ClientRequestBodyError => {
                attr::ERROR_TYPE_CLIENT_REQUEST_BODY_ERROR.to_string()
            }
            ErrorType::ClientResponseDecodeError => {
                attr::ERROR_TYPE_CLIENT_RESPONSE_DECODE_ERROR.to_string()
            }
            ErrorType::ClientRedirectError => attr::ERROR_TYPE_CLIENT_REDIRECT_ERROR.to_string(),
            ErrorType::Internal => attr::ERROR_TYPE_INTERNAL.to_string(),
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
    use test_case::test_case;

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

    #[test_case(ErrorType::HttpError { code: StatusCode::OK, reason: None }, "200", Code::Ok; "OK")]
    #[test_case(ErrorType::HttpError { code: StatusCode::BAD_REQUEST, reason: None }, "400", Code::InvalidArgument; "Bad Request")]
    #[test_case(ErrorType::HttpError { code: StatusCode::UNAUTHORIZED, reason: None }, "401", Code::Unauthenticated; "Unauthorized")]
    #[test_case(ErrorType::HttpError { code: StatusCode::FORBIDDEN, reason: None }, "403", Code::PermissionDenied; "Forbidden")]
    #[test_case(ErrorType::HttpError { code: StatusCode::NOT_FOUND, reason: None }, "404", Code::NotFound; "Not Found")]
    #[test_case(ErrorType::HttpError { code: StatusCode::CONFLICT, reason: None }, "409", Code::Aborted; "Conflict")]
    #[test_case(ErrorType::HttpError { code: StatusCode::TOO_MANY_REQUESTS, reason: None }, "429", Code::ResourceExhausted; "Too Many Requests")]
    #[test_case(ErrorType::HttpError { code: StatusCode::INTERNAL_SERVER_ERROR, reason: None }, "500", Code::Internal; "Internal Server Error")]
    #[test_case(ErrorType::HttpError { code: StatusCode::NOT_IMPLEMENTED, reason: None }, "501", Code::Unimplemented; "Not Implemented")]
    #[test_case(ErrorType::HttpError { code: StatusCode::SERVICE_UNAVAILABLE, reason: None }, "503", Code::Unavailable; "Service Unavailable")]
    #[test_case(ErrorType::HttpError { code: StatusCode::GATEWAY_TIMEOUT, reason: None }, "504", Code::DeadlineExceeded; "Gateway Timeout")]
    #[test_case(ErrorType::HttpError { code: StatusCode::IM_A_TEAPOT, reason: None }, "418", Code::FailedPrecondition; "I'm a teapot")]
    #[test_case(ErrorType::HttpError { code: StatusCode::CREATED, reason: None }, "201", Code::Ok; "Created")]
    #[test_case(ErrorType::HttpError { code: StatusCode::METHOD_NOT_ALLOWED, reason: None }, "405", Code::FailedPrecondition; "Method Not Allowed")]
    #[test_case(ErrorType::HttpError { code: StatusCode::BAD_GATEWAY, reason: None }, "502", Code::Internal; "Bad Gateway")]
    #[test_case(ErrorType::HttpError { code: StatusCode::from_u16(499).unwrap(), reason: None }, "499", Code::Cancelled; "Client Closed Request")]
    #[test_case(ErrorType::HttpError { code: StatusCode::BAD_REQUEST, reason: Some("REASON".to_string()) }, "REASON", Code::InvalidArgument; "Bad Request with Reason")]
    #[test_case(ErrorType::ClientTimeout, attr::ERROR_TYPE_CLIENT_TIMEOUT, Code::DeadlineExceeded; "Client Timeout")]
    #[test_case(ErrorType::ClientConnectionError, attr::ERROR_TYPE_CLIENT_CONNECTION_ERROR, Code::Unavailable; "Client Connection Error")]
    #[test_case(ErrorType::ClientRequestError, attr::ERROR_TYPE_CLIENT_REQUEST_ERROR, Code::InvalidArgument; "Client Request Error")]
    #[test_case(ErrorType::ClientRequestBodyError, attr::ERROR_TYPE_CLIENT_REQUEST_BODY_ERROR, Code::InvalidArgument; "Client Request Body Error")]
    #[test_case(ErrorType::ClientResponseDecodeError, attr::ERROR_TYPE_CLIENT_RESPONSE_DECODE_ERROR, Code::Internal; "Client Response Decode Error")]
    #[test_case(ErrorType::ClientRedirectError, attr::ERROR_TYPE_CLIENT_REDIRECT_ERROR, Code::Aborted; "Client Redirect Error")]
    #[test_case(ErrorType::Internal, attr::ERROR_TYPE_INTERNAL, Code::Internal; "Internal")]
    fn test_error_type_conversions(
        error_type: ErrorType,
        expected_as_str: &str,
        expected_grpc_code: Code,
    ) {
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

    #[test_case(MockReqwestError { is_timeout: true, ..Default::default() }, ErrorType::ClientTimeout; "Timeout")]
    #[test_case(MockReqwestError { is_connect: true, ..Default::default() }, ErrorType::ClientConnectionError; "Connect")]
    #[test_case(MockReqwestError { is_request: true, ..Default::default() }, ErrorType::ClientRequestError; "Request")]
    #[test_case(MockReqwestError { is_body: true, ..Default::default() }, ErrorType::ClientRequestBodyError; "Body")]
    #[test_case(MockReqwestError { is_decode: true, ..Default::default() }, ErrorType::ClientResponseDecodeError; "Decode")]
    #[test_case(MockReqwestError { is_redirect: true, ..Default::default() }, ErrorType::ClientRedirectError; "Redirect")]
    #[test_case(MockReqwestError { ..Default::default() }, ErrorType::Internal; "Internal")]
    fn test_from_reqwest_error(mock_err: MockReqwestError, expected_error_type: ErrorType) {
        assert_eq!(
            ErrorType::from_reqwest_error(&mock_err),
            expected_error_type
        );
    }
}
