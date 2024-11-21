// Copyright 2024 Google LLC
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

use crate::error::{Error, HttpError};
use serde::{Deserialize, Serialize};

mod generated;
pub use generated::*;

/// The [Status] type defines a logical error model that is suitable for
/// different programming environments, including REST APIs and RPC APIs. Each
/// [Status] message contains three pieces of data: error code, error message,
///  and error details.
///
/// You can find out more about this error model and how to work with it in the
/// [API Design Guide](https://cloud.google.com/apis/design/errors).
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
#[non_exhaustive]
pub struct Status {
    /// The status code.
    ///
    /// When using a HTTP transport this is the HTTP status code. When using
    /// gRPC, this is one of the values enumerated in [Code].
    pub code: i32,

    /// A developer-facing error message, which should be in English. Any
    /// user-facing error message should be localized and sent in the
    /// [Status] `details` field, or localized by the client.
    pub message: String,

    /// A list of messages that carry the error details.  There is a common set
    /// of message types for APIs to use.
    pub details: Vec<StatusDetails>,
}

/// The canonical error codes for APIs.
//
/// Sometimes multiple error codes may apply.  Services should return
/// the most specific error code that applies.  For example, prefer
/// `OUT_OF_RANGE` over `FAILED_PRECONDITION` if both codes apply.
/// Similarly prefer `NOT_FOUND` or `ALREADY_EXISTS` over `FAILED_PRECONDITION`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Code {
    /// Not an error; returned on success.
    ///
    /// HTTP Mapping: 200 OK
    Ok = 0,

    /// The operation was cancelled, typically by the caller.
    ///
    /// HTTP Mapping: 499 Client Closed Request
    Canceled = 1,

    /// Unknown error.  For example, this error may be returned when
    /// a `Status` value received from another address space belongs to
    /// an error space that is not known in this address space.  Also
    /// errors raised by APIs that do not return enough error information
    /// may be converted to this error.
    ///
    /// HTTP Mapping: 500 Internal Server Error
    Unknown = 2,

    /// The client specified an invalid argument.  Note that this differs
    /// from `FAILED_PRECONDITION`.  `INVALID_ARGUMENT` indicates arguments
    /// that are problematic regardless of the state of the system
    /// (e.g., a malformed file name).
    ///
    /// HTTP Mapping: 400 Bad Request
    InvalidArgument = 3,

    /// The deadline expired before the operation could complete. For operations
    /// that change the state of the system, this error may be returned
    /// even if the operation has completed successfully.  For example, a
    /// successful response from a server could have been delayed long
    /// enough for the deadline to expire.
    ///
    /// HTTP Mapping: 504 Gateway Timeout
    DeadlineExceeded = 4,

    /// Some requested entity (e.g., file or directory) was not found.
    ///
    /// Note to server developers: if a request is denied for an entire class
    /// of users, such as gradual feature rollout or undocumented allowlist,
    /// `NOT_FOUND` may be used. If a request is denied for some users within
    /// a class of users, such as user-based access control, `PERMISSION_DENIED`
    /// must be used.
    ///
    /// HTTP Mapping: 404 Not Found
    NotFound = 5,

    /// The entity that a client attempted to create (e.g., file or directory)
    /// already exists.
    ///
    /// HTTP Mapping: 409 Conflict
    AlreadyExists = 6,

    /// The caller does not have permission to execute the specified
    /// operation. `PERMISSION_DENIED` must not be used for rejections
    /// caused by exhausting some resource (use `RESOURCE_EXHAUSTED`
    /// instead for those errors). `PERMISSION_DENIED` must not be
    /// used if the caller can not be identified (use `UNAUTHENTICATED`
    /// instead for those errors). This error code does not imply the
    /// request is valid or the requested entity exists or satisfies
    /// other pre-conditions.
    ///
    /// HTTP Mapping: 403 Forbidden
    PermissionDenied = 7,

    /// Some resource has been exhausted, perhaps a per-user quota, or
    /// perhaps the entire file system is out of space.
    ///
    /// HTTP Mapping: 429 Too Many Requests
    ResourceExhausted = 8,

    /// The operation was rejected because the system is not in a state
    /// required for the operation's execution.  For example, the directory
    /// to be deleted is non-empty, an rmdir operation is applied to
    /// a non-directory, etc.
    ///
    /// Service implementors can use the following guidelines to decide
    /// between `FAILED_PRECONDITION`, `ABORTED`, and `UNAVAILABLE`:
    ///  (a) Use `UNAVAILABLE` if the client can retry just the failing call.
    ///  (b) Use `ABORTED` if the client should retry at a higher level. For
    ///      example, when a client-specified test-and-set fails, indicating the
    ///      client should restart a read-modify-write sequence.
    ///  (c) Use `FAILED_PRECONDITION` if the client should not retry until
    ///      the system state has been explicitly fixed. For example, if an "rmdir"
    ///      fails because the directory is non-empty, `FAILED_PRECONDITION`
    ///      should be returned since the client should not retry unless
    ///      the files are deleted from the directory.
    ///
    /// HTTP Mapping: 400 Bad Request
    FailedPrecondition = 9,

    /// The operation was aborted, typically due to a concurrency issue such as
    /// a sequencer check failure or transaction abort.
    ///
    /// See the guidelines above for deciding between `FAILED_PRECONDITION`,
    /// `ABORTED`, and `UNAVAILABLE`.
    ///
    /// HTTP Mapping: 409 Conflict
    /// HTTP Mapping: 400 Bad Request
    Aborted = 10,

    /// The operation was attempted past the valid range.  E.g., seeking or
    /// reading past end-of-file.
    ///
    /// Unlike `INVALID_ARGUMENT`, this error indicates a problem that may
    /// be fixed if the system state changes. For example, a 32-bit file
    /// system will generate `INVALID_ARGUMENT` if asked to read at an
    /// offset that is not in the range [0,2^32-1], but it will generate
    /// `OUT_OF_RANGE` if asked to read from an offset past the current
    /// file size.
    ///
    /// There is a fair bit of overlap between `FAILED_PRECONDITION` and
    /// `OUT_OF_RANGE`.  We recommend using `OUT_OF_RANGE` (the more specific
    /// error) when it applies so that callers who are iterating through
    /// a space can easily look for an `OUT_OF_RANGE` error to detect when
    /// they are done.
    ///
    /// HTTP Mapping: 400 Bad Request
    OutOfRange = 11,

    /// The operation is not implemented or is not supported/enabled in this
    /// service.
    ///
    /// HTTP Mapping: 501 Not Implemented
    Unimplemented = 12,

    /// Internal errors.  This means that some invariants expected by the
    /// underlying system have been broken.  This error code is reserved
    /// for serious errors.
    ///
    /// HTTP Mapping: 500 Internal Server Error
    Internal = 13,

    /// The service is currently unavailable.  This is most likely a
    /// transient condition, which can be corrected by retrying with
    /// a backoff. Note that it is not always safe to retry
    /// non-idempotent operations.
    ///
    /// See the guidelines above for deciding between `FAILED_PRECONDITION`,
    /// `ABORTED`, and `UNAVAILABLE`.
    ///
    /// HTTP Mapping: 503 Service Unavailable
    Unavailable = 14,

    /// Unrecoverable data loss or corruption.
    ///
    /// HTTP Mapping: 500 Internal Server Error
    DataLoss = 15,

    /// The request does not have valid authentication credentials for the
    /// operation.
    ///
    /// HTTP Mapping: 401 Unauthorized
    Unauthenticated = 16,
}

impl Default for Code {
    fn default() -> Self {
        Self::Unknown
    }
}

impl Serialize for Code {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.clone() as i32)
    }
}

impl<'de> Deserialize<'de> for Code {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match i32::deserialize(deserializer)? {
            0 => Ok(Code::Ok),
            1 => Ok(Code::Canceled),
            2 => Ok(Code::Unknown),
            3 => Ok(Code::InvalidArgument),
            4 => Ok(Code::DeadlineExceeded),
            5 => Ok(Code::NotFound),
            6 => Ok(Code::AlreadyExists),
            7 => Ok(Code::PermissionDenied),
            8 => Ok(Code::ResourceExhausted),
            9 => Ok(Code::FailedPrecondition),
            10 => Ok(Code::Aborted),
            11 => Ok(Code::OutOfRange),
            12 => Ok(Code::Unimplemented),
            13 => Ok(Code::Internal),
            14 => Ok(Code::Unavailable),
            15 => Ok(Code::DataLoss),
            16 => Ok(Code::Unauthenticated),
            _ => Ok(Code::default()),
        }
    }
}

/// A helper class to deserialized wrapped Status messages.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct ErrorWrapper {
    error: Status,
}

impl TryFrom<HttpError> for Status {
    type Error = Error;

    fn try_from(value: HttpError) -> Result<Self, Self::Error> {
        let wrapper: ErrorWrapper =
            serde_json::from_slice(value.payload().unwrap()).map_err(Error::serde)?;
        Ok(wrapper.error)
    }
}

impl TryFrom<bytes::Bytes> for Status {
    type Error = Error;

    fn try_from(value: bytes::Bytes) -> Result<Self, Self::Error> {
        let wrapper: ErrorWrapper = serde_json::from_slice(&value).map_err(Error::serde)?;
        Ok(wrapper.error)
    }
}

/// The type of details associated with [Status].
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
#[non_exhaustive]
pub enum StatusDetails {
    BadRequest(BadRequest),
    DebugInfo(DebugInfo),
    ErrorInfo(ErrorInfo),
    Help(Help),
    LocalizedMessage(LocalizedMessage),
    PreconditionFailure(PreconditionFailure),
    QuotaFailure(QuotaFailure),
    RequestInfo(RequestInfo),
    ResourceInfo(ResourceInfo),
    RetryInfo(RetryInfo),
    Other(wkt::Any),
}

impl Default for StatusDetails {
    fn default() -> Self {
        Self::Other(wkt::Any::default())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn serialization_all_variants() {
        let status = Status {
            code: 12,
            message: "test".to_string(),

            details: vec![
                StatusDetails::BadRequest(BadRequest {
                    field_violations: vec![bad_request::FieldViolation {
                        field: "field".to_string(),
                        description: "desc".to_string(),
                    }],
                }),
                StatusDetails::DebugInfo(DebugInfo {
                    stack_entries: vec!["stack".to_string()],
                    detail: "detail".to_string(),
                }),
                StatusDetails::ErrorInfo(ErrorInfo {
                    reason: "reason".to_string(),
                    domain: "domain".to_string(),
                    metadata: HashMap::new(),
                }),
                StatusDetails::Help(Help {
                    links: vec![help::Link {
                        description: "desc".to_string(),
                        url: "url".to_string(),
                    }],
                }),
                StatusDetails::LocalizedMessage(LocalizedMessage {
                    locale: "locale".to_string(),
                    message: "message".to_string(),
                }),
                StatusDetails::PreconditionFailure(PreconditionFailure {
                    violations: vec![precondition_failure::Violation {
                        r#type: "type".to_string(),
                        subject: "subject".to_string(),
                        description: "desc".to_string(),
                    }],
                }),
                StatusDetails::QuotaFailure(QuotaFailure {
                    violations: vec![quota_failure::Violation {
                        subject: "subject".to_string(),
                        description: "desc".to_string(),
                    }],
                }),
                StatusDetails::RequestInfo(RequestInfo {
                    request_id: "id".to_string(),
                    serving_data: "data".to_string(),
                }),
                StatusDetails::ResourceInfo(ResourceInfo {
                    resource_type: "type".to_string(),
                    resource_name: "name".to_string(),
                    owner: "owner".to_string(),
                    description: "desc".to_string(),
                }),
                StatusDetails::RetryInfo(RetryInfo {
                    retry_delay: Some(wkt::Duration::from_seconds(1)),
                }),
            ],
        };
        let got = serde_json::to_value(&status).unwrap();
        let want = json!({
            "code": 12,
            "message": "test",
            "details": [
                {"fieldViolations": [{"field": "field", "description": "desc"}]},
                {"stackEntries": ["stack"], "detail": "detail"},
                {"reason": "reason", "domain": "domain", "metadata": {}},
                {"links": [{"description": "desc", "url": "url"}]},
                {"locale": "locale", "message": "message"},
                {"violations": [{"type": "type", "subject": "subject", "description": "desc"}]},
                {"violations": [{"subject": "subject", "description": "desc"}]},
                {"requestId": "id", "servingData": "data"},
                {"resourceType": "type", "resourceName": "name", "owner": "owner", "description": "desc"},
                {"retryDelay": "1s"},
            ]
        });
        assert_eq!(got, want);
    }

    #[test]
    fn deserialization_all_variants() {
        let json = json!({
            "code": 20,
            "message": "test",
            "details": [
                {"fieldViolations": [{"field": "field", "description": "desc"}]},
                {"stackEntries": ["stack"], "detail": "detail"},
                {"reason": "reason", "domain": "domain", "metadata": {}},
                {"links": [{"description": "desc", "url": "url"}]},
                {"locale": "locale", "message": "message"},
                {"violations": [{"type": "type", "subject": "subject", "description": "desc"}]},
                {"violations": [{"subject": "subject", "description": "desc"}]},
                {"requestId": "id", "servingData": "data"},
                {"resourceType": "type", "resourceName": "name", "owner": "owner", "description": "desc"},
                {"retryDelay": "1s"},
            ]
        });
        let got: Status = serde_json::from_value(json).unwrap();
        let want = Status {
            code: 20,
            message: "test".to_string(),
            details: vec![
                StatusDetails::BadRequest(BadRequest {
                    field_violations: vec![bad_request::FieldViolation {
                        field: "field".to_string(),
                        description: "desc".to_string(),
                    }],
                }),
                StatusDetails::DebugInfo(DebugInfo {
                    stack_entries: vec!["stack".to_string()],
                    detail: "detail".to_string(),
                }),
                StatusDetails::ErrorInfo(ErrorInfo {
                    reason: "reason".to_string(),
                    domain: "domain".to_string(),
                    metadata: HashMap::new(),
                }),
                StatusDetails::Help(Help {
                    links: vec![help::Link {
                        description: "desc".to_string(),
                        url: "url".to_string(),
                    }],
                }),
                StatusDetails::LocalizedMessage(LocalizedMessage {
                    locale: "locale".to_string(),
                    message: "message".to_string(),
                }),
                StatusDetails::PreconditionFailure(PreconditionFailure {
                    violations: vec![precondition_failure::Violation {
                        r#type: "type".to_string(),
                        subject: "subject".to_string(),
                        description: "desc".to_string(),
                    }],
                }),
                StatusDetails::QuotaFailure(QuotaFailure {
                    violations: vec![quota_failure::Violation {
                        subject: "subject".to_string(),
                        description: "desc".to_string(),
                    }],
                }),
                StatusDetails::RequestInfo(RequestInfo {
                    request_id: "id".to_string(),
                    serving_data: "data".to_string(),
                }),
                StatusDetails::ResourceInfo(ResourceInfo {
                    resource_type: "type".to_string(),
                    resource_name: "name".to_string(),
                    owner: "owner".to_string(),
                    description: "desc".to_string(),
                }),
                StatusDetails::RetryInfo(RetryInfo {
                    retry_delay: Some(wkt::Duration::from_seconds(1)),
                }),
            ],
        };
        assert_eq!(got, want);
    }

    // This is a sample string received from production. It is useful to
    // validate the serialization helpers.
    const SAMPLE_PAYLOAD: &[u8] = b"{\n  \"error\": {\n    \"code\": 400,\n    \"message\": \"The provided Secret ID [] does not match the expected format [[a-zA-Z_0-9]+]\",\n    \"status\": \"INVALID_ARGUMENT\"\n  }\n}\n";

    #[test]
    fn deserialize_status() {
        let got = serde_json::from_slice::<ErrorWrapper>(SAMPLE_PAYLOAD).unwrap();
        let want = ErrorWrapper {
            error: Status {
                code: 400,
                message:
                    "The provided Secret ID [] does not match the expected format [[a-zA-Z_0-9]+]"
                        .into(),
                details: [].into(),
            },
        };
        assert_eq!(got.error, want.error);
    }

    #[test]
    fn try_from_bytes() {
        let got = TryInto::<Status>::try_into(bytes::Bytes::from_static(SAMPLE_PAYLOAD)).unwrap();
        let want = Status {
            code: 400,
            message: "The provided Secret ID [] does not match the expected format [[a-zA-Z_0-9]+]"
                .into(),
            details: [].into(),
        };
        assert_eq!(got, want);
    }
}
