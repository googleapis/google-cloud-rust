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

use crate::error::Error;
use serde::{Deserialize, Serialize};

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

    /// The underlying `google.rpc.Status.code`, as a string.
    ///
    /// When serialized over JSON, status messages include both the HTTP status
    /// code (in the `code` field), and the status [Code] as a string.
    pub status: Option<String>,

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

impl std::convert::From<i32> for Code {
    fn from(value: i32) -> Self {
        match value {
            0 => Code::Ok,
            1 => Code::Canceled,
            2 => Code::Unknown,
            3 => Code::InvalidArgument,
            4 => Code::DeadlineExceeded,
            5 => Code::NotFound,
            6 => Code::AlreadyExists,
            7 => Code::PermissionDenied,
            8 => Code::ResourceExhausted,
            9 => Code::FailedPrecondition,
            10 => Code::Aborted,
            11 => Code::OutOfRange,
            12 => Code::Unimplemented,
            13 => Code::Internal,
            14 => Code::Unavailable,
            15 => Code::DataLoss,
            16 => Code::Unauthenticated,
            _ => Code::default(),
        }
    }
}

impl std::convert::From<Code> for String {
    fn from(value: Code) -> String {
        match value {
            Code::Ok => "OK",
            Code::Canceled => "CANCELED",
            Code::Unknown => "UNKNOWN",
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Code::NotFound => "NOT_FOUND",
            Code::AlreadyExists => "ALREADY_EXISTS",
            Code::PermissionDenied => "PERMISSION_DENIED",
            Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Code::FailedPrecondition => "FAILED_PRECONDITION",
            Code::Aborted => "ABORTED",
            Code::OutOfRange => "OUT_OF_RANGE",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::Internal => "INTERNAL",
            Code::Unavailable => "UNAVAILABLE",
            Code::DataLoss => "DATA_LOSS",
            Code::Unauthenticated => "UNAUTHENTICATED",
        }
        .to_string()
    }
}

impl std::convert::TryFrom<&str> for Code {
    type Error = String;
    fn try_from(value: &str) -> std::result::Result<Code, Self::Error> {
        match value {
            "OK" => Ok(Code::Ok),
            "CANCELED" => Ok(Code::Canceled),
            "UNKNOWN" => Ok(Code::Unknown),
            "INVALID_ARGUMENT" => Ok(Code::InvalidArgument),
            "DEADLINE_EXCEEDED" => Ok(Code::DeadlineExceeded),
            "NOT_FOUND" => Ok(Code::NotFound),
            "ALREADY_EXISTS" => Ok(Code::AlreadyExists),
            "PERMISSION_DENIED" => Ok(Code::PermissionDenied),
            "RESOURCE_EXHAUSTED" => Ok(Code::ResourceExhausted),
            "FAILED_PRECONDITION" => Ok(Code::FailedPrecondition),
            "ABORTED" => Ok(Code::Aborted),
            "OUT_OF_RANGE" => Ok(Code::OutOfRange),
            "UNIMPLEMENTED" => Ok(Code::Unimplemented),
            "INTERNAL" => Ok(Code::Internal),
            "UNAVAILABLE" => Ok(Code::Unavailable),
            "DATA_LOSS" => Ok(Code::DataLoss),
            "UNAUTHENTICATED" => Ok(Code::Unauthenticated),
            _ => Err(format!("unknown status code value {value}")),
        }
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
        i32::deserialize(deserializer).map(Code::from)
    }
}

/// A helper class to deserialized wrapped Status messages.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct ErrorWrapper {
    error: Status,
}

impl TryFrom<&bytes::Bytes> for Status {
    type Error = Error;

    fn try_from(value: &bytes::Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice::<ErrorWrapper>(value)
            .map(|w| w.error)
            .map_err(Error::serde)
    }
}

impl From<rpc::model::Status> for Status {
    fn from(value: rpc::model::Status) -> Self {
        Self {
            code: value.code,
            message: value.message,
            status: Some(String::from(Code::from(value.code))),
            details: value.details.into_iter().map(StatusDetails::from).collect(),
        }
    }
}

/// The type of details associated with [Status].
///
/// Google cloud RPCs often return a detailed error description. This details
/// can be used to better understand the root cause of the problem.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
#[serde(tag = "@type")]
pub enum StatusDetails {
    #[serde(rename = "google.rpc.BadRequest")]
    BadRequest(rpc::model::BadRequest),
    #[serde(rename = "google.rpc.DebugInfo")]
    DebugInfo(rpc::model::DebugInfo),
    #[serde(rename = "google.rpc.ErrorInfo")]
    ErrorInfo(rpc::model::ErrorInfo),
    #[serde(rename = "google.rpc.Help")]
    Help(rpc::model::Help),
    #[serde(rename = "google.rpc.LocalizedMessage")]
    LocalizedMessage(rpc::model::LocalizedMessage),
    #[serde(rename = "google.rpc.PreconditionFailure")]
    PreconditionFailure(rpc::model::PreconditionFailure),
    #[serde(rename = "google.rpc.QuotaFailure")]
    QuotaFailure(rpc::model::QuotaFailure),
    #[serde(rename = "google.rpc.RequestInfo")]
    RequestInfo(rpc::model::RequestInfo),
    #[serde(rename = "google.rpc.ResourceInfo")]
    ResourceInfo(rpc::model::ResourceInfo),
    #[serde(rename = "google.rpc.RetryInfo")]
    RetryInfo(rpc::model::RetryInfo),
    Other(wkt::Any),
}

impl From<wkt::Any> for StatusDetails {
    fn from(value: wkt::Any) -> Self {
        use rpc::model::*;
        if let Ok(v) = value.try_into_message::<BadRequest>() {
            return StatusDetails::BadRequest(v);
        } else if let Ok(v) = value.try_into_message::<DebugInfo>() {
            return StatusDetails::DebugInfo(v);
        } else if let Ok(v) = value.try_into_message::<ErrorInfo>() {
            return StatusDetails::ErrorInfo(v);
        } else if let Ok(v) = value.try_into_message::<Help>() {
            return StatusDetails::Help(v);
        } else if let Ok(v) = value.try_into_message::<LocalizedMessage>() {
            return StatusDetails::LocalizedMessage(v);
        } else if let Ok(v) = value.try_into_message::<PreconditionFailure>() {
            return StatusDetails::PreconditionFailure(v);
        } else if let Ok(v) = value.try_into_message::<QuotaFailure>() {
            return StatusDetails::QuotaFailure(v);
        } else if let Ok(v) = value.try_into_message::<RequestInfo>() {
            return StatusDetails::RequestInfo(v);
        } else if let Ok(v) = value.try_into_message::<ResourceInfo>() {
            return StatusDetails::ResourceInfo(v);
        } else if let Ok(v) = value.try_into_message::<RetryInfo>() {
            return StatusDetails::RetryInfo(v);
        }
        StatusDetails::Other(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rpc::model::BadRequest;
    use rpc::model::DebugInfo;
    use rpc::model::ErrorInfo;
    use rpc::model::Help;
    use rpc::model::LocalizedMessage;
    use rpc::model::PreconditionFailure;
    use rpc::model::QuotaFailure;
    use rpc::model::RequestInfo;
    use rpc::model::ResourceInfo;
    use rpc::model::RetryInfo;
    use serde_json::json;
    use std::collections::HashMap;
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn serialization_all_variants() {
        let status =
            Status {
                code: 12,
                message: "test".to_string(),
                status: Some("UNIMPLEMENTED".to_string()),

                details: vec![
                    StatusDetails::BadRequest(BadRequest::default().set_field_violations(
                        vec![rpc::model::bad_request::FieldViolation::default()
                        .set_field("field").set_description("desc")],
                    )),
                    StatusDetails::DebugInfo(
                        DebugInfo::default()
                            .set_stack_entries(vec!["stack".to_string()])
                            .set_detail("detail"),
                    ),
                    StatusDetails::ErrorInfo(
                        ErrorInfo::default()
                            .set_reason("reason")
                            .set_domain("domain")
                            .set_metadata(HashMap::new()),
                    ),
                    StatusDetails::Help(Help::default().set_links(
                        vec![rpc::model::help::Link::default()
                        .set_description( "desc")
                        .set_url( "url")
                    ],
                    )),
                    StatusDetails::LocalizedMessage(
                        LocalizedMessage::default()
                            .set_locale("locale")
                            .set_message("message"),
                    ),
                    StatusDetails::PreconditionFailure(
                        PreconditionFailure::default().set_violations(vec![
                            rpc::model::precondition_failure::Violation::default()
                                .set_type("type")
                                .set_subject("subject")
                                .set_description("desc"),
                        ]),
                    ),
                    StatusDetails::QuotaFailure(QuotaFailure::default().set_violations(
                        vec![rpc::model::quota_failure::Violation::default()
                        .set_subject( "subject")
                        .set_description( "desc")
                    ],
                    )),
                    StatusDetails::RequestInfo(
                        RequestInfo::default()
                            .set_request_id("id")
                            .set_serving_data("data"),
                    ),
                    StatusDetails::ResourceInfo(
                        ResourceInfo::default()
                            .set_resource_type("type")
                            .set_resource_name("name")
                            .set_owner("owner")
                            .set_description("desc"),
                    ),
                    StatusDetails::RetryInfo(
                        RetryInfo::default().set_retry_delay(wkt::Duration::clamp(1, 0)),
                    ),
                ],
            };
        // TODO(#505) - change this test so it does not require updates as new
        //     fields appear.
        let got = serde_json::to_value(&status).unwrap();
        let want = json!({
            "code": 12,
            "message": "test",
            "status": "UNIMPLEMENTED",
            "details": [
                {"@type": "google.rpc.BadRequest", "fieldViolations": [{"field": "field", "description": "desc"}]},
                {"@type": "google.rpc.DebugInfo", "stackEntries": ["stack"], "detail": "detail"},
                {"@type": "google.rpc.ErrorInfo", "reason": "reason", "domain": "domain"},
                {"@type": "google.rpc.Help", "links": [{"description": "desc", "url": "url"}]},
                {"@type": "google.rpc.LocalizedMessage", "locale": "locale", "message": "message"},
                {"@type": "google.rpc.PreconditionFailure", "violations": [{"type": "type", "subject": "subject", "description": "desc"}]},
                {"@type": "google.rpc.QuotaFailure", "violations": [{"subject": "subject", "description": "desc"}]},
                {"@type": "google.rpc.RequestInfo", "requestId": "id", "servingData": "data"},
                {"@type": "google.rpc.ResourceInfo", "resourceType": "type", "resourceName": "name", "owner": "owner", "description": "desc"},
                {"@type": "google.rpc.RetryInfo", "retryDelay": "1s"},
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
                {"@type": "google.rpc.BadRequest", "fieldViolations": [{"field": "field", "description": "desc"}]},
                {"@type": "google.rpc.DebugInfo", "stackEntries": ["stack"], "detail": "detail"},
                {"@type": "google.rpc.ErrorInfo", "reason": "reason", "domain": "domain", "metadata": {}},
                {"@type": "google.rpc.Help", "links": [{"description": "desc", "url": "url"}]},
                {"@type": "google.rpc.LocalizedMessage", "locale": "locale", "message": "message"},
                {"@type": "google.rpc.PreconditionFailure", "violations": [{"type": "type", "subject": "subject", "description": "desc"}]},
                {"@type": "google.rpc.QuotaFailure", "violations": [{"subject": "subject", "description": "desc"}]},
                {"@type": "google.rpc.RequestInfo", "requestId": "id", "servingData": "data"},
                {"@type": "google.rpc.ResourceInfo", "resourceType": "type", "resourceName": "name", "owner": "owner", "description": "desc"},
                {"@type": "google.rpc.RetryInfo", "retryDelay": "1s"},
            ]
        });
        let got: Status = serde_json::from_value(json).unwrap();
        let want = Status {
            code: 20,
            message: "test".to_string(),
            status: None,
            details: vec![
                StatusDetails::BadRequest(BadRequest::default().set_field_violations(
                    vec![rpc::model::bad_request::FieldViolation::default()
                        .set_field( "field" )
                        .set_description( "desc" )
                    ],
                )),
                StatusDetails::DebugInfo(
                    DebugInfo::default()
                        .set_stack_entries(vec!["stack".to_string()])
                        .set_detail("detail"),
                ),
                StatusDetails::ErrorInfo(
                    ErrorInfo::default()
                        .set_reason("reason")
                        .set_domain("domain"),
                ),
                StatusDetails::Help(Help::default().set_links(vec![
                    rpc::model::help::Link::default().set_description("desc").set_url("url"),
                ])),
                StatusDetails::LocalizedMessage(
                    LocalizedMessage::default()
                        .set_locale("locale")
                        .set_message("message"),
                ),
                StatusDetails::PreconditionFailure(PreconditionFailure::default().set_violations(
                    vec![rpc::model::precondition_failure::Violation::default()
                        .set_type( "type" )
                        .set_subject( "subject" )
                        .set_description( "desc" )
                    ],
                )),
                StatusDetails::QuotaFailure(QuotaFailure::default().set_violations(
                    vec![rpc::model::quota_failure::Violation::default()
                        .set_subject( "subject")
                        .set_description( "desc")
                    ],
                )),
                StatusDetails::RequestInfo(
                    RequestInfo::default()
                        .set_request_id("id")
                        .set_serving_data("data"),
                ),
                StatusDetails::ResourceInfo(
                    ResourceInfo::default()
                        .set_resource_type("type")
                        .set_resource_name("name")
                        .set_owner("owner")
                        .set_description("desc"),
                ),
                StatusDetails::RetryInfo(
                    RetryInfo::default().set_retry_delay(wkt::Duration::clamp(1, 0)),
                ),
            ],
        };
        assert_eq!(got, want);
    }

    #[test]
    fn status_from_rpc_no_details() {
        let input = rpc::model::Status::default()
            .set_code(Code::Unavailable as i32)
            .set_message("try-again");
        let got = Status::from(input);
        assert_eq!(got.code, Code::Unavailable as i32);
        assert_eq!(got.message, "try-again");
        assert_eq!(got.status.as_deref(), Some("UNAVAILABLE"));
    }

    #[test_case(
        BadRequest::default(),
        StatusDetails::BadRequest(BadRequest::default())
    )]
    #[test_case(DebugInfo::default(), StatusDetails::DebugInfo(DebugInfo::default()))]
    #[test_case(ErrorInfo::default(), StatusDetails::ErrorInfo(ErrorInfo::default()))]
    #[test_case(Help::default(), StatusDetails::Help(Help::default()))]
    #[test_case(
        LocalizedMessage::default(),
        StatusDetails::LocalizedMessage(LocalizedMessage::default())
    )]
    #[test_case(
        PreconditionFailure::default(),
        StatusDetails::PreconditionFailure(PreconditionFailure::default())
    )]
    #[test_case(
        QuotaFailure::default(),
        StatusDetails::QuotaFailure(QuotaFailure::default())
    )]
    #[test_case(
        RequestInfo::default(),
        StatusDetails::RequestInfo(RequestInfo::default())
    )]
    #[test_case(
        ResourceInfo::default(),
        StatusDetails::ResourceInfo(ResourceInfo::default())
    )]
    #[test_case(RetryInfo::default(), StatusDetails::RetryInfo(RetryInfo::default()))]
    fn status_from_rpc_status_known_detail_type<T>(detail: T, want: StatusDetails)
    where
        T: wkt::message::Message + serde::ser::Serialize,
    {
        let input = rpc::model::Status::default()
            .set_code(Code::Unavailable as i32)
            .set_message("try-again")
            .set_details(vec![wkt::Any::try_from(&detail).unwrap()]);

        let status = Status::from(input);
        assert_eq!(status.code, Code::Unavailable as i32);
        assert_eq!(status.message, "try-again");
        assert_eq!(status.status.as_deref(), Some("UNAVAILABLE"));

        let got = status.details.get(0);
        assert_eq!(got, Some(&want));
    }

    #[test]
    fn status_from_rpc_unknown_details() {
        let any = wkt::Any::try_from(&wkt::Duration::clamp(123, 0)).unwrap();
        let input = rpc::model::Status::default()
            .set_code(Code::Unavailable as i32)
            .set_message("try-again")
            .set_details(vec![any.clone()]);
        let got = Status::from(input);
        assert_eq!(got.code, Code::Unavailable as i32);
        assert_eq!(got.message, "try-again");
        assert_eq!(got.status.as_deref(), Some("UNAVAILABLE"));

        let got = got.details.get(0);
        let want = StatusDetails::Other(any);
        assert_eq!(got, Some(&want));
    }

    // This is a sample string received from production. It is useful to
    // validate the serialization helpers.
    const SAMPLE_PAYLOAD: &[u8] = b"{\n  \"error\": {\n    \"code\": 400,\n    \"message\": \"The provided Secret ID [] does not match the expected format [[a-zA-Z_0-9]+]\",\n    \"status\": \"INVALID_ARGUMENT\"\n  }\n}\n";

    // The corresponding status message.
    fn sample_status() -> Status {
        Status {
            code: 400,
            status: Some("INVALID_ARGUMENT".to_string()),
            message: "The provided Secret ID [] does not match the expected format [[a-zA-Z_0-9]+]"
                .into(),
            details: [].into(),
        }
    }

    #[test]
    fn deserialize_status() {
        let got = serde_json::from_slice::<ErrorWrapper>(SAMPLE_PAYLOAD).unwrap();
        let want = ErrorWrapper {
            error: Status {
                code: 400,
                status: Some("INVALID_ARGUMENT".to_string()),
                message:
                    "The provided Secret ID [] does not match the expected format [[a-zA-Z_0-9]+]"
                        .into(),
                details: [].into(),
            },
        };
        assert_eq!(got.error, want.error);
    }

    #[test]
    fn try_from_bytes() -> Result {
        let got = Status::try_from(&bytes::Bytes::from_static(SAMPLE_PAYLOAD))?;
        let want = sample_status();
        assert_eq!(got, want);

        let got = Status::try_from(&bytes::Bytes::from_static(b"\"error\": 1234"));
        assert!(got.is_err());
        let err = got.err().unwrap();
        assert_eq!(err.kind(), crate::error::ErrorKind::Serde);

        let got = Status::try_from(&bytes::Bytes::from_static(b"\"missing-error\": 1234"));
        assert!(got.is_err());
        let err = got.err().unwrap();
        assert_eq!(err.kind(), crate::error::ErrorKind::Serde);
        Ok(())
    }

    #[test]
    fn code_to_string() {
        let got = String::from(Code::AlreadyExists);
        let want = "ALREADY_EXISTS";
        assert_eq!(got, want);
    }

    #[test_case("OK")]
    #[test_case("CANCELED")]
    #[test_case("UNKNOWN")]
    #[test_case("INVALID_ARGUMENT")]
    #[test_case("DEADLINE_EXCEEDED")]
    #[test_case("NOT_FOUND")]
    #[test_case("ALREADY_EXISTS")]
    #[test_case("PERMISSION_DENIED")]
    #[test_case("RESOURCE_EXHAUSTED")]
    #[test_case("FAILED_PRECONDITION")]
    #[test_case("ABORTED")]
    #[test_case("OUT_OF_RANGE")]
    #[test_case("UNIMPLEMENTED")]
    #[test_case("INTERNAL")]
    #[test_case("UNAVAILABLE")]
    #[test_case("DATA_LOSS")]
    #[test_case("UNAUTHENTICATED")]
    fn code_roundtrip(input: &str) -> Result {
        let code = Code::try_from(input)?;
        let output = String::from(code);
        assert_eq!(output.as_str(), input.to_string());
        Ok(())
    }

    #[test_case("OK")]
    #[test_case("CANCELED")]
    #[test_case("UNKNOWN")]
    #[test_case("INVALID_ARGUMENT")]
    #[test_case("DEADLINE_EXCEEDED")]
    #[test_case("NOT_FOUND")]
    #[test_case("ALREADY_EXISTS")]
    #[test_case("PERMISSION_DENIED")]
    #[test_case("RESOURCE_EXHAUSTED")]
    #[test_case("FAILED_PRECONDITION")]
    #[test_case("ABORTED")]
    #[test_case("OUT_OF_RANGE")]
    #[test_case("UNIMPLEMENTED")]
    #[test_case("INTERNAL")]
    #[test_case("UNAVAILABLE")]
    #[test_case("DATA_LOSS")]
    #[test_case("UNAUTHENTICATED")]
    fn code_serialize_roundtrip(input: &str) -> Result {
        let want = Code::try_from(input).unwrap();
        let serialized = serde_json::to_value(&want)?;
        let got = serde_json::from_value::<Code>(serialized)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn code_try_from_string_error() {
        let err = Code::try_from("INVALID-NOT-A-CODE");
        match err {
            Err(s) => assert!(
                s.contains("INVALID-NOT-A-CODE"),
                "expected invalid string in error {s}"
            ),
            Ok(v) => assert!(false, "expected error in try_from, got {v:?}"),
        };
    }

    #[test]
    fn code_deserialize_invalid_type() {
        let input = json!({"k": "v"});
        let err = serde_json::from_value::<Code>(input);
        assert!(err.is_err(), "expected an error, got {err:?}");
    }

    #[test]
    fn code_deserialize_unknown() -> Result {
        let input = json!(-17);
        let code = serde_json::from_value::<Code>(input)?;
        assert_eq!(code, Code::Unknown);
        Ok(())
    }
}
