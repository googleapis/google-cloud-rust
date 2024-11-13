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

mod error_details;
pub use error_details::*;

/// The [Status] type defines a logical error model that is suitable for
/// different programming environments, including REST APIs and RPC APIs. It is
/// used by [gRPC](https://github.com/grpc). Each [Status] message contains
/// three pieces of data: error code, error message, and error details.
///
/// You can find out more about this error model and how to work with it in the
/// [API Design Guide](https://cloud.google.com/apis/design/errors).
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub code: i32,
    pub message: String,
    pub details: Vec<StatusDetails>,
}

/// The type of details associated with [Status].
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", untagged)]
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
    Other(types::Any),
}

impl Default for StatusDetails {
    fn default() -> Self {
        Self::Other(types::Any::default())
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
            code: 123,
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
                    retry_delay: Some(types::Duration::from_seconds(1)),
                }),
            ],
        };
        let got = serde_json::to_value(&status).unwrap();
        let want = json!({
            "code": 123,
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
            "code": 123,
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
            code: 123,
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
                    retry_delay: Some(types::Duration::from_seconds(1)),
                }),
            ],
        };
        assert_eq!(got, want);
    }
}
