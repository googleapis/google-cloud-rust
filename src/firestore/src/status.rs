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

use super::google;

impl gaxi::prost::Convert<google::rpc::Status> for rpc::model::Status {
    fn cnv(self) -> google::rpc::Status {
        google::rpc::Status {
            code: self.code.cnv(),
            message: self.message.cnv(),
            details: self.details.into_iter().filter_map(any_to_prost).collect(),
        }
    }
}

impl gaxi::prost::Convert<rpc::model::Status> for google::rpc::Status {
    fn cnv(self) -> rpc::model::Status {
        rpc::model::Status::new()
            .set_code(self.code)
            .set_message(self.message)
            .set_details(self.details.into_iter().filter_map(any_from_prost))
    }
}

fn any_to_prost(value: wkt::Any) -> Option<prost_types::Any> {
    use gaxi::prost::Convert;
    let mapped = value.type_url().map(|url| match url {
        "type.googleapis.com/google.rpc.BadRequest" => value
            .try_into_message::<rpc::model::BadRequest>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.DebugInfo" => value
            .try_into_message::<rpc::model::DebugInfo>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.ErrorInfo" => value
            .try_into_message::<rpc::model::ErrorInfo>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.Help" => value
            .try_into_message::<rpc::model::Help>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.LocalizedMessage" => value
            .try_into_message::<rpc::model::LocalizedMessage>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.PreconditionFailure" => value
            .try_into_message::<rpc::model::PreconditionFailure>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.QuotaFailure" => value
            .try_into_message::<rpc::model::QuotaFailure>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.RequestInfo" => value
            .try_into_message::<rpc::model::RequestInfo>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.ResourceInfo" => value
            .try_into_message::<rpc::model::ResourceInfo>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        "type.googleapis.com/google.rpc.RetryInfo" => value
            .try_into_message::<rpc::model::RetryInfo>()
            .ok()
            .map(|v| prost_types::Any::from_msg(&v.cnv())),
        _ => None,
    });
    mapped.flatten().transpose().ok().flatten()
}

fn any_from_prost(value: prost_types::Any) -> Option<wkt::Any> {
    use gaxi::prost::Convert;
    let mapped = match value.type_url.as_str() {
        "type.googleapis.com/google.rpc.BadRequest" => value
            .to_msg::<google::rpc::BadRequest>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.DebugInfo" => value
            .to_msg::<google::rpc::DebugInfo>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.ErrorInfo" => value
            .to_msg::<google::rpc::ErrorInfo>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.Help" => value
            .to_msg::<google::rpc::Help>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.LocalizedMessage" => value
            .to_msg::<google::rpc::LocalizedMessage>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.PreconditionFailure" => value
            .to_msg::<google::rpc::PreconditionFailure>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.QuotaFailure" => value
            .to_msg::<google::rpc::QuotaFailure>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.RequestInfo" => value
            .to_msg::<google::rpc::RequestInfo>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.ResourceInfo" => value
            .to_msg::<google::rpc::ResourceInfo>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        "type.googleapis.com/google.rpc.RetryInfo" => value
            .to_msg::<google::rpc::RetryInfo>()
            .ok()
            .map(|v| wkt::Any::try_from(&v.cnv())),
        _ => None,
    };
    mapped.transpose().ok().flatten()
}

#[cfg(test)]
mod test {
    use super::*;
    use gaxi::prost::Convert;

    #[test]
    fn from_prost() {
        let input = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            details: prost_details(),
        };
        let got: rpc::model::Status = input.cnv();
        let want = rpc::model::Status::new()
            .set_code(12)
            .set_message("test-message")
            .set_details(wkt_details());
        assert_eq!(got, want);
    }

    #[test]
    fn from_rpc_model() {
        let input = rpc::model::Status::new()
            .set_code(12)
            .set_message("test-message")
            .set_details(wkt_details());
        let got: google::rpc::Status = input.cnv();
        let want = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            details: prost_details(),
        };
        assert_eq!(got, want);
    }

    fn prost_details() -> Vec<prost_types::Any> {
        use google::rpc::*;
        use prost_types::Any;
        let from_msg = vec![
            Any::from_msg(&BadRequest {
                field_violations: vec![bad_request::FieldViolation {
                    field: "field".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Any::from_msg(&DebugInfo {
                stack_entries: ["stack"].map(str::to_string).to_vec(),
                detail: "detail".into(),
                ..Default::default()
            }),
            Any::from_msg(&ErrorInfo {
                reason: "reason".into(),
                domain: "domain".into(),
                ..Default::default()
            }),
            Any::from_msg(&Help {
                links: vec![help::Link {
                    description: "desc".into(),
                    url: "url".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&LocalizedMessage {
                locale: "locale".into(),
                message: "message".into(),
            }),
            Any::from_msg(&PreconditionFailure {
                violations: vec![precondition_failure::Violation {
                    r#type: "type".into(),
                    subject: "subject".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Any::from_msg(&QuotaFailure {
                violations: vec![quota_failure::Violation {
                    subject: "subject".into(),
                    description: "desc".into(),
                }],
            }),
            Any::from_msg(&RequestInfo {
                request_id: "id".into(),
                serving_data: "data".into(),
            }),
            Any::from_msg(&ResourceInfo {
                resource_type: "type".into(),
                resource_name: "name".into(),
                owner: "owner".into(),
                description: "desc".into(),
            }),
            Any::from_msg(&RetryInfo {
                retry_delay: prost_types::Duration {
                    seconds: 1,
                    nanos: 0,
                }
                .into(),
            }),
        ];
        from_msg.into_iter().map(|r| r.unwrap()).collect()
    }

    fn wkt_details() -> Vec<wkt::Any> {
        use rpc::model::*;
        use wkt::Any;
        let try_from = vec![
            Any::try_from(&BadRequest::default().set_field_violations(vec![
                rpc::model::bad_request::FieldViolation::default()
                    .set_field("field")
                    .set_description("desc"),
            ])),
            Any::try_from(
                &DebugInfo::default()
                    .set_stack_entries(vec!["stack".to_string()])
                    .set_detail("detail"),
            ),
            Any::try_from(
                &ErrorInfo::default()
                    .set_reason("reason")
                    .set_domain("domain"),
            ),
            Any::try_from(&Help::default().set_links(vec![
                    rpc::model::help::Link::default()
                        .set_description("desc")
                        .set_url("url"),
                ])),
            Any::try_from(
                &LocalizedMessage::default()
                    .set_locale("locale")
                    .set_message("message"),
            ),
            Any::try_from(&PreconditionFailure::default().set_violations(vec![
                rpc::model::precondition_failure::Violation::default()
                    .set_type("type")
                    .set_subject("subject")
                    .set_description("desc"),
            ])),
            Any::try_from(&QuotaFailure::default().set_violations(vec![
                rpc::model::quota_failure::Violation::default()
                    .set_subject("subject")
                    .set_description("desc"),
            ])),
            Any::try_from(
                &RequestInfo::default()
                    .set_request_id("id")
                    .set_serving_data("data"),
            ),
            Any::try_from(
                &ResourceInfo::default()
                    .set_resource_type("type")
                    .set_resource_name("name")
                    .set_owner("owner")
                    .set_description("desc"),
            ),
            Any::try_from(&RetryInfo::default().set_retry_delay(wkt::Duration::clamp(1, 0))),
        ];
        try_from.into_iter().map(|x| x.unwrap()).collect()
    }
}
