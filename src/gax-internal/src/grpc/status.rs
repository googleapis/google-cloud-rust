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

use crate::google;
use crate::prost::{ConvertError, FromProto, ToProto};

// TODO(#1699) - use to convert a `tonic::Status`
#[allow(dead_code)]
fn status_from_proto(s: google::rpc::Status) -> Result<rpc::model::Status, ConvertError> {
    Ok(rpc::model::Status::new()
        .set_code(s.code)
        .set_message(s.message)
        .set_details(s.details.into_iter().filter_map(any_from_prost)))
}

pub fn any_to_prost(value: wkt::Any) -> Option<prost_types::Any> {
    let mapped = value.type_url().map(|url| match url {
        "type.googleapis.com/google.rpc.BadRequest" => value
            .to_msg::<rpc::model::BadRequest>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.DebugInfo" => value
            .to_msg::<rpc::model::DebugInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ErrorInfo" => value
            .to_msg::<rpc::model::ErrorInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.Help" => value
            .to_msg::<rpc::model::Help>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.LocalizedMessage" => value
            .to_msg::<rpc::model::LocalizedMessage>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.PreconditionFailure" => value
            .to_msg::<rpc::model::PreconditionFailure>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.QuotaFailure" => value
            .to_msg::<rpc::model::QuotaFailure>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RequestInfo" => value
            .to_msg::<rpc::model::RequestInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ResourceInfo" => value
            .to_msg::<rpc::model::ResourceInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RetryInfo" => value
            .to_msg::<rpc::model::RetryInfo>()
            .ok()
            .and_then(|v| v.to_proto().ok())
            .map(|v| prost_types::Any::from_msg(&v)),
        _ => None,
    });
    mapped.flatten().transpose().ok().flatten()
}

pub fn any_from_prost(value: prost_types::Any) -> Option<wkt::Any> {
    let mapped = match value.type_url.as_str() {
        "type.googleapis.com/google.rpc.BadRequest" => value
            .to_msg::<google::rpc::BadRequest>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.DebugInfo" => value
            .to_msg::<google::rpc::DebugInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ErrorInfo" => value
            .to_msg::<google::rpc::ErrorInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.Help" => value
            .to_msg::<google::rpc::Help>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.LocalizedMessage" => value
            .to_msg::<google::rpc::LocalizedMessage>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.PreconditionFailure" => value
            .to_msg::<google::rpc::PreconditionFailure>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.QuotaFailure" => value
            .to_msg::<google::rpc::QuotaFailure>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RequestInfo" => value
            .to_msg::<google::rpc::RequestInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.ResourceInfo" => value
            .to_msg::<google::rpc::ResourceInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        "type.googleapis.com/google.rpc.RetryInfo" => value
            .to_msg::<google::rpc::RetryInfo>()
            .ok()
            .and_then(|v| v.cnv().ok())
            .map(|v| wkt::Any::from_msg(&v)),
        _ => None,
    };
    mapped.transpose().ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_proto() -> anyhow::Result<()> {
        let got: Vec<wkt::Any> = prost_details()
            .into_iter()
            .filter_map(any_from_prost)
            .collect();
        assert_eq!(got, wkt_details());
        Ok(())
    }

    #[test]
    fn to_proto() -> anyhow::Result<()> {
        let got: Vec<prost_types::Any> =
            wkt_details().into_iter().filter_map(any_to_prost).collect();
        assert_eq!(got, prost_details());
        Ok(())
    }

    fn prost_details() -> Vec<prost_types::Any> {
        use google::rpc::*;
        use prost_types::Any;
        // We do not want our CI to break if/when the protos grow.
        #[allow(clippy::needless_update)]
        let from_msg = vec![
            Any::from_msg(&BadRequest {
                field_violations: vec![bad_request::FieldViolation {
                    field: "field".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
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
                ..Default::default()
            }),
            Any::from_msg(&PreconditionFailure {
                violations: vec![precondition_failure::Violation {
                    r#type: "type".into(),
                    subject: "subject".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&QuotaFailure {
                violations: vec![quota_failure::Violation {
                    subject: "subject".into(),
                    description: "desc".into(),
                    ..Default::default()
                }],
            }),
            Any::from_msg(&RequestInfo {
                request_id: "id".into(),
                serving_data: "data".into(),
                ..Default::default()
            }),
            Any::from_msg(&ResourceInfo {
                resource_type: "type".into(),
                resource_name: "name".into(),
                owner: "owner".into(),
                description: "desc".into(),
                ..Default::default()
            }),
            Any::from_msg(&RetryInfo {
                retry_delay: prost_types::Duration {
                    seconds: 1,
                    nanos: 0,
                    ..Default::default()
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
            Any::from_msg(&BadRequest::default().set_field_violations(vec![
                rpc::model::bad_request::FieldViolation::default()
                    .set_field("field")
                    .set_description("desc"),
            ])),
            Any::from_msg(
                &DebugInfo::default()
                    .set_stack_entries(vec!["stack".to_string()])
                    .set_detail("detail"),
            ),
            Any::from_msg(
                &ErrorInfo::default()
                    .set_reason("reason")
                    .set_domain("domain"),
            ),
            Any::from_msg(&Help::default().set_links(vec![
                    rpc::model::help::Link::default()
                        .set_description("desc")
                        .set_url("url"),
                ])),
            Any::from_msg(
                &LocalizedMessage::default()
                    .set_locale("locale")
                    .set_message("message"),
            ),
            Any::from_msg(&PreconditionFailure::default().set_violations(vec![
                rpc::model::precondition_failure::Violation::default()
                    .set_type("type")
                    .set_subject("subject")
                    .set_description("desc"),
            ])),
            Any::from_msg(&QuotaFailure::default().set_violations(vec![
                rpc::model::quota_failure::Violation::default()
                    .set_subject("subject")
                    .set_description("desc"),
            ])),
            Any::from_msg(
                &RequestInfo::default()
                    .set_request_id("id")
                    .set_serving_data("data"),
            ),
            Any::from_msg(
                &ResourceInfo::default()
                    .set_resource_type("type")
                    .set_resource_name("name")
                    .set_owner("owner")
                    .set_description("desc"),
            ),
            Any::from_msg(&RetryInfo::default().set_retry_delay(wkt::Duration::clamp(1, 0))),
        ];
        try_from.into_iter().map(|x| x.unwrap()).collect()
    }
}
