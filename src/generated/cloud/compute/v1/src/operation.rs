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

use crate::model::{
    ErrorInfo, Help, HelpLink, LocalizedMessage, Operation, QuotaExceededInfo, operation::Error,
};
use gax::error::rpc::{Code, Status, StatusDetails};

impl lro::internal::DiscoveryOperation for Operation {
    fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
    fn done(&self) -> bool {
        self.status == Some(crate::model::operation::Status::Done)
    }
    fn status(&self) -> Option<Status> {
        let error = self.error.as_ref()?;
        Some(error.into())
    }
}

#[doc(hidden)]
impl From<&Error> for Status {
    fn from(value: &Error) -> Self {
        let code = value
            .errors
            .iter()
            .filter_map(|e| e.code.as_ref())
            .filter_map(|c| Code::try_from(c.as_str()).ok())
            .take(1)
            .next();
        let message = value
            .errors
            .iter()
            .flat_map(|e| e.message.as_ref())
            .take(1)
            .next();
        let details: Vec<StatusDetails> = value
            .errors
            .iter()
            .flat_map(|e| e.error_details.iter())
            .flat_map(|d| {
                [
                    d.error_info.clone().map(StatusDetails::from),
                    d.help.clone().map(StatusDetails::from),
                    d.localized_message.clone().map(StatusDetails::from),
                    d.quota_info.clone().map(StatusDetails::from),
                ]
                .into_iter()
                .flatten()
            })
            .collect();

        let status = Status::default().set_details(details);
        let status = code.into_iter().fold(status, |s, c| s.set_code(c));
        message.into_iter().fold(status, |s, m| s.set_message(m))
    }
}

#[doc(hidden)]
impl From<ErrorInfo> for StatusDetails {
    fn from(value: ErrorInfo) -> Self {
        Self::ErrorInfo(value.into())
    }
}

#[doc(hidden)]
impl From<ErrorInfo> for rpc::model::ErrorInfo {
    fn from(value: ErrorInfo) -> Self {
        let result = Self::new().set_metadata(value.metadatas);
        let result = value
            .domain
            .into_iter()
            .fold(result, |r, v| r.set_domain(v));
        value
            .reason
            .into_iter()
            .fold(result, |r, v| r.set_reason(v))
    }
}

#[doc(hidden)]
impl From<Help> for StatusDetails {
    fn from(value: crate::model::Help) -> Self {
        Self::Help(value.into())
    }
}

#[doc(hidden)]
impl From<Help> for rpc::model::Help {
    fn from(value: Help) -> Self {
        Self::new().set_links(value.links)
    }
}

#[doc(hidden)]
impl From<HelpLink> for rpc::model::help::Link {
    fn from(value: HelpLink) -> Self {
        let result = Self::new();
        let result = value
            .description
            .into_iter()
            .fold(result, |r, v| r.set_description(v));
        value.url.into_iter().fold(result, |r, v| r.set_url(v))
    }
}

#[doc(hidden)]
impl From<LocalizedMessage> for StatusDetails {
    fn from(value: LocalizedMessage) -> Self {
        Self::LocalizedMessage(value.into())
    }
}

#[doc(hidden)]
impl From<LocalizedMessage> for rpc::model::LocalizedMessage {
    fn from(value: LocalizedMessage) -> Self {
        let result = Self::new();
        let result = value
            .locale
            .into_iter()
            .fold(result, |r, v| r.set_locale(v));
        value
            .message
            .into_iter()
            .fold(result, |r, v| r.set_message(v))
    }
}

#[doc(hidden)]
impl From<QuotaExceededInfo> for StatusDetails {
    fn from(value: QuotaExceededInfo) -> Self {
        StatusDetails::QuotaFailure(value.into())
    }
}

#[doc(hidden)]
impl From<QuotaExceededInfo> for rpc::model::QuotaFailure {
    fn from(value: QuotaExceededInfo) -> Self {
        let r = rpc::model::quota_failure::Violation::new()
            .set_api_service("compute.googleapis.com")
            .set_quota_dimensions(value.dimensions);
        let r = value
            .future_limit
            .into_iter()
            .fold(r, |r, v| r.set_future_quota_value(v as i64));
        let r = value
            .limit
            .into_iter()
            .fold(r, |r, v| r.set_quota_value(v as i64));
        // The documentation for "quota_id" says that it is sometimes called "limit_name".
        let r = value
            .limit_name
            .into_iter()
            .fold(r, |r, v| r.set_quota_id(v));
        let r = value
            .metric_name
            .into_iter()
            .fold(r, |r, v| r.set_quota_metric(v));
        // There is no way to represent representation for `value.rollout_status`
        // let r = r.rollout_status.....

        Self::new().set_violations([r])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::operation::error::{Errors, errors::ErrorDetails};

    #[test]
    fn discovery_operation() {
        use lro::internal::DiscoveryOperation as _;

        let operation = Operation::new();
        assert!(operation.name().is_none(), "{operation:?}");
        assert!(!operation.done(), "{operation:?}");
        assert!(operation.status().is_none(), "{operation:?}");

        let operation = Operation::new().set_name("abc-123");
        assert_eq!(
            operation.name().map(String::as_str),
            Some("abc-123"),
            "{operation:?}"
        );
        assert!(!operation.done(), "{operation:?}");
        assert!(operation.status().is_none(), "{operation:?}");

        let operation = Operation::new().set_status(crate::model::operation::Status::Done);
        assert!(operation.name().is_none(), "{operation:?}");
        assert!(operation.done(), "{operation:?}");
        assert!(operation.status().is_none(), "{operation:?}");

        let operation = Operation::new().set_error(
            Error::new().set_errors([Errors::new()
                .set_code("UNAVAILABLE")
                .set_message("try-again")]),
        );
        assert!(operation.name().is_none(), "{operation:?}");
        assert!(!operation.done(), "{operation:?}");
        assert!(
            matches!(operation.status(), Some(s) if s.code == Code::Unavailable && s.message == "try-again"),
            "{operation:?}"
        );
    }

    #[test]
    fn status_from_error() {
        let input = Error::new();
        let got = Status::from(&input);
        assert_eq!(got, Status::default());

        let input = Error::new().set_errors([
            Errors::new().set_code("INTERNAL"),
            Errors::new().set_code("UNAVAILABLE"),
        ]);
        let got = Status::from(&input);
        assert_eq!(got, Status::default().set_code(Code::Internal));

        let input = Error::new().set_errors([
            Errors::new().set_message("message0"),
            Errors::new().set_message("message1"),
        ]);
        let got = Status::from(&input);
        assert_eq!(got, Status::default().set_message("message0"));

        let input = Error::new().set_errors([
            Errors::new().set_error_details([
                ErrorDetails::new().set_error_info(ErrorInfo::new().set_domain("e0"))
            ]),
            Errors::new().set_error_details([ErrorDetails::new()
                .set_error_info(ErrorInfo::new().set_domain("e1"))
                .set_help(Help::new().set_links([HelpLink::new().set_description("helplink1")]))
                .set_localized_message(LocalizedMessage::new().set_message("localized1"))
                .set_quota_info(QuotaExceededInfo::new().set_metric_name("quota1"))]),
            Errors::new().set_error_details([ErrorDetails::new()
                .set_error_info(ErrorInfo::new().set_domain("e2"))
                .set_help(Help::new().set_links([HelpLink::new().set_description("helplink2")]))
                .set_localized_message(LocalizedMessage::new().set_message("localized2"))
                .set_quota_info(QuotaExceededInfo::new().set_metric_name("quota2"))]),
        ]);
        let got = Status::from(&input);
        assert!(
            matches!(got.details.first(), Some(StatusDetails::ErrorInfo(e)) if e.domain == "e0"),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(1), Some(StatusDetails::ErrorInfo(e)) if e.domain == "e1"),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(2), Some(StatusDetails::Help(_))),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(3), Some(StatusDetails::LocalizedMessage(m)) if m.message == "localized1"),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(4), Some(StatusDetails::QuotaFailure(_))),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(5), Some(StatusDetails::ErrorInfo(e)) if e.domain == "e2"),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(6), Some(StatusDetails::Help(_))),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(7), Some(StatusDetails::LocalizedMessage(m)) if m.message == "localized2"),
            "{got:?}"
        );
        assert!(
            matches!(got.details.get(8), Some(StatusDetails::QuotaFailure(_))),
            "{got:?}"
        );
    }

    #[test]
    fn status_details_from_error_info() {
        use rpc::model::ErrorInfo as RpcErrorInfo;

        let input = ErrorInfo::new()
            .set_domain("domain")
            .set_reason("reason")
            .set_metadatas([("k0", "v0"), ("k1", "v1")]);
        let got = StatusDetails::from(input.clone());
        let want = RpcErrorInfo::from(input);
        assert!(
            matches!(got, StatusDetails::ErrorInfo(ref e) if e == &want),
            "{got:?}"
        );
    }

    #[test]
    fn error_info_from() {
        use rpc::model::ErrorInfo as RpcErrorInfo;

        let input = ErrorInfo::new();
        let got = RpcErrorInfo::from(input);
        assert_eq!(got, RpcErrorInfo::new());

        let input = ErrorInfo::new()
            .set_domain("domain")
            .set_reason("reason")
            .set_metadatas([("k0", "v0"), ("k1", "v1")]);
        let got = RpcErrorInfo::from(input);
        let want = RpcErrorInfo::new()
            .set_domain("domain")
            .set_reason("reason")
            .set_metadata([("k0", "v0"), ("k1", "v1")]);
        assert_eq!(got, want);
    }

    #[test]
    fn status_details_from_help() {
        use rpc::model::Help as RpcHelp;

        let input = Help::new().set_links([HelpLink::new(), HelpLink::new().set_url("url")]);
        let got = StatusDetails::from(input.clone());
        let want = RpcHelp::from(input);
        assert!(
            matches!(got, StatusDetails::Help(ref h) if h == &want),
            "{got:?}"
        );
    }

    #[test]
    fn from_help() {
        use rpc::model::Help as RpcHelp;
        use rpc::model::help::Link as RpcLink;

        let input = Help::new();
        let got = RpcHelp::from(input);
        assert_eq!(got, RpcHelp::new());

        let input = Help::new().set_links([HelpLink::new(), HelpLink::new().set_url("url")]);
        let got = RpcHelp::from(input);
        assert_eq!(
            got,
            RpcHelp::new().set_links([RpcLink::new(), RpcLink::new().set_url("url")])
        );
    }

    #[test]
    fn from_link() {
        use rpc::model::help::Link as RpcLink;

        let input = HelpLink::new();
        let got = RpcLink::from(input);
        assert_eq!(got, RpcLink::new());

        let input = HelpLink::new().set_description("description");
        let got = RpcLink::from(input);
        assert_eq!(got, RpcLink::new().set_description("description"));

        let input = HelpLink::new().set_url("url");
        let got = RpcLink::from(input);
        assert_eq!(got, RpcLink::new().set_url("url"));

        let input = HelpLink::new()
            .set_description("description")
            .set_url("url");
        let got = RpcLink::from(input);
        assert_eq!(
            got,
            RpcLink::new().set_description("description").set_url("url")
        );
    }

    #[test]
    fn status_details_from_localized_message() {
        use rpc::model::LocalizedMessage as RpcLocalizedMessage;

        let input = LocalizedMessage::new()
            .set_locale("NYC")
            .set_message("fuggedaboudit");
        let got = StatusDetails::from(input.clone());
        let want = RpcLocalizedMessage::from(input);
        assert!(
            matches!(got, StatusDetails::LocalizedMessage(ref m) if m == &want),
            "{got:?}"
        );
    }

    #[test]
    fn from_localized_message() {
        use rpc::model::LocalizedMessage as RpcLocalizedMessage;

        let input = LocalizedMessage::new();
        let got = RpcLocalizedMessage::from(input);
        assert_eq!(got, RpcLocalizedMessage::new());

        let input = LocalizedMessage::new()
            .set_locale("NYC")
            .set_message("fuggedaboudit");
        let got = RpcLocalizedMessage::from(input);
        let want = RpcLocalizedMessage::new()
            .set_locale("NYC")
            .set_message("fuggedaboudit");
        assert_eq!(got, want);
    }

    #[test]
    fn status_details_from_quota_exceeded_info() {
        use rpc::model::QuotaFailure;

        let input = QuotaExceededInfo::new()
            .set_dimensions([("k1", "v1"), ("k2", "v2")])
            .set_future_limit(123.0)
            .set_limit(234.0)
            .set_limit_name("limit_name")
            .set_metric_name("metric_name");
        let got = StatusDetails::from(input.clone());
        let want = QuotaFailure::from(input);
        assert!(
            matches!(got, StatusDetails::QuotaFailure(ref e) if e == &want),
            "{got:?}"
        );
    }

    #[test]
    fn from_quota_exceeded_info() {
        use rpc::model::{QuotaFailure, quota_failure::Violation};

        let input = QuotaExceededInfo::new();
        let got = QuotaFailure::from(input);
        let want = Violation::new().set_api_service("compute.googleapis.com");
        assert_eq!(got, QuotaFailure::new().set_violations([want]));

        let input = QuotaExceededInfo::new()
            .set_dimensions([("k1", "v1"), ("k2", "v2")])
            .set_future_limit(123.0)
            .set_limit(234.0)
            .set_limit_name("limit_name")
            .set_metric_name("metric_name");
        let got = QuotaFailure::from(input);
        let want = Violation::new()
            .set_api_service("compute.googleapis.com")
            .set_quota_dimensions([("k1", "v1"), ("k2", "v2")])
            .set_future_quota_value(123)
            .set_quota_value(234)
            .set_quota_id("limit_name")
            .set_quota_metric("metric_name");
        assert_eq!(got, QuotaFailure::new().set_violations([want]));
    }
}
