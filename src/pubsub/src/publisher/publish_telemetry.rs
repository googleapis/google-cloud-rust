// Copyright 2026 Google LLC
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

use base64::{Engine, prelude::BASE64_STANDARD};
use gaxi::prost::ToProto;
use google_cloud_gax::options::RequestOptionsBuilder;
use http::header::{HeaderName, HeaderValue};
use prost::Message as _;

use crate::generated::gapic_dataplane::builder::publisher::Publish as PublishRequestBuilder;
use crate::google::pubsub::v1::PubsubClientTelemetry;
use crate::google::pubsub::v1::pubsub_client_telemetry::{Operation, PublishOperation};

pub(crate) const PUBSUB_CLIENT_TELEMETRY_HEADER: HeaderName =
    HeaderName::from_static("x-goog-pubsub-client-telemetry");

/// Encodes the internal `x-goog-pubsub-client-telemetry` header value for publish attempts.
///
/// This header is used by Cloud Pub/Sub servers to associate hedged duplicates with their original
/// publish operation and track attempt count and original start time.
///
/// * `attempt_count`: `0` for the initial/normal request, `1` for the first hedge, `2` for the second, etc.
/// * `start_time`: Optional original start time of the publish operation.
pub(crate) fn format_pubsub_client_telemetry_header(
    attempt_count: i32,
    start_time: Option<wkt::Timestamp>,
) -> Option<HeaderValue> {
    let p = PubsubClientTelemetry {
        operation: Some(Operation::PublishOperation(PublishOperation {
            hedged_attempt_count: attempt_count,
            publish_start_time: start_time.and_then(|t| t.to_proto().ok()),
        })),
    };

    let encoded_proto = p.encode_to_vec();
    let b64_str = BASE64_STANDARD.encode(&encoded_proto);
    HeaderValue::try_from(b64_str).ok()
}

impl PublishRequestBuilder {
    pub(crate) fn set_pubsub_client_telemetry_header(
        self,
        attempt_count: i32,
        start_time: Option<wkt::Timestamp>,
    ) -> Self {
        if let Some(val) = format_pubsub_client_telemetry_header(attempt_count, start_time) {
            return self.with_custom_header(PUBSUB_CLIENT_TELEMETRY_HEADER, val);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestBuilder as _;
    use google_cloud_gax::options::internal::RequestOptionsExt as _;

    mockall::mock! {
        #[derive(Debug)]
        Publisher {}
        impl crate::generated::gapic_dataplane::stub::Publisher for Publisher {
            async fn publish(
                &self,
                req: crate::model::PublishRequest,
                _options: crate::RequestOptions,
            ) -> crate::Result<crate::Response<crate::model::PublishResponse>>;
        }
    }

    #[test]
    fn test_format_telemetry_header_with_start_time() -> anyhow::Result<()> {
        let start_time = wkt::Timestamp::clamp(1_700_000_000, 500_000_000);
        let header_val = format_pubsub_client_telemetry_header(2, Some(start_time))
            .ok_or_else(|| anyhow::anyhow!("header value should be generated"))?;

        let decoded_bytes = BASE64_STANDARD.decode(header_val.as_bytes())?;
        let telemetry = PubsubClientTelemetry::decode(&decoded_bytes[..])?;

        assert_eq!(
            telemetry,
            PubsubClientTelemetry {
                operation: Some(Operation::PublishOperation(PublishOperation {
                    hedged_attempt_count: 2,
                    publish_start_time: Some(prost_types::Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 500_000_000,
                    }),
                })),
            }
        );
        Ok(())
    }

    #[test]
    fn test_format_telemetry_header_initial_attempt_without_start_time() -> anyhow::Result<()> {
        let header_val = format_pubsub_client_telemetry_header(0, None)
            .ok_or_else(|| anyhow::anyhow!("header value should be generated for initial attempt"))?;

        let decoded_bytes = BASE64_STANDARD.decode(header_val.as_bytes())?;
        let telemetry = PubsubClientTelemetry::decode(&decoded_bytes[..])?;

        assert_eq!(
            telemetry,
            PubsubClientTelemetry {
                operation: Some(Operation::PublishOperation(PublishOperation {
                    hedged_attempt_count: 0,
                    publish_start_time: None,
                })),
            }
        );
        Ok(())
    }

    #[test]
    fn test_format_telemetry_header_wire_format() -> anyhow::Result<()> {
        let initial = format_pubsub_client_telemetry_header(0, None)
            .ok_or_else(|| anyhow::anyhow!("header value should be generated"))?;
        assert_eq!(initial.to_str()?, "CgA=");

        let first_hedge = format_pubsub_client_telemetry_header(1, None)
            .ok_or_else(|| anyhow::anyhow!("header value should be generated"))?;
        assert_eq!(first_hedge.to_str()?, "CgIIAQ==");
        Ok(())
    }

    #[test]
    fn test_publish_request_builder_attaches_header() -> anyhow::Result<()> {
        let mut builder = PublishRequestBuilder::new(std::sync::Arc::new(MockPublisher::new()));
        builder = builder.set_pubsub_client_telemetry_header(1, None);

        let headers = builder
            .request_options()
            .get_extension::<http::HeaderMap>()
            .ok_or_else(|| anyhow::anyhow!("custom headers extension should be present"))?;
        let header_val = headers
            .get(&PUBSUB_CLIENT_TELEMETRY_HEADER)
            .ok_or_else(|| anyhow::anyhow!("telemetry header should be present"))?;

        let expected = format_pubsub_client_telemetry_header(1, None)
            .ok_or_else(|| anyhow::anyhow!("expected header value to generate"))?;
        assert_eq!(header_val, &expected);
        Ok(())
    }
}
