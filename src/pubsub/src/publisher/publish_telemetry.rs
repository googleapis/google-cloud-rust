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
    fn test_format_telemetry_header_with_start_time() {
        let start_time = wkt::Timestamp::clamp(1_700_000_000, 500_000_000);
        let header_val = format_pubsub_client_telemetry_header(2, Some(start_time))
            .expect("header value should be generated");

        let decoded_bytes = BASE64_STANDARD
            .decode(header_val.as_bytes())
            .expect("should be valid base64");

        let telemetry = PubsubClientTelemetry::decode(&decoded_bytes[..])
            .expect("should decode into PubsubClientTelemetry");

        match telemetry.operation {
            Some(Operation::PublishOperation(op)) => {
                assert_eq!(op.hedged_attempt_count, 2);
                let ts = op.publish_start_time.expect("timestamp should be present");
                assert_eq!(ts.seconds, 1_700_000_000);
                assert_eq!(ts.nanos, 500_000_000);
            }
            _ => panic!("unexpected operation in telemetry proto"),
        }
    }

    #[test]
    fn test_format_telemetry_header_initial_attempt_without_start_time() {
        let header_val = format_pubsub_client_telemetry_header(0, None)
            .expect("header value should be generated for initial attempt");

        let decoded_bytes = BASE64_STANDARD
            .decode(header_val.as_bytes())
            .expect("should be valid base64");

        let telemetry = PubsubClientTelemetry::decode(&decoded_bytes[..])
            .expect("should decode into PubsubClientTelemetry");

        match telemetry.operation {
            Some(Operation::PublishOperation(op)) => {
                assert_eq!(op.hedged_attempt_count, 0);
                assert!(op.publish_start_time.is_none());
            }
            _ => panic!("unexpected operation in telemetry proto"),
        }
    }

    #[test]
    fn test_publish_request_builder_attaches_header() {
        let mut builder = PublishRequestBuilder::new(std::sync::Arc::new(MockPublisher::new()));
        builder = builder.set_pubsub_client_telemetry_header(1, None);

        let headers = builder
            .request_options()
            .get_extension::<http::HeaderMap>()
            .expect("custom headers extension should be present");
        let header_val = headers
            .get(&PUBSUB_CLIENT_TELEMETRY_HEADER)
            .expect("telemetry header should be present");

        assert!(!header_val.is_empty());
    }
}
