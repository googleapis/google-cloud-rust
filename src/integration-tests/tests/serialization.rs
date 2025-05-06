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

//! Verify sidekick generate types with the expected serialization behavior.

#[cfg(test)]
mod serialization {
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn protobuf() -> Result<()> {
        let secret =
            sm::model::Secret::default().set_name("projects/p-test-only/secrets/s-test-only");
        let got = serde_json::to_value(&secret)?;
        let want = serde_json::json!({
            "name": "projects/p-test-only/secrets/s-test-only"
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn respects_protobuf_presence() -> Result<()> {
        let message = sm::model::SecretPayload::default().set_data_crc32c(0);
        let got = serde_json::to_value(&message)?;
        let want = serde_json::json!({
            "dataCrc32c": "0"
        });
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn serde_with_float() -> Result<()> {
        let generation_config =
            aiplatform::model::GenerationConfig::default().set_temperature(9876.5);
        let got = serde_json::to_value(&generation_config)?;
        let want = serde_json::json!({
            "temperature": 9876.5_f32
        });
        assert_eq!(got, want);
        let rt = serde_json::from_value(got)?;
        assert_eq!(generation_config, rt);
        let generation_config = generation_config.set_temperature(f32::INFINITY);
        let got = serde_json::to_value(&generation_config)?;
        let want = serde_json::json!({
            "temperature": "Infinity"
        });
        assert_eq!(got, want);
        let rt = serde_json::from_value(got)?;
        assert_eq!(generation_config, rt);
        Ok(())
    }

    #[test]
    fn serde_with_double() -> Result<()> {
        let logging_config =
            aiplatform::model::PredictRequestResponseLoggingConfig::default().set_sampling_rate(53);
        let got = serde_json::to_value(&logging_config)?;
        let want = serde_json::json!({
            "samplingRate": 53_f64
        });
        assert_eq!(got, want);
        let rt = serde_json::from_value(got)?;
        assert_eq!(logging_config, rt);
        let logging_config = logging_config.set_sampling_rate(f64::INFINITY);
        let got = serde_json::to_value(&logging_config)?;
        let want = serde_json::json!({
            "samplingRate": "Infinity"
        });
        assert_eq!(got, want);
        let rt = serde_json::from_value(got)?;
        assert_eq!(logging_config, rt);
        Ok(())
    }

    #[test]
    fn serde_with_i64() -> Result<()> {
        let completion_stats =
            aiplatform::model::CompletionStats::default().set_successful_count(5);
        let got = serde_json::to_value(&completion_stats)?;
        let want = serde_json::json!({
            "successfulCount": "5"
        });
        assert_eq!(got, want);
        let rt = serde_json::from_value(got)?;
        assert_eq!(completion_stats, rt);
        Ok(())
    }

    #[test]
    fn multiple_serde_attributes() -> Result<()> {
        let input = Test {
            f_bytes: bytes::Bytes::from("the quick brown fox jumps over the lazy dog"),
            ..Default::default()
        };
        let got = serde_json::to_value(&input)?;
        let want = serde_json::json!({
            "fancyName": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="
        });
        assert_eq!(got, want);

        let input = Test {
            f_string: "the quick brown fox jumps over the lazy dog".to_string(),
            ..Default::default()
        };
        let got = serde_json::to_value(&input)?;
        let want = serde_json::json!({
            "fString": "the quick brown fox jumps over the lazy dog"
        });
        assert_eq!(got, want);

        let input = Test::default();
        let got = serde_json::to_value(&input)?;
        let want = serde_json::json!({});
        assert_eq!(got, want);

        Ok(())
    }

    #[serde_with::serde_as]
    #[derive(Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Test {
        #[serde(rename = "fancyName")]
        #[serde(skip_serializing_if = "bytes::Bytes::is_empty")]
        #[serde_as(as = "serde_with::base64::Base64")]
        f_bytes: bytes::Bytes,

        #[serde(rename = "fString")]
        #[serde(skip_serializing_if = "String::is_empty")]
        f_string: String,
    }
}
