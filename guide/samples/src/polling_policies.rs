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

//! Examples showing how to configure the polling policies.

// ANCHOR: use
use google_cloud_speech_v2 as speech;
// ANCHOR_END: use

// ANCHOR: client-backoff
pub async fn client_backoff(project_id: &str) -> crate::Result<()> {
    // ANCHOR: client-backoff-use
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    // ANCHOR_END: client-backoff-use
    use speech::Poller;
    use speech::model::*;
    use std::time::Duration;

    // ANCHOR: client-backoff-client
    let client = speech::client::Speech::builder()
        .with_polling_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(250))
                .with_maximum_delay(Duration::from_secs(10))
                .build()?,
        )
        .build()
        .await?;
    // ANCHOR_END: client-backoff-client

    // ANCHOR: client-backoff-builder
    let response = client
        .batch_recognize()
        .set_recognizer(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        // ANCHOR_END: client-backoff-builder
        // ANCHOR: client-backoff-prepare
        .set_files([BatchRecognizeFileMetadata::new().set_audio_source(
            batch_recognize_file_metadata::AudioSource::Uri(
                "gs://cloud-samples-data/speech/hello.wav".into(),
            ),
        )])
        .set_recognition_output_config(RecognitionOutputConfig::new().set_output(
            recognition_output_config::Output::InlineResponseConfig(
                InlineOutputConfig::new().into(),
            ),
        ))
        .set_processing_strategy(batch_recognize_request::ProcessingStrategy::DynamicBatching)
        .set_config(
            RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_decoding_config(recognition_config::DecodingConfig::AutoDecodingConfig(
                    AutoDetectDecodingConfig::new().into(),
                )),
        )
        // ANCHOR_END: client-backoff-prepare
        // ANCHOR: client-backoff-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: client-backoff-print

    Ok(())
}
// ANCHOR_END: client-backoff

// ANCHOR: rpc-backoff
pub async fn rpc_backoff(project_id: &str) -> crate::Result<()> {
    // ANCHOR: rpc-backoff-use
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use std::time::Duration;
    // ANCHOR_END: rpc-backoff-use
    // ANCHOR: rpc-backoff-builder-trait
    use google_cloud_gax::options::RequestOptionsBuilder;
    // ANCHOR_END: rpc-backoff-builder-trait
    use speech::Poller;
    use speech::model::*;

    // ANCHOR: rpc-backoff-client
    let client = speech::client::Speech::builder().build().await?;
    // ANCHOR_END: rpc-backoff-client

    // ANCHOR: rpc-backoff-builder
    let response = client
        .batch_recognize()
        .set_recognizer(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        // ANCHOR_END: rpc-backoff-builder
        // ANCHOR: rpc-backoff-rpc-polling-backoff
        .with_polling_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(250))
                .with_maximum_delay(Duration::from_secs(10))
                .build()?,
        )
        // ANCHOR_END: rpc-backoff-rpc-polling-backoff
        // ANCHOR: rpc-backoff-prepare
        .set_files([BatchRecognizeFileMetadata::new().set_audio_source(
            batch_recognize_file_metadata::AudioSource::Uri(
                "gs://cloud-samples-data/speech/hello.wav".into(),
            ),
        )])
        .set_recognition_output_config(RecognitionOutputConfig::new().set_output(
            recognition_output_config::Output::InlineResponseConfig(
                InlineOutputConfig::new().into(),
            ),
        ))
        .set_processing_strategy(batch_recognize_request::ProcessingStrategy::DynamicBatching)
        .set_config(
            RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_decoding_config(recognition_config::DecodingConfig::AutoDecodingConfig(
                    AutoDetectDecodingConfig::new().into(),
                )),
        )
        // ANCHOR_END: rpc-backoff-prepare
        // ANCHOR: rpc-backoff-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: rpc-backoff-print

    Ok(())
}
// ANCHOR_END: rpc-backoff

// ANCHOR: client-errors
pub async fn client_errors(project_id: &str) -> crate::Result<()> {
    // ANCHOR: client-errors-use
    use google_cloud_gax::polling_error_policy::Aip194Strict;
    use google_cloud_gax::polling_error_policy::PollingErrorPolicyExt;
    use std::time::Duration;
    // ANCHOR_END: client-errors-use
    use speech::Poller;
    use speech::model::*;

    // ANCHOR: client-errors-client
    let client = speech::client::Speech::builder()
        .with_polling_error_policy(
            Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        .build()
        .await?;
    // ANCHOR_END: client-errors-client

    // ANCHOR: client-errors-builder
    let response = client
        .batch_recognize()
        .set_recognizer(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        // ANCHOR_END: client-errors-builder
        // ANCHOR: client-errors-prepare
        .set_files([BatchRecognizeFileMetadata::new().set_audio_source(
            batch_recognize_file_metadata::AudioSource::Uri(
                "gs://cloud-samples-data/speech/hello.wav".into(),
            ),
        )])
        .set_recognition_output_config(RecognitionOutputConfig::new().set_output(
            recognition_output_config::Output::InlineResponseConfig(
                InlineOutputConfig::new().into(),
            ),
        ))
        .set_processing_strategy(batch_recognize_request::ProcessingStrategy::DynamicBatching)
        .set_config(
            RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_decoding_config(recognition_config::DecodingConfig::AutoDecodingConfig(
                    AutoDetectDecodingConfig::new().into(),
                )),
        )
        // ANCHOR_END: client-errors-prepare
        // ANCHOR: client-errors-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: client-errors-print

    Ok(())
}
// ANCHOR_END: client-errors

// ANCHOR: rpc-errors
pub async fn rpc_errors(project_id: &str) -> crate::Result<()> {
    // ANCHOR: rpc-errors-use
    use google_cloud_gax::polling_error_policy::Aip194Strict;
    use google_cloud_gax::polling_error_policy::PollingErrorPolicyExt;
    use std::time::Duration;
    // ANCHOR_END: rpc-errors-use
    // ANCHOR: rpc-errors-builder-trait
    use google_cloud_gax::options::RequestOptionsBuilder;
    // ANCHOR_END: rpc-errors-builder-trait
    use speech::Poller;
    use speech::model::*;

    // ANCHOR: rpc-errors-client
    let client = speech::client::Speech::builder().build().await?;
    // ANCHOR_END: rpc-errors-client

    // ANCHOR: rpc-errors-builder
    let response = client
        .batch_recognize()
        .set_recognizer(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        // ANCHOR_END: rpc-errors-builder
        // ANCHOR: rpc-errors-rpc-polling-errors
        .with_polling_error_policy(
            Aip194Strict
                .with_attempt_limit(100)
                .with_time_limit(Duration::from_secs(300)),
        )
        // ANCHOR_END: rpc-errors-rpc-polling-backoff
        // ANCHOR: rpc-errors-prepare
        .set_files([BatchRecognizeFileMetadata::new().set_audio_source(
            batch_recognize_file_metadata::AudioSource::Uri(
                "gs://cloud-samples-data/speech/hello.wav".into(),
            ),
        )])
        .set_recognition_output_config(RecognitionOutputConfig::new().set_output(
            recognition_output_config::Output::InlineResponseConfig(
                InlineOutputConfig::new().into(),
            ),
        ))
        .set_processing_strategy(batch_recognize_request::ProcessingStrategy::DynamicBatching)
        .set_config(
            RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_decoding_config(recognition_config::DecodingConfig::AutoDecodingConfig(
                    AutoDetectDecodingConfig::new().into(),
                )),
        )
        // ANCHOR_END: rpc-errors-prepare
        // ANCHOR: rpc-errors-print
        .poller()
        .until_done()
        .await?;

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: rpc-errors-print

    Ok(())
}
// ANCHOR_END: rpc-errors
