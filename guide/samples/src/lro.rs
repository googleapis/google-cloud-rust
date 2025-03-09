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

// ANCHOR: use
use google_cloud_longrunning as longrunning;
use google_cloud_speech_v2 as speech;
// ANCHOR_END: use

// ANCHOR: start
pub async fn start(project_id: &str) -> crate::Result<()> {
    // ANCHOR: client
    let client = speech::client::Speech::new().await?;
    // ANCHOR_END: client

    // ANCHOR: request-builder
    let operation = client
        .batch_recognize(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        // ANCHOR_END: request-builder
        // ANCHOR: audio-file
        .set_files([speech::model::BatchRecognizeFileMetadata::new()
            .set_uri("gs://cloud-samples-data/speech/hello.wav")])
        // ANCHOR_END: audio-file
        // ANCHOR: transcript-output
        .set_recognition_output_config(
            speech::model::RecognitionOutputConfig::new()
                .set_inline_response_config(speech::model::InlineOutputConfig::new()),
        )
        // ANCHOR_END: transcript-output
        // ANCHOR: configuration
        .set_processing_strategy(
            speech::model::batch_recognize_request::ProcessingStrategy::DYNAMIC_BATCHING,
        )
        .set_config(
            speech::model::RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_auto_decoding_config(speech::model::AutoDetectDecodingConfig::new()),
        )
        // ANCHOR_END: configuration
        // ANCHOR: send
        .send()
        .await?;
    // ANCHOR_END: send
    println!("LRO started, response={operation:?}");

    // ANCHOR: call-manual
    let response = manually_poll_lro(client, operation).await;
    println!("LRO completed, response={response:?}");
    // ANCHOR_END: call-manual

    Ok(())
}
// ANCHOR_END: start

// ANCHOR: automatic
pub async fn automatic(project_id: &str) -> crate::Result<()> {
    // ANCHOR: automatic-use
    use speech::Poller;
    // ANCHOR_END: automatic-use
    // ANCHOR: automatic-prepare
    let client = speech::client::Speech::new().await?;

    let response = client
        .batch_recognize(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        .set_files([speech::model::BatchRecognizeFileMetadata::new()
            .set_uri("gs://cloud-samples-data/speech/hello.wav")])
        .set_recognition_output_config(
            speech::model::RecognitionOutputConfig::new()
                .set_inline_response_config(speech::model::InlineOutputConfig::new()),
        )
        .set_processing_strategy(
            speech::model::batch_recognize_request::ProcessingStrategy::DYNAMIC_BATCHING,
        )
        .set_config(
            speech::model::RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_auto_decoding_config(speech::model::AutoDetectDecodingConfig::new()),
        )
        // ANCHOR_END: automatic-prepare
        // ANCHOR: automatic-print
        // ANCHOR: automatic-poller-until-done
        .poller()
        .until_done()
        .await?;
    // ANCHOR_END: automatic-poller-until-done

    println!("LRO completed, response={response:?}");
    // ANCHOR_END: automatic-print

    Ok(())
}
// ANCHOR_END: automatic

// ANCHOR: polling
pub async fn polling(project_id: &str) -> crate::Result<()> {
    // ANCHOR: polling-use
    use speech::Poller;
    // ANCHOR_END: polling-use
    // ANCHOR: polling-prepare
    let client = speech::client::Speech::new().await?;

    let mut poller = client
        .batch_recognize(format!(
            "projects/{project_id}/locations/global/recognizers/_"
        ))
        .set_files([speech::model::BatchRecognizeFileMetadata::new()
            .set_uri("gs://cloud-samples-data/speech/hello.wav")])
        .set_recognition_output_config(
            speech::model::RecognitionOutputConfig::new()
                .set_inline_response_config(speech::model::InlineOutputConfig::new()),
        )
        .set_processing_strategy(
            speech::model::batch_recognize_request::ProcessingStrategy::DYNAMIC_BATCHING,
        )
        .set_config(
            speech::model::RecognitionConfig::new()
                .set_language_codes(["en-US"])
                .set_model("short")
                .set_auto_decoding_config(speech::model::AutoDetectDecodingConfig::new()),
        )
        // ANCHOR_END: polling-prepare
        // ANCHOR: polling-poller
        .poller();
    // ANCHOR_END: polling-poller

    // ANCHOR: polling-loop
    while let Some(p) = poller.poll().await {
        match p {
            speech::PollingResult::Completed(r) => {
                println!("LRO completed, response={r:?}");
            }
            speech::PollingResult::InProgress(m) => {
                println!("LRO in progress, metadata={m:?}");
            }
            speech::PollingResult::PollingError(e) => {
                println!("Transient error polling the LRO: {e}");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    // ANCHOR_END: polling-loop

    Ok(())
}
// ANCHOR_END: polling

// ANCHOR: manual
pub async fn manually_poll_lro(
    client: speech::client::Speech,
    operation: longrunning::model::Operation,
) -> crate::Result<speech::model::BatchRecognizeResponse> {
    let mut operation = operation;
    // ANCHOR: manual-if-done
    loop {
        if operation.done {
            // ANCHOR_END: manual-if-done
            // ANCHOR: manual-match-none
            match &operation.result {
                None => {
                    return Err("missing result for finished operation".into());
                }
                // ANCHOR_END: manual-match-none
                // ANCHOR: manual-match-error
                Some(r) => {
                    return match r {
                        longrunning::model::operation::Result::Error(e) => {
                            Err(format!("{e:?}").into())
                        }
                        // ANCHOR_END: manual-match-error
                        // ANCHOR: manual-match-success
                        longrunning::model::operation::Result::Response(any) => {
                            let response =
                                any.try_into_message::<speech::model::BatchRecognizeResponse>()?;
                            Ok(response)
                        }
                        // ANCHOR_END: manual-match-success
                        // ANCHOR: manual-match-default
                        _ => Err(format!("unexpected result branch {r:?}").into()),
                        // ANCHOR_END: manual-match-default
                    };
                }
            }
        }
        // ANCHOR: manual-metadata
        if let Some(any) = &operation.metadata {
            let metadata = any.try_into_message::<speech::model::OperationMetadata>()?;
            println!("LRO in progress, metadata={metadata:?}");
        }
        // ANCHOR_END: manual-metadata
        // ANCHOR: manual-backoff
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // ANCHOR_END: manual-backoff
        // ANCHOR: manual-poll-again
        if let Ok(attempt) = client.get_operation(operation.name.clone()).send().await {
            operation = attempt;
        }
        // ANCHOR_END: manual-poll-again
    }
}
// ANCHOR_END: manual
