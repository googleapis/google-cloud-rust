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

use anyhow::Result;
use google_cloud_speech_v2::client::Speech;
use google_cloud_speech_v2::model::{
    AutoDetectDecodingConfig, RecognitionConfig, StreamingRecognitionConfig,
    StreamingRecognizeRequest,
};
use google_cloud_test_utils::runtime_config::project_id;

pub async fn streaming_recognize() -> Result<()> {
    let project_id = project_id()?;
    let recognizer = format!("projects/{project_id}/locations/global/recognizers/_");

    let client = Speech::builder().with_tracing().build().await?;

    let recognition_config = RecognitionConfig::new()
        .set_auto_decoding_config(AutoDetectDecodingConfig::new())
        .set_language_codes(["en-US"])
        .set_model("latest_short");

    let streaming_config = StreamingRecognitionConfig::new().set_config(recognition_config);

    let (sender, mut resp_stream) = client.streaming_recognize().build();

    let config_req = StreamingRecognizeRequest::new()
        .set_recognizer(&recognizer)
        .set_streaming_config(Box::new(streaming_config));
    sender.send(config_req).await?;

    let audio = reqwest::get("https://storage.googleapis.com/cloud-samples-data/speech/hello.wav")
        .await?
        .bytes()
        .await?;

    // Stream audio in 4KB chunks
    let mut offset = 0;
    while offset < audio.len() {
        let end = std::cmp::min(offset + 4096, audio.len());
        let chunk = audio.slice(offset..end);
        let req = StreamingRecognizeRequest::new().set_audio(chunk);
        sender.send(req).await?;
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        offset = end;
    }

    // Half-close the stream from client side
    drop(sender);

    let mut transcripts = Vec::new();
    while let Some(res) = resp_stream.next().await {
        let response = res?;
        tracing::info!("Received response: {response:?}");
        for result in response.results {
            for alt in result.alternatives {
                transcripts.push(alt.transcript);
            }
        }
    }

    tracing::info!("Streaming recognize completed successfully");
    assert!(
        transcripts
            .iter()
            .any(|t| t.to_lowercase().contains("hello")),
        "expected 'hello' in transcripts: {transcripts:?}"
    );

    Ok(())
}
