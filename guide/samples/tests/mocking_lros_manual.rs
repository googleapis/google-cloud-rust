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

//! Examples showing how to simulate LROs in tests.

// ANCHOR: all
use gax::Result;
use gax::response::Response;
use google_cloud_gax as gax;
use google_cloud_longrunning as longrunning;
use google_cloud_speech_v2 as speech;
use google_cloud_wkt as wkt;
use longrunning::model::operation::Result as OperationResult;
use longrunning::model::{GetOperationRequest, Operation};
use speech::model::{BatchRecognizeRequest, BatchRecognizeResponse, OperationMetadata};

// Example application code that is under test
mod my_application {
    use super::*;

    // ANCHOR: app-fn
    pub struct BatchRecognizeResult {
        pub progress_updates: Vec<i32>,
        pub billed_duration: Result<Option<wkt::Duration>>,
    }

    // An example application function that manually polls.
    //
    // It starts an LRO. It consolidates the polling results, whether full or
    // partial.
    //
    // In this case, it is the `BatchRecognize` RPC. If we get a partial update,
    // we extract the `progress_percent` field. If we get a final result, we
    // extract the `total_billed_duration` field.
    pub async fn my_manual_poller(
        client: &speech::client::Speech,
        project_id: &str,
    ) -> BatchRecognizeResult {
        use google_cloud_lro::{Poller, PollingResult};
        let mut progress_updates = Vec::new();
        let mut poller = client
            .batch_recognize()
            .set_recognizer(format!(
                "projects/{project_id}/locations/global/recognizers/_"
            ))
            .poller();
        while let Some(p) = poller.poll().await {
            match p {
                PollingResult::Completed(r) => {
                    let billed_duration = r.map(|r| r.total_billed_duration);
                    return BatchRecognizeResult {
                        progress_updates,
                        billed_duration,
                    };
                }
                PollingResult::InProgress(m) => {
                    if let Some(metadata) = m {
                        // This is a silly application. Your application likely
                        // performs some task immediately with the partial
                        // update, instead of storing it for after the operation
                        // has completed.
                        progress_updates.push(metadata.progress_percent);
                    }
                }
                PollingResult::PollingError(e) => {
                    return BatchRecognizeResult {
                        progress_updates,
                        billed_duration: Err(e),
                    };
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // We can only get here if `poll()` returns `None`, but it only returns
        // `None` after it returned `PollingResult::Completed`. Therefore this
        // is never reached.
        unreachable!("loop should exit via the `Completed` branch.");
    }
    // ANCHOR_END: app-fn
}

#[cfg(test)]
mod tests {
    use super::my_application::*;
    use super::*;

    // ANCHOR: mockall-macro
    mockall::mock! {
        #[derive(Debug)]
        Speech {}
        impl speech::stub::Speech for Speech {
            async fn batch_recognize(&self, req: BatchRecognizeRequest, _options: gax::options::RequestOptions) -> Result<Response<Operation>>;
            async fn get_operation(&self, req: GetOperationRequest, _options: gax::options::RequestOptions) -> Result<Response<Operation>>;
        }
    }
    // ANCHOR_END: mockall-macro

    fn expected_duration() -> Option<wkt::Duration> {
        Some(wkt::Duration::clamp(100, 0))
    }

    fn expected_response() -> BatchRecognizeResponse {
        BatchRecognizeResponse::new().set_or_clear_total_billed_duration(expected_duration())
    }

    fn make_finished_operation(
        response: &BatchRecognizeResponse,
    ) -> Result<gax::response::Response<Operation>> {
        let any = wkt::Any::from_msg(response).expect("test message should succeed");
        let operation = Operation::new()
            .set_done(true)
            .set_result(OperationResult::Response(any.into()));
        Ok(Response::from(operation))
    }

    // ANCHOR: partial-op
    fn make_partial_operation(progress: i32) -> Result<Response<Operation>> {
        let metadata = OperationMetadata::new().set_progress_percent(progress);
        let any = wkt::Any::from_msg(&metadata).expect("test message should succeed");
        let operation = Operation::new().set_metadata(any);
        Ok(Response::from(operation))
    }
    // ANCHOR_END: partial-op

    #[tokio::test]
    async fn manual_polling_with_metadata() -> Result<()> {
        // ANCHOR: mock-expectations
        let mut seq = mockall::Sequence::new();
        let mut mock = MockSpeech::new();
        mock.expect_batch_recognize()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(25));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(50));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(75));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_finished_operation(&expected_response()));
        // ANCHOR_END: mock-expectations

        // ANCHOR: client-call
        // Create a client, implemented by our mock.
        let client = speech::client::Speech::from_stub(mock);

        // Call our function which manually polls.
        let result = my_manual_poller(&client, "my-project").await;

        // Verify the partial metadata updates, and the final result.
        assert_eq!(result.progress_updates, [25, 50, 75]);
        assert_eq!(result.billed_duration?, expected_duration());
        // ANCHOR_END: client-call

        Ok(())
    }
}
// ANCHOR_END: all
