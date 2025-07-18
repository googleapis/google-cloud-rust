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
use longrunning::model::Operation;
use longrunning::model::operation::Result as OperationResult;
use speech::model::{BatchRecognizeRequest, BatchRecognizeResponse};

// Example application code that is under test
mod my_application {
    use super::*;

    // ANCHOR: app-fn
    // An example application function that automatically polls.
    //
    // It starts an LRO, awaits the result, and processes it.
    pub async fn my_automatic_poller(
        client: &speech::client::Speech,
        project_id: &str,
    ) -> Result<Option<wkt::Duration>> {
        use google_cloud_lro::Poller;
        client
            .batch_recognize()
            .set_recognizer(format!(
                "projects/{project_id}/locations/global/recognizers/_"
            ))
            .poller()
            .until_done()
            .await
            .map(|r| r.total_billed_duration)
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
        }
    }
    // ANCHOR_END: mockall-macro

    // ANCHOR: expected-response
    fn expected_duration() -> Option<wkt::Duration> {
        Some(wkt::Duration::clamp(100, 0))
    }

    fn expected_response() -> BatchRecognizeResponse {
        BatchRecognizeResponse::new().set_or_clear_total_billed_duration(expected_duration())
    }
    // ANCHOR_END: expected-response

    // ANCHOR: finished-op
    fn make_finished_operation(response: &BatchRecognizeResponse) -> Result<Response<Operation>> {
        let any = wkt::Any::from_msg(response).expect("test message should succeed");
        let operation = Operation::new()
            // ANCHOR: set-done-true
            .set_done(true)
            // ANCHOR_END: set-done-true
            .set_result(OperationResult::Response(any.into()));
        Ok(Response::from(operation))
    }
    // ANCHOR_END: finished-op

    #[tokio::test]
    async fn automatic_polling() -> Result<()> {
        // Create a mock, and set expectations on it.
        // ANCHOR: mock-expectations
        let mut mock = MockSpeech::new();
        mock.expect_batch_recognize()
            .return_once(|_, _| make_finished_operation(&expected_response()));
        // ANCHOR_END: mock-expectations

        // ANCHOR: client-call
        // Create a client, implemented by our mock.
        let client = speech::client::Speech::from_stub(mock);

        // Call our function which automatically polls.
        let billed_duration = my_automatic_poller(&client, "my-project").await?;

        // Verify the final result of the LRO.
        assert_eq!(billed_duration, expected_duration());
        // ANCHOR_END: client-call

        Ok(())
    }
}
// ANCHOR_END: all
