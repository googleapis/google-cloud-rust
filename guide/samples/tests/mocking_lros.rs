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

//! Examples showing how to mock a client in tests.

// ANCHOR: all
#[cfg(test)]
mod test {
    use google_cloud_gax as gax;
    use google_cloud_longrunning as longrunning;
    use google_cloud_speech_v2 as speech;
    use google_cloud_wkt as wkt;
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    // ANCHOR: mockall-macro
    mockall::mock! {
        #[derive(Debug)]
        Speech {}
        impl speech::stub::Speech for Speech {
            async fn batch_recognize(&self, req: speech::model::BatchRecognizeRequest, _options: gax::options::RequestOptions) -> gax::Result<longrunning::model::Operation>;
            async fn get_operation(&self, req: longrunning::model::GetOperationRequest, _options: gax::options::RequestOptions) -> gax::Result<longrunning::model::Operation>;
        }
    }
    // ANCHOR_END: mockall-macro

    // ANCHOR: expected-response
    fn expected_duration() -> Option<wkt::Duration> {
        Some(wkt::Duration::clamp(100, 0))
    }

    fn expected_response() -> speech::model::BatchRecognizeResponse {
        speech::model::BatchRecognizeResponse::new().set_total_billed_duration(expected_duration())
    }
    // ANCHOR_END: expected-response

    // ANCHOR: auto-fn
    // An example application function that automatically polls.
    //
    // It starts an LRO, awaits the result, and processes it.
    async fn my_automatic_poller(
        client: &speech::client::Speech,
    ) -> gax::Result<Option<wkt::Duration>> {
        use speech::Poller;
        client
            .batch_recognize("invalid-test-recognizer")
            .poller()
            .until_done()
            .await
            .map(|r| r.total_billed_duration)
    }
    // ANCHOR_END: auto-fn

    // ANCHOR: finished-op
    fn make_finished_operation(
        response: &speech::model::BatchRecognizeResponse,
    ) -> Result<longrunning::model::Operation> {
        let any = wkt::Any::try_from(response)?;
        let operation = longrunning::model::Operation::new()
            // ANCHOR: set-done-true
            .set_done(true)
            // ANCHOR_END: set-done-true
            .set_result(longrunning::model::operation::Result::Response(any.into()));
        Ok(operation)
    }
    // ANCHOR_END: finished-op

    #[tokio::test]
    async fn automatic_polling() -> Result<()> {
        // Create a mock, and set expectations on it.
        // ANCHOR: auto-mock-expectations
        let mut mock = MockSpeech::new();
        mock.expect_batch_recognize().return_once(move |_, _| {
            make_finished_operation(&expected_response()).map_err(gax::error::Error::serde)
        });
        // ANCHOR_END: auto-mock-expectations

        // ANCHOR: auto-client-call
        // Create a client, implemented by our mock.
        let client = speech::client::Speech::from_stub(mock);

        // Call our function which automatically polls.
        let billed_duration = my_automatic_poller(&client).await?;

        // Verify the final result of the LRO.
        assert_eq!(billed_duration, expected_duration());
        // ANCHOR_END: auto-client-call

        Ok(())
    }

    // ANCHOR: manual-fn
    struct BatchRecognizeResult {
        progress_updates: Vec<i32>,
        billed_duration: gax::Result<Option<wkt::Duration>>,
    }

    // An example application function that manually polls.
    //
    // It starts an LRO. It consolidates the polling results, whether full or
    // partial.
    //
    // In this case, it is the `BatchRecognize` RPC. If we get a partial update,
    // we extract the `progress_percent` field. If we get a final result, we
    // extract the `total_billed_duration` field.
    async fn my_manual_poller(client: &speech::client::Speech) -> BatchRecognizeResult {
        use speech::Poller;
        let mut progress_updates = Vec::<i32>::new();
        let mut poller = client.batch_recognize("invalid-test-recognizer").poller();
        while let Some(p) = poller.poll().await {
            match p {
                speech::PollingResult::Completed(r) => {
                    let billed_duration = r.map(|r| r.total_billed_duration);
                    return BatchRecognizeResult {
                        progress_updates,
                        billed_duration,
                    };
                }
                speech::PollingResult::InProgress(m) => {
                    println!("LRO in progress, metadata={m:?}");
                    if let Some(metadata) = m {
                        // This is a silly application. Your application likely
                        // performs some task immediately with the partial
                        // update, instead of storing it for after the operation
                        // has completed.
                        progress_updates.push(metadata.progress_percent);
                    }
                }
                speech::PollingResult::PollingError(e) => {
                    return BatchRecognizeResult {
                        progress_updates,
                        billed_duration: Err(gax::error::Error::from(e)),
                    };
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        // ANCHOR_END: manual-fn

        BatchRecognizeResult {
            progress_updates,
            billed_duration: Ok(None),
        }
        // ANCHOR: manual-fn
    }
    // ANCHOR_END: manual-fn

    // ANCHOR: partial-op
    fn make_partial_operation(progress: i32) -> Result<longrunning::model::Operation> {
        let metadata = speech::model::OperationMetadata::new().set_progress_percent(progress);
        let any = wkt::Any::try_from(&metadata)?;
        let operation = longrunning::model::Operation::new().set_metadata(Some(any));
        Ok(operation)
    }
    // ANCHOR_END: partial-op

    #[tokio::test]
    async fn manual_polling_with_metadata() -> Result<()> {
        // ANCHOR: manual-mock-expectations
        let mut seq = mockall::Sequence::new();
        let mut mock = MockSpeech::new();
        mock.expect_batch_recognize()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(25).map_err(gax::error::Error::serde));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(50).map_err(gax::error::Error::serde));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(75).map_err(gax::error::Error::serde));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| {
                make_finished_operation(&expected_response()).map_err(gax::error::Error::serde)
            });
        // ANCHOR_END: manual-mock-expectations

        // ANCHOR: manual-client-call
        // Create a client, implemented by our mock.
        let client = speech::client::Speech::from_stub(mock);

        // Call our function which manually polls.
        let result = my_manual_poller(&client).await;

        // Verify the partial metadata updates, and the final result.
        assert_eq!(result.progress_updates, [25, 50, 75]);
        assert_eq!(result.billed_duration?, expected_duration());
        // ANCHOR_END: manual-client-call

        Ok(())
    }

    #[tokio::test]
    async fn simulating_errors() -> Result<()> {
        let mut seq = mockall::Sequence::new();
        let mut mock = MockSpeech::new();
        mock.expect_batch_recognize()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(25).map_err(gax::error::Error::serde));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(50).map_err(gax::error::Error::serde));
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| make_partial_operation(75).map_err(gax::error::Error::serde));
        // ANCHOR: error
        mock.expect_get_operation()
            .once()
            .in_sequence(&mut seq)
            .returning(|_, _| {
                // This time, return an error.
                Err(gax::error::Error::other("fail"))
            });
        // ANCHOR_END: error

        // Create a client, implemented by our mock.
        let client = speech::client::Speech::from_stub(mock);

        // Call our function which manually polls.
        let result = my_manual_poller(&client).await;

        // Verify the partial metadata updates, and the final result.
        assert_eq!(result.progress_updates, [25, 50, 75]);
        assert!(result.billed_duration.is_err());

        Ok(())
    }
}
// ANCHOR_END: all
