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
mod tests {
    // ANCHOR: use
    use google_cloud_gax as gax;
    use google_cloud_speech_v2 as speech;
    // ANCHOR_END: use
    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    // ANCHOR: my_application_function
    // An example application function.
    //
    // It makes an RPC, setting some field. In this case, it is the `GetRecognizer`
    // RPC, setting the name field.
    //
    // It processes the response from the server. In this case, it extracts the
    // display name of the recognizer.
    async fn my_application_function(client: &speech::client::Speech) -> gax::Result<String> {
        client
            .get_recognizer()
            .set_name("invalid-test-recognizer")
            .send()
            .await
            .map(|r| r.display_name)
    }
    // ANCHOR_END: my_application_function

    // ANCHOR: mockall_macro
    mockall::mock! {
        #[derive(Debug)]
        Speech {}
        impl speech::stub::Speech for Speech {
            async fn get_recognizer(&self, req: speech::model::GetRecognizerRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<speech::model::Recognizer>>;
        }
    }
    // ANCHOR_END: mockall_macro

    #[tokio::test]
    async fn basic_success() -> Result<()> {
        // Create a mock, and set expectations on it.
        // ANCHOR: mock_new
        let mut mock = MockSpeech::new();
        // ANCHOR_END: mock_new
        // ANCHOR: mock_expectation
        mock.expect_get_recognizer()
            .withf(move |r, _|
                // Optionally, verify fields in the request.
                r.name == "invalid-test-recognizer")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    speech::model::Recognizer::new().set_display_name("test-display-name"),
                ))
            });
        // ANCHOR_END: mock_expectation

        // Create a client, implemented by the mock.
        // ANCHOR: client_from_mock
        let client = speech::client::Speech::from_stub(mock);
        // ANCHOR_END: client_from_mock

        // Call our function.
        // ANCHOR: call_fn
        let display_name = my_application_function(&client).await?;
        // ANCHOR_END: call_fn

        // Verify the final result of the RPC.
        // ANCHOR: validate
        assert_eq!(display_name, "test-display-name");
        // ANCHOR_END: validate

        Ok(())
    }

    #[tokio::test]
    async fn basic_fail() -> Result<()> {
        let mut mock = MockSpeech::new();
        // ANCHOR: error
        mock.expect_get_recognizer().return_once(|_, _| {
            // This time, return an error.
            use gax::error::Error;
            use gax::error::rpc::{Code, Status};
            let status = Status::default()
                .set_code(Code::NotFound)
                .set_message("Resource not found");
            Err(Error::service(status))
        });
        // ANCHOR_END: error

        // Create a client, implemented by the mock.
        let client = speech::client::Speech::from_stub(mock);

        // Call our function.
        let display_name = my_application_function(&client).await;

        // Verify the final result of the RPC.
        assert!(display_name.is_err());

        Ok(())
    }
}
// ANCHOR_END: all
