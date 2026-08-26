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

#[cfg(all(test, feature = "run-integration-tests"))]
mod driver {

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn run_speech_streaming_recognize() -> anyhow::Result<()> {
        use google_cloud_test_utils::errors::anydump;
        use google_cloud_test_utils::tracing::enable_tracing;

        let _guard = enable_tracing();
        integration_tests_streaming::speech::streaming_recognize()
            .await
            .inspect_err(anydump)
    }
}
