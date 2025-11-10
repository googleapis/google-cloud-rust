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

pub mod pubsub {
    pub mod quickstart;

    pub use pubsub_samples::random_topic_id;

    #[cfg(all(test, feature = "run-integration-tests"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn quickstart() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
        let topic_id = random_topic_id();
        let result = quickstart::quickstart(&project_id, &topic_id).await;
        result
    }
}
