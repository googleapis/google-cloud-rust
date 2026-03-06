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

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::resource::ResourceDetector;

#[derive(Clone, Debug)]
pub struct TestResourceDetector(String);

impl TestResourceDetector {
    pub fn new(id: &str) -> Self {
        Self(id.to_string())
    }
}

impl ResourceDetector for TestResourceDetector {
    fn detect(&self) -> Resource {
        // Provide resources that make this look like a `generic_node`. It must
        // have a `location`, `namespace`, and `node_id`:
        //     https://docs.cloud.google.com/monitoring/api/resources#tag_generic_node
        //
        // Using the [global] resource type, does not seem to work.
        //
        // [global]: https://docs.cloud.google.com/monitoring/api/resources#tag_global
        Resource::builder_empty()
            .with_attributes([
                // It seems that `telemetry.googleapis.com` rejects locations
                // that are not a valid region or zone. Since these tests may run
                // on laptops and workstations, we hard-code a value.
                KeyValue::new("location", "us-central1"),
                KeyValue::new("namespace", "google-cloud-rust"),
                KeyValue::new("node_id", self.0.clone()),
            ])
            .build()
    }
}
