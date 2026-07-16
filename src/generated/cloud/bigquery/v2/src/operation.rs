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

use crate::model::Job;
use google_cloud_gax::error::rpc::{Code, Status};

impl google_cloud_lro::internal::DiscoveryOperation for Job {
    fn name(&self) -> Option<&String> {
        self.job_reference.as_ref().map(|r| &r.job_id)
    }

    fn done(&self) -> bool {
        self.status
            .as_ref()
            .map(|s| s.state == "DONE")
            .unwrap_or(false)
    }

    fn error(&self) -> Option<Status> {
        self.status.as_ref().and_then(|s| {
            s.error_result.as_ref().map(|e| {
                Status::default()
                    .set_code(Code::Unknown)
                    .set_message(e.message.clone())
            })
        })
    }
}
