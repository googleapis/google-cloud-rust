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

impl lro::internal::discovery::DiscoveryOperation for crate::model::Operation {
    fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
    fn done(&self) -> bool {
        self.status == Some(crate::model::operation::Status::Done)
    }
    fn error(&self) -> Option<crate::Error> {
        use gax::error::rpc::Code;

        let error = self.error.as_ref()?;
        let code = error
            .errors
            .iter()
            .filter_map(|e| e.code.as_ref())
            .filter_map(|c| Code::try_from(c.as_str()).ok())
            .take(1).next();
        let message = error
            .errors
            .iter()
            .flat_map(|e| e.message.as_ref())
            .take(1).next();
        let status = gax::error::rpc::Status::default();
        let status = code.into_iter().fold(status, |s, c| s.set_code(c));
        let status = message.into_iter().fold(status, |s, m| s.set_message(m));
        Some(crate::Error::service(status))
    }
}
