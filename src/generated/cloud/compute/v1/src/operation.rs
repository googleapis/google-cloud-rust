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

use crate::model::Operation;

impl google_cloud_lro::internal::DiscoveryOperation for Operation {
    fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
    fn done(&self) -> bool {
        self.status == Some(crate::model::operation::Status::Done)
    }
    fn error(&self) -> Option<google_cloud_gax::error::rpc::Status> {
        if self.error.is_none()
            && self.http_error_status_code.is_none()
            && self.http_error_message.is_none()
        {
            return None;
        }

        let mut status = google_cloud_gax::error::rpc::Status::default();

        let http_status = self.http_error_status_code.unwrap_or(200);
        let mut code = match http_status {
            200 => google_cloud_gax::error::rpc::Code::Ok,
            400 => google_cloud_gax::error::rpc::Code::InvalidArgument,
            401 => google_cloud_gax::error::rpc::Code::Unauthenticated,
            403 => google_cloud_gax::error::rpc::Code::PermissionDenied,
            404 => google_cloud_gax::error::rpc::Code::NotFound,
            409 => google_cloud_gax::error::rpc::Code::AlreadyExists,
            429 => google_cloud_gax::error::rpc::Code::ResourceExhausted,
            499 => google_cloud_gax::error::rpc::Code::Cancelled,
            500 => google_cloud_gax::error::rpc::Code::Internal,
            501 => google_cloud_gax::error::rpc::Code::Unimplemented,
            503 => google_cloud_gax::error::rpc::Code::Unavailable,
            504 => google_cloud_gax::error::rpc::Code::DeadlineExceeded,
            _ => google_cloud_gax::error::rpc::Code::Unknown,
        };

        let mut message = self.http_error_message.clone().unwrap_or_default();

        if let Some(first_err) = self.error.as_ref().and_then(|err| err.errors.first()) {
            if code == google_cloud_gax::error::rpc::Code::Ok {
                code = google_cloud_gax::error::rpc::Code::Unknown;
            }
            if let Some(err_code) = &first_err.code {
                if let Ok(c) = google_cloud_gax::error::rpc::Code::try_from(err_code.as_str()) {
                    code = c;
                } else if err_code == "QUOTA_EXCEEDED" {
                    code = google_cloud_gax::error::rpc::Code::ResourceExhausted;
                }
            }
            if let Some(err_msg) = &first_err.message {
                message = err_msg.clone();
            }
        }

        if code == google_cloud_gax::error::rpc::Code::Ok && http_status == 200 {
            return None;
        }

        status = status.set_code(code).set_message(message);
        Some(status)
    }
}
