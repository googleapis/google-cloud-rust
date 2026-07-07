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
use google_cloud_gax::error::rpc::{Code, Status};

impl google_cloud_lro::internal::DiscoveryOperation for Operation {
    fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }
    fn done(&self) -> bool {
        self.status == Some(crate::model::operation::Status::Done)
    }
    fn error(&self) -> Option<Status> {
        if self.error.is_none()
            && self.http_error_status_code.is_none()
            && self.http_error_message.is_none()
        {
            return None;
        }

        let mut status = Status::default();

        let http_status = self.http_error_status_code.unwrap_or(200);
        let mut code = match http_status {
            200 => Code::Ok,
            400 => Code::InvalidArgument,
            401 => Code::Unauthenticated,
            403 => Code::PermissionDenied,
            404 => Code::NotFound,
            409 => Code::AlreadyExists,
            429 => Code::ResourceExhausted,
            499 => Code::Cancelled,
            500 => Code::Internal,
            501 => Code::Unimplemented,
            503 => Code::Unavailable,
            504 => Code::DeadlineExceeded,
            _ => Code::Unknown,
        };

        let mut message = self.http_error_message.clone().unwrap_or_default();

        if let Some(first_err) = self.error.as_ref().and_then(|err| err.errors.first()) {
            if code == Code::Ok {
                code = Code::Unknown;
            }
            if let Some(err_code) = &first_err.code {
                code = match err_code.as_str() {
                    "QUOTA_EXCEEDED" => Code::ResourceExhausted,
                    other => Code::try_from(other).unwrap_or(code),
                };
            }
            if let Some(err_msg) = &first_err.message {
                message = err_msg.clone();
            }
        }

        if code == Code::Ok && http_status == 200 {
            return None;
        }

        status = status.set_code(code).set_message(message);
        Some(status)
    }
}
