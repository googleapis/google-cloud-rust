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

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use google_cloud_auth::build_errors::Error as BuildError;
use google_cloud_gax::client_builder::Error as ClientBuilderError;
use google_cloud_gax::error::Error;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("the backend reported an error: {0}")]
    Backend(#[source] Error),
    #[error("the backend response had an unexpected format: {0}")]
    BadResponseFormat(String),
    #[error("there was a problem contacting the backend: {0}")]
    Request(#[source] Error),
    #[error("cannot initialize the service credentials: {0}")]
    Credentials(#[source] BuildError),
    #[error("cannot initialize a client: {0}")]
    Client(#[source] ClientBuilderError),
}

impl From<Error> for AppError {
    fn from(value: Error) -> Self {
        if value.status().is_some() {
            Self::Backend(value)
        } else {
            Self::Request(value)
        }
    }
}

impl From<BuildError> for AppError {
    fn from(value: BuildError) -> Self {
        Self::Credentials(value)
    }
}

impl From<ClientBuilderError> for AppError {
    fn from(value: ClientBuilderError) -> Self {
        Self::Client(value)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!("internal service error: {self:?}");
        let (status, message) = match self {
            Self::Backend(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")),
            Self::BadResponseFormat(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")),
            Self::Request(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#?}")),
            Self::Credentials(e) => (StatusCode::UNAUTHORIZED, format!("{e:#?}")),
            Self::Client(e) => (StatusCode::SERVICE_UNAVAILABLE, format!("{e:#?}")),
        };
        (status, message).into_response()
    }
}
