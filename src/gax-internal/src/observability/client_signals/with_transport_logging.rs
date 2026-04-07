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

//! Implements [WithTransportLogging] a decorator for [Future] logging transport attempts.
//!
//! This is a private module, it is not exposed in the public API.

use super::RequestRecorder;
use crate::observability::attributes::SCHEMA_URL_VALUE;
use crate::observability::attributes::keys::*;
use crate::observability::errors::ErrorType;
use google_cloud_gax::error::Error;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub const NAME: &str = "experimental.transport.request.error";
pub const TARGET: &str = "experimental.transport.request";

/// A future instrumented to log transport attempts on failure.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WithTransportLogging<F> {
    #[pin]
    inner: F,
}

impl<F> WithTransportLogging<F>
where
    F: Future<Output = Result<reqwest::Response, Error>>,
{
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> Future for WithTransportLogging<F>
where
    F: Future<Output = Result<reqwest::Response, Error>>,
{
    type Output = <F as Future>::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let output = futures::ready!(this.inner.poll(cx));

        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return Poll::Ready(output);
        };

        match &output {
            Ok(_) => (),
            Err(error) => {
                let error_type = ErrorType::from_gax_error(error);
                let error_info = error.status().and_then(|s| {
                    s.details.iter().find_map(|d| match d {
                        google_cloud_gax::error::rpc::StatusDetails::ErrorInfo(i) => Some(i),
                        _ => None,
                    })
                });
                let error_domain = error_info.map(|i| i.domain.as_str());
                let error_metadata = error_info.and_then(|i| {
                    if i.metadata.is_empty() {
                        None
                    } else {
                        serde_json::to_string(&i.metadata).ok()
                    }
                });

                tracing::event!(
                    name: NAME,
                    target: TARGET,
                    tracing::Level::DEBUG,
                    { RPC_SYSTEM_NAME } = snapshot.rpc_system(),
                    { RPC_METHOD } = snapshot.rpc_method(),
                    { GCP_CLIENT_VERSION } = snapshot.client_version(),
                    { GCP_CLIENT_REPO } = snapshot.client_repo(),
                    { GCP_CLIENT_ARTIFACT } = snapshot.client_artifact(),
                    { GCP_SCHEMA_URL } = SCHEMA_URL_VALUE,
                    { URL_FULL } = snapshot.sanitized_url(),
                    { ERROR_TYPE } = error_type.as_str(),
                    { HTTP_RESPONSE_STATUS_CODE } = error.http_status_code().map(|v| v as i64),
                    { GCP_ERRORS_DOMAIN } = error_domain,
                    { GCP_ERRORS_METADATA } = error_metadata,
                    { HTTP_REQUEST_RESEND_COUNT } = snapshot.http_resend_count().map(|v| v as i64)
                );
            }
        }

        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn poll_without_recorder() -> anyhow::Result<()> {
        let pending = async move {
            let res: Result<reqwest::Response, Error> =
                Err(google_cloud_gax::error::Error::io("simulated"));
            res
        };

        // No recorder in scope
        let future = WithTransportLogging::new(pending);
        let result = future.await;
        assert!(result.is_err(), "{result:?}");

        Ok(())
    }
}
