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

use crate::Result;
use crate::model_ext::ObjectHighlights;
use crate::read_object::dynamic::ReadObjectResponse as DynamicReadObjectResponse;

/// Implements the [ReadObjectResponse][DynamicReadObjectResponse] trait with
/// tracing annotations.
#[derive(Debug)]
pub(crate) struct TracingResponse<T> {
    inner: T,
    span: tracing::Span,
}

impl<T> TracingResponse<T> {
    pub fn new(inner: T, span: tracing::Span) -> Self {
        Self { inner, span }
    }
}

#[async_trait::async_trait]
impl DynamicReadObjectResponse for TracingResponse<Box<dyn DynamicReadObjectResponse + Send>> {
    fn object(&self) -> ObjectHighlights {
        self.inner.object()
    }

    async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        use ::tracing::Instrument as _;
        self.inner.next().instrument(self.span.clone()).await
    }
}
