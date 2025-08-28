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

use crate::Result;
use crate::model::{Object, ReadObjectRequest};
use crate::model_ext::WriteObjectRequest;
use crate::read_object::ReadObjectResponse;
use crate::storage::request_options::RequestOptions;
use crate::streaming_source::{Seek, StreamingSource};

/// Defines the trait used to implement [crate::client::Storage].
///
/// Application developers may need to implement this trait to mock
/// `client::Storage`. In other use-cases, application developers only
/// use `client::Storage` and need not be concerned with this trait or
/// its implementations.
///
/// Services gain new RPCs routinely. Consequently, this trait gains new methods
/// too. To avoid breaking applications the trait provides a default
/// implementation of each method. Most of these implementations just return an
/// error.
pub trait Storage: std::fmt::Debug + Send + Sync {
    /// Implements [crate::client::Storage::read_object].
    fn read_object(
        &self,
        _req: ReadObjectRequest,
        _options: RequestOptions,
    ) -> impl std::future::Future<Output = Result<ReadObjectResponse>> + Send {
        unimplemented_stub::<ReadObjectResponse>()
    }
    /// Implements [crate::client::Storage::write_object].
    fn write_object_buffered<P>(
        &self,
        _payload: P,
        _req: WriteObjectRequest,
        _options: RequestOptions,
    ) -> impl std::future::Future<Output = Result<Object>> + Send
    where
        P: StreamingSource + Send + Sync + 'static,
    {
        unimplemented_stub::<Object>()
    }
    /// Implements [crate::client::Storage::write_object].
    fn write_object_unbuffered<P>(
        &self,
        _payload: P,
        _req: WriteObjectRequest,
        _options: RequestOptions,
    ) -> impl std::future::Future<Output = Result<Object>> + Send
    where
        P: StreamingSource + Seek + Send + Sync + 'static,
    {
        unimplemented_stub::<Object>()
    }
}

async fn unimplemented_stub<T>() -> gax::Result<T> {
    unimplemented!(concat!(
        "to prevent breaking changes as services gain new RPCs, the stub ",
        "traits provide default implementations of each method. In the client ",
        "libraries, all implementations of the traits override all methods. ",
        "Therefore, this error should not appear in normal code using the ",
        "client libraries. The only expected context for this error is test ",
        "code mocking the client libraries. If that is how you got this ",
        "error, verify that you have mocked all methods used in your test. ",
        "Otherwise, please open a bug at ",
        "https://github.com/googleapis/google-cloud-rust/issues"
    ));
}
