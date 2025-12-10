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
#[cfg(google_cloud_unstable_storage_bidi)]
use crate::{
    model_ext::{OpenObjectRequest, ReadRange},
    object_descriptor::HeaderMap,
    object_descriptor::ObjectDescriptor as Descriptor,
};
use gaxi::unimplemented::UNIMPLEMENTED;

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

    #[cfg(google_cloud_unstable_storage_bidi)]
    /// Implements [crate::client::Storage::open_object].
    fn open_object(
        &self,
        _request: OpenObjectRequest,
        _options: RequestOptions,
    ) -> impl std::future::Future<Output = Result<Descriptor>> + Send {
        unimplemented_stub::<Descriptor>()
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
/// Defines the trait used to implement [crate::object_descriptor::ObjectDescriptor].
///
/// Application developers may need to implement this trait to mock
/// `ObjectDescriptor`. In other use-cases, application developers
/// should use `ObjectDescriptor` directly, and need not be concerned
/// with this trait or its implementations.
pub trait ObjectDescriptor: std::fmt::Debug + Send + Sync {
    /// The implementation for [ObjectDescriptor::object][Descriptor::object].
    fn object(&self) -> &Object;

    /// The implementation for [ObjectDescriptor::read_range][Descriptor::read_range].
    fn read_range(
        &self,
        range: ReadRange,
    ) -> impl Future<Output = ReadObjectResponse> + Send + Sync;

    /// The implementation for [ObjectDescriptor::headers][Descriptor::headers].
    fn headers(&self) -> &HeaderMap;
}

async fn unimplemented_stub<T>() -> gax::Result<T> {
    unimplemented!("{UNIMPLEMENTED}");
}
