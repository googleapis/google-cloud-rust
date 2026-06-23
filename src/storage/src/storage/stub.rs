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
use crate::{
    http::HeaderMap,
    model_ext::{OpenObjectRequest, ReadRange},
    object_descriptor::ObjectDescriptor as Descriptor,
};
#[cfg(google_cloud_unstable_storage_bidi)]
use bytes::Bytes;
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

    /// Implements [crate::client::Storage::open_object].
    fn open_object(
        &self,
        _request: OpenObjectRequest,
        _options: RequestOptions,
    ) -> impl std::future::Future<Output = Result<(Descriptor, Vec<ReadObjectResponse>)>> + Send
    {
        unimplemented_stub::<(Descriptor, Vec<ReadObjectResponse>)>()
    }
}

/// Defines the trait used to implement [crate::object_descriptor::ObjectDescriptor].
///
/// Application developers may need to implement this trait to mock
/// `ObjectDescriptor`. In other use-cases, application developers
/// should use `ObjectDescriptor` directly, and need not be concerned
/// with this trait or its implementations.
pub trait ObjectDescriptor: std::fmt::Debug + Send + Sync {
    /// The implementation for [ObjectDescriptor::object][Descriptor::object].
    fn object(&self) -> Object;

    /// The implementation for [ObjectDescriptor::read_range][Descriptor::read_range].
    fn read_range(&self, range: ReadRange) -> impl Future<Output = ReadObjectResponse> + Send;

    /// The implementation for [ObjectDescriptor::headers][Descriptor::headers].
    fn headers(&self) -> HeaderMap;
}

/// Defines the trait used to implement [crate::appendable_object_writer::AppendableObjectWriter].
///
/// Application developers may need to implement this trait to mock
/// `AppendableObjectWriter`. In other use-cases, application developers
/// should use `AppendableObjectWriter` directly, and need not be concerned
/// with this trait or its implementations.
#[cfg(google_cloud_unstable_storage_bidi)]
pub trait AppendableObjectWriter: std::fmt::Debug + Send + Sync {
    /// The implementation for [AppendableObjectWriter::append][crate::appendable_object_writer::AppendableObjectWriter::append].
    fn append(
        &mut self,
        chunk: Bytes,
    ) -> impl std::future::Future<Output = crate::Result<()>> + Send;

    /// The implementation for [AppendableObjectWriter::flush][crate::appendable_object_writer::AppendableObjectWriter::flush].
    fn flush(&mut self) -> impl std::future::Future<Output = crate::Result<i64>> + Send;

    /// The implementation for [AppendableObjectWriter::finalize][crate::appendable_object_writer::AppendableObjectWriter::finalize].
    fn finalize(
        self,
    ) -> impl std::future::Future<Output = crate::Result<crate::model::Object>> + Send;

    /// The implementation for [AppendableObjectWriter::close][crate::appendable_object_writer::AppendableObjectWriter::close].
    fn close(self) -> impl std::future::Future<Output = crate::Result<i64>> + Send;

    /// The implementation for [AppendableObjectWriter::generation][crate::appendable_object_writer::AppendableObjectWriter::generation].
    fn generation(&self) -> i64;

    /// The implementation for [AppendableObjectWriter::persisted_size][crate::appendable_object_writer::AppendableObjectWriter::persisted_size].
    fn persisted_size(&self) -> i64;
}

async fn unimplemented_stub<T>() -> google_cloud_gax::Result<T> {
    unimplemented!("{UNIMPLEMENTED}");
}
