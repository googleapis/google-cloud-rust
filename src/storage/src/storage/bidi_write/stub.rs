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

#[cfg(google_cloud_unstable_storage_bidi)]
pub(crate) mod dynamic {
    use bytes::Bytes;

    /// An object-safe (Boxable) dynamic trait for AppendableObjectWriter.
    #[async_trait::async_trait]
    pub trait AppendableObjectWriter: std::fmt::Debug + Send + Sync {
        async fn append(&mut self, chunk: Bytes) -> crate::Result<()>;
        async fn flush(&mut self) -> crate::Result<i64>;
        async fn finalize(self: Box<Self>) -> crate::Result<crate::model::Object>;
        async fn close(self: Box<Self>) -> crate::Result<i64>;
        fn generation(&self) -> i64;
        fn persisted_size(&self) -> i64;
    }

    #[async_trait::async_trait]
    impl<T: crate::stub::AppendableObjectWriter> AppendableObjectWriter for T {
        async fn append(&mut self, chunk: Bytes) -> crate::Result<()> {
            T::append(self, chunk).await
        }

        async fn flush(&mut self) -> crate::Result<i64> {
            T::flush(self).await
        }

        async fn finalize(self: Box<Self>) -> crate::Result<crate::model::Object> {
            T::finalize(*self).await
        }

        async fn close(self: Box<Self>) -> crate::Result<i64> {
            T::close(*self).await
        }

        fn generation(&self) -> i64 {
            T::generation(self)
        }

        fn persisted_size(&self) -> i64 {
            T::persisted_size(self)
        }
    }
}
