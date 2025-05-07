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

//! Types and functions to make LROs easier to use and to require less boilerplate.

use gax::Result;
use gax::error::Error;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::PollingErrorPolicy;
use std::future::Future;

/// The result of polling a Long-Running Operation (LRO).
///
/// # Parameters
/// * `R` - the response type. This is the type returned when the LRO completes
///   successfully.
/// * `M` - the metadata type. While operations are in progress the LRO may
///   return values of this type.
#[derive(Debug)]
pub enum PollingResult<R, M> {
    /// The operation is still in progress.
    InProgress(Option<M>),
    /// The operation completed. This includes the result.
    Completed(Result<R>),
    /// An error trying to poll the LRO.
    ///
    /// Not all errors indicate that the operation failed. For example, this
    /// may fail because it was not possible to connect to Google Cloud. Such
    /// transient errors may disappear in the next polling attempt.
    ///
    /// Other errors will never recover. For example, a [ServiceError] with
    /// a [NOT_FOUND], [ABORTED], or [PERMISSION_DENIED] code will never
    /// recover.
    ///
    /// [ServiceError]: gax::error::ServiceError
    /// [NOT_FOUND]: rpc::model::Code::NotFound
    /// [ABORTED]: rpc::model::Code::Aborted
    /// [PERMISSION_DENIED]: rpc::model::Code::PermissionDenied
    PollingError(Error),
}

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
pub mod internal;

mod sealed {
    pub trait Poller {}
}

/// The trait implemented by LRO helpers.
///
/// # Parameters
/// * `R` - the response type, that is, the type of response included when the
///   long-running operation completes successfully.
/// * `M` - the metadata type, that is, the type returned by the service when
///   the long-running operation is still in progress.
pub trait Poller<R, M>: Send + sealed::Poller {
    /// Query the current status of the long-running operation.
    fn poll(&mut self) -> impl Future<Output = Option<PollingResult<R, M>>> + Send;

    /// Poll the long-running operation until it completes.
    fn until_done(self) -> impl Future<Output = Result<R>> + Send;

    /// Convert a poller to a [futures::Stream].
    #[cfg(feature = "unstable-stream")]
    fn into_stream(self) -> impl futures::Stream<Item = PollingResult<R, M>>;
}

mod details;
