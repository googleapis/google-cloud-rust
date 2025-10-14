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
//!
//! Occasionally, a Google Cloud service may need to expose a method that takes
//! a significant amount of time to complete. In these situations, it is often
//! a poor user experience to simply block while the task runs. Such services
//! return a long-running operation, a type of promise that can be polled until
//! it completes successfully.
//!
//! Polling these operations can be tedious. The application needs to
//! periodically make RPCs, extract the result from the response, and may need
//! to implement a stream to return metadata representing any progress in the
//! RPC.
//!
//! The Google Cloud client libraries for Rust return implementations of this
//! trait to simplify working with these long-running operations.
//!
//! # Example: automatically poll until completion
//! ```no_run
//! # use google_cloud_lro::{internal::Operation, Poller};
//! # use serde::{Deserialize, Serialize};
//! # use gax::Result;
//! # use wkt::Timestamp as Response;
//! # use wkt::Duration as Metadata;
//! async fn start_lro() -> impl Poller<Response, Metadata> {
//!     // ... details omitted ...
//!     # async fn start() -> Result<Operation<Response, Metadata>> { panic!(); }
//!     # async fn query(_: String) -> Result<Operation<Response, Metadata>> { panic!(); }
//!     # google_cloud_lro::internal::new_poller(
//!     #    std::sync::Arc::new(gax::polling_error_policy::AlwaysContinue),
//!     #    std::sync::Arc::new(gax::exponential_backoff::ExponentialBackoff::default()),
//!     #    start, query
//!     # )
//! }
//! # tokio_test::block_on(async {
//! let response = start_lro()
//!     .await
//!     .until_done()
//!     .await?;
//! println!("response = {response:?}");
//! # gax::Result::<()>::Ok(()) });
//! ```
//!
//! # Example: poll with metadata
//! ```no_run
//! # use google_cloud_lro::{internal::Operation, Poller, PollingResult};
//! # use serde::{Deserialize, Serialize};
//! # use gax::Result;
//! # use wkt::Timestamp as Response;
//! # use wkt::Duration as Metadata;
//!
//! async fn start_lro() -> impl Poller<Response, Metadata> {
//!     // ... details omitted ...
//!     # async fn start() -> Result<Operation<Response, Metadata>> { panic!(); }
//!     # async fn query(_: String) -> Result<Operation<Response, Metadata>> { panic!(); }
//!     # google_cloud_lro::internal::new_poller(
//!     #    std::sync::Arc::new(gax::polling_error_policy::AlwaysContinue),
//!     #    std::sync::Arc::new(gax::exponential_backoff::ExponentialBackoff::default()),
//!     #    start, query
//!     # )
//! }
//! # tokio_test::block_on(async {
//! let mut poller = start_lro().await;
//! while let Some(p) = poller.poll().await {
//!     match p {
//!         PollingResult::Completed(r) => { println!("LRO completed, response={r:?}"); }
//!         PollingResult::InProgress(m) => { println!("LRO in progress, metadata={m:?}"); }
//!         PollingResult::PollingError(e) => { println!("Transient error polling the LRO: {e}"); }
//!     }
//!     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//! }
//! # gax::Result::<()>::Ok(()) });
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]

use gax::Result;
use gax::error::Error;
use gax::polling_backoff_policy::PollingBackoffPolicy;
use gax::polling_error_policy::PollingErrorPolicy;
use std::future::Future;

/// The result of polling a Long-Running Operation (LRO).
///
/// # Parameters
/// * `ResponseType` - This is the type returned when the LRO completes
///   successfully.
/// * `MetadataType` - The LRO may return values of this type while the
///   operation is in progress. This may include some measure of "progress".
#[derive(Debug)]
pub enum PollingResult<ResponseType, MetadataType> {
    /// The operation is still in progress.
    InProgress(Option<MetadataType>),
    /// The operation completed. This includes the result.
    Completed(Result<ResponseType>),
    /// An error trying to poll the LRO.
    ///
    /// Not all errors indicate that the operation failed. For example, this
    /// may fail because it was not possible to connect to Google Cloud. Such
    /// transient errors may disappear in the next polling attempt.
    ///
    /// Other errors will never recover. For example, a [Error] with
    /// a [NOT_FOUND], [ABORTED], or [PERMISSION_DENIED] status code will never
    /// recover.
    ///
    /// [Error]: gax::error::Error
    /// [NOT_FOUND]: rpc::model::Code::NotFound
    /// [ABORTED]: rpc::model::Code::Aborted
    /// [PERMISSION_DENIED]: rpc::model::Code::PermissionDenied
    PollingError(Error),
}

#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
pub mod internal;

pub(crate) mod sealed {
    pub trait Poller {}
}

/// Automatically polls long-running operations.
///
/// # Parameters
/// * `ResponseType` - This is the type returned when the LRO completes
///   successfully.
/// * `MetadataType` - The LRO may return values of this type while the
///   operation is in progress. This may include some measure of "progress".
pub trait Poller<ResponseType, MetadataType>: Send + sealed::Poller {
    /// Query the current status of the long-running operation.
    fn poll(
        &mut self,
    ) -> impl Future<Output = Option<PollingResult<ResponseType, MetadataType>>> + Send;

    /// Poll the long-running operation until it completes.
    fn until_done(self) -> impl Future<Output = Result<ResponseType>> + Send;

    /// Convert a poller to a [Stream][futures::Stream].
    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    fn into_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin;
}

mod details;
