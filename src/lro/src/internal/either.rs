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

#[cfg(feature = "unstable-stream")]
use crate::PollingResult;
use crate::{Poller, Result, sealed};
use google_cloud_gax::polling_state::PollingState;

/// Combine two different `Poller` types into a single type.
#[derive(Clone, Debug)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A, B> sealed::Poller for Either<A, B> {}

impl<A, B, ResponseType, MetadataType> Poller<ResponseType, MetadataType> for Either<A, B>
where
    A: Poller<ResponseType, MetadataType>,
    B: Poller<ResponseType, MetadataType>,
    ResponseType: Send,
    MetadataType: Send,
{
    async fn poll(&mut self) -> Option<crate::PollingResult<ResponseType, MetadataType>> {
        match self {
            Self::Left(s) => s.poll().await,
            Self::Right(s) => s.poll().await,
        }
    }
    async fn backoff(&mut self, state: &PollingState) {
        match self {
            Self::Left(s) => s.backoff(state).await,
            Self::Right(s) => s.backoff(state).await,
        }
    }
    async fn until_done(self) -> Result<ResponseType> {
        crate::until_done(self).await
    }

    #[cfg(feature = "unstable-stream")]
    fn into_stream(
        self,
    ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin {
        crate::into_stream(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PollingResult;
    use google_cloud_wkt::{Duration, Timestamp};
    use mockall::mock;

    type ResponseType = Duration;
    type MetadataType = Timestamp;

    mock! {
        PollerA {}
        impl sealed::Poller for PollerA {}
        impl Poller<ResponseType, MetadataType> for PollerA {
            async fn poll(&mut self) -> Option<PollingResult<ResponseType, MetadataType>>;
            async fn backoff(&mut self, state: &PollingState);
            async fn until_done(self) -> google_cloud_gax::Result<ResponseType>;
            #[cfg(feature = "unstable-stream")]
            fn into_stream(
                self,
            ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin;
        }
    }
    mock! {
        PollerB {}
        impl sealed::Poller for PollerB {}
        impl Poller<ResponseType, MetadataType> for PollerB {
            async fn poll(&mut self) -> Option<PollingResult<ResponseType, MetadataType>>;
            async fn backoff(&mut self, state: &PollingState);
            async fn until_done(self) -> google_cloud_gax::Result<ResponseType>;
            #[cfg(feature = "unstable-stream")]
            fn into_stream(
                self,
            ) -> impl futures::Stream<Item = PollingResult<ResponseType, MetadataType>> + Unpin;
        }
    }

    #[tokio::test]
    async fn test_either_poller_left() {
        let mut mock = MockPollerA::new();
        mock.expect_poll().times(1).returning(|| None);
        mock.expect_backoff().times(1).returning(|_| ());

        let mut poller: Either<MockPollerA, MockPollerB> = Either::Left(mock);

        poller.poll().await;
        poller.backoff(&PollingState::default()).await;
    }

    #[tokio::test]
    async fn test_either_poller_right() {
        let mut mock = MockPollerB::new();
        mock.expect_poll().times(1).returning(|| None);
        mock.expect_backoff().times(1).returning(|_| ());

        let mut poller: Either<MockPollerA, MockPollerB> = Either::Right(mock);

        poller.poll().await;
        poller.backoff(&PollingState::default()).await;
    }

    #[tokio::test]
    async fn test_return_impl_base_poller() {
        fn factory(use_a: bool) -> impl Poller<ResponseType, MetadataType> {
            if use_a {
                Either::Left(MockPollerA::new())
            } else {
                Either::Right(MockPollerB::new())
            }
        }

        let _poller_a = factory(true);
        let _poller_b = factory(false);
    }
}
