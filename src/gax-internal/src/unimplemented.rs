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

use google_cloud_gax::Result;
use google_cloud_gax::response::Response;
#[cfg(google_cloud_unstable_gapic_streaming)]
use google_cloud_gax::streaming::{RequestSender, ResponseReceiver};

pub const UNIMPLEMENTED: &str = concat!(
    "to prevent breaking changes as services gain new RPCs, the stub ",
    "traits provide default implementations of each method. In the client ",
    "libraries, all implementations of the traits override all methods. ",
    "Therefore, this error should not appear in normal code using the ",
    "client libraries. The only expected context for this error is test ",
    "code mocking the client libraries. If that is how you got this ",
    "error, verify that you have mocked all methods used in your test. ",
    "Otherwise, please open a bug at ",
    "https://github.com/googleapis/google-cloud-rust/issues"
);

pub async fn unimplemented_stub<T: Send>() -> Result<Response<T>> {
    unimplemented!("{UNIMPLEMENTED}");
}

#[cfg(google_cloud_unstable_gapic_streaming)]
pub async fn unimplemented_bidi_stub<I: Send, O: Send>()
-> Result<(RequestSender<I>, ResponseReceiver<O>)> {
    unimplemented!("{UNIMPLEMENTED}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[should_panic(expected = "to prevent breaking changes as services gain new RPCs")]
    async fn test_unimplemented_stub() {
        let _ = unimplemented_stub::<()>().await;
    }

    #[cfg(google_cloud_unstable_gapic_streaming)]
    #[tokio::test]
    #[should_panic(expected = "to prevent breaking changes as services gain new RPCs")]
    async fn test_unimplemented_bidi_stub() {
        let _ = unimplemented_bidi_stub::<(), ()>().await;
    }
}
