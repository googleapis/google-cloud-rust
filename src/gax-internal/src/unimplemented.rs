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

pub async fn unimplemented_stub<T: Send>() -> gax::Result<gax::response::Response<T>> {
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
