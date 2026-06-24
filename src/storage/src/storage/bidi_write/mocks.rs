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

// TODO(#5716): Lift to shared bidi module

use crate::google::storage::v2::BidiWriteObjectResponse;
use gaxi::grpc::tonic::Result as TonicResult;
use tokio::sync::mpsc::Sender;

#[allow(dead_code)]
pub type MockStream = tokio::sync::mpsc::Receiver<TonicResult<BidiWriteObjectResponse>>;
#[allow(dead_code)]
pub type MockStreamSender = Sender<TonicResult<BidiWriteObjectResponse>>;

#[allow(dead_code)]
pub fn mock_stream() -> (MockStreamSender, MockStream) {
    tokio::sync::mpsc::channel(10)
}
