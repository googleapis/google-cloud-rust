// Copyright 2024 Google LLC
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

// Use separate modules to keep the file sizes under control and avoid name
// clashes. We prefer to have a single driver program because cargo runs all
// the tests within one program in parallel.
pub mod secret_manager_openapi;
pub mod secret_manager_protobuf;

#[cfg(all(test, feature = "run-integration-tests"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn run_secretmanager_protobuf() -> integration_tests::Result<()> {
    secret_manager_protobuf::run().await
}

#[cfg(all(test, feature = "run-integration-tests"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn run_secretmanager_openapi() -> integration_tests::Result<()> {
    secret_manager_openapi::run().await
}
