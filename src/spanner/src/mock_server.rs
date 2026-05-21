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

use spanner_grpc_mock::MockSpanner;

#[allow(clippy::disallowed_methods)]
pub async fn start_panic_safe_spanner_mock(
    address: &str,
    mock: MockSpanner,
) -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
    let (address, handle) = spanner_grpc_mock::start(address, mock).await?;
    let wrapper = tokio::spawn(async move {
        if let Err(e) = handle.await {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            }
        }
    });
    Ok((address, wrapper))
}
