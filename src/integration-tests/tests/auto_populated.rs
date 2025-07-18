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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use gax::error::Error;
    use gax::paginator::Paginator;
    use std::collections::HashSet;

    mockall::mock! {
        #[derive(Debug)]
        StorageControl {}
        impl storage::stub::StorageControl for StorageControl {
            async fn create_folder(&self, req: storage::model::CreateFolderRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<storage::model::Folder>>;
            async fn list_anywhere_caches(&self, req: storage::model::ListAnywhereCachesRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<storage::model::ListAnywhereCachesResponse>>;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn one_request_id_per_retry_loop() -> Result<()> {
        let mut mock = MockStorageControl::new();
        mock.expect_create_folder()
            .once()
            // The retry loop lives within a stub. If this stub is given a
            // request ID, it must be set for the entire loop.
            .withf(|r, _| !r.request_id.is_empty())
            .return_once(|_, _| Err(unavailable()));

        let client = storage::client::StorageControl::from_stub(mock);
        let _ = client.create_folder().send().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn pagination() -> Result<()> {
        // Each subsequent request should have a new request ID.
        let seen = std::sync::Arc::new(std::sync::Mutex::new(HashSet::new()));

        let mut mock = MockStorageControl::new();
        let mut seq = mockall::Sequence::new();
        let page_tokens = ["", "page-001", "page-002", "page-003", ""];
        for i in 1..page_tokens.len() {
            let current = page_tokens[i - 1];
            let next = page_tokens[i];
            let seen_clone = seen.clone();

            mock.expect_list_anywhere_caches()
                .once()
                .in_sequence(&mut seq)
                .withf(move |r, _| r.page_token == current)
                .return_once(move |r, _| {
                    tracing::info!("attempt={i}, request ID={}", r.request_id);
                    assert!(
                        seen_clone.lock().unwrap().insert(r.request_id),
                        "Request ID repeated for a request with different contents."
                    );
                    Ok(gax::response::Response::from(
                        storage::model::ListAnywhereCachesResponse::default()
                            .set_next_page_token(next),
                    ))
                });
        }

        let client = storage::client::StorageControl::from_stub(mock);
        let mut paginator = client.list_anywhere_caches().by_page();
        while paginator.next().await.transpose()?.is_some() {}

        // Just to be overly cautious, verify we made N calls, with N different request IDs.
        let seen = seen.lock().unwrap();
        assert!(seen.len() == page_tokens.len() - 1, "{seen:?}");

        Ok(())
    }

    fn unavailable() -> Error {
        use gax::error::rpc::{Code, Status};
        Error::service(
            Status::default()
                .set_code(Code::Unavailable)
                .set_message("try-again"),
        )
    }
}
