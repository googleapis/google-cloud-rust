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

//! Verify it is possible to mock pagination APIs.

#[cfg(test)]
mod mocking {
    use google_cloud_gax::Result as GaxResult;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::paginator::{ItemPaginator, Paginator};
    use google_cloud_gax::response::Response as GaxResponse;
    use google_cloud_secretmanager_v1::client::SecretManagerService;
    use google_cloud_secretmanager_v1::model::{ListSecretsRequest, ListSecretsResponse, Secret};
    use google_cloud_secretmanager_v1::stub::SecretManagerService as Stub;

    mockall::mock! {
        #[derive(Debug)]
        SecretManagerService {}
        impl Stub for SecretManagerService {
            async fn list_secrets(&self, req: ListSecretsRequest, _options: RequestOptions) -> GaxResult<GaxResponse<ListSecretsResponse>>;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn paginators_are_send() -> anyhow::Result<()> {
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 0)),
                ))
            });

        async fn other_task(
            client: SecretManagerService,
        ) -> anyhow::Result<Vec<ListSecretsResponse>> {
            let mut pages = client
                .list_secrets()
                .set_parent("projects/test-project")
                .by_page();
            let mut responses = Vec::new();
            while let Some(response) = pages.next().await {
                responses.push(response?);
            }
            Ok(responses)
        }

        let client = SecretManagerService::from_stub(mock);
        let join = tokio::spawn(async move { other_task(client).await });
        let responses = join.await??;
        assert_eq!(
            responses,
            [ListSecretsResponse::default().set_secrets(make_secrets(3, 0))]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn item_paginators_are_send() -> anyhow::Result<()> {
        use ListSecretsResponse;
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 0)),
                ))
            });

        async fn other_task(client: SecretManagerService) -> GaxResult<Vec<Secret>> {
            let mut paginator = client
                .list_secrets()
                .set_parent("projects/test-project")
                .by_item();
            let mut responses = Vec::new();
            while let Some(response) = paginator.next().await {
                responses.push(response?);
            }
            Ok(responses)
        }

        let client = SecretManagerService::from_stub(mock);
        let join = tokio::spawn(async move { other_task(client).await });
        let responses = join.await??;
        assert_eq!(responses, make_secrets(3, 0));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_pages() -> anyhow::Result<()> {
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = SecretManagerService::from_stub(mock);
        let mut paginator = client
            .list_secrets()
            .set_parent("projects/test-project")
            .by_page();
        let mut responses = Vec::new();
        while let Some(response) = paginator.next().await {
            responses.push(response?);
        }

        assert_eq!(
            responses,
            [
                ListSecretsResponse::default()
                    .set_next_page_token("test-page-001")
                    .set_secrets(make_secrets(3, 0)),
                ListSecretsResponse::default()
                    .set_next_page_token("test-page-002")
                    .set_secrets(make_secrets(3, 3)),
                ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
            ]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_pages_as_stream() -> anyhow::Result<()> {
        use futures::stream::StreamExt;

        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = SecretManagerService::from_stub(mock);
        let mut paginator = client
            .list_secrets()
            .set_parent("projects/test-project")
            .by_page()
            .into_stream();
        let mut responses = Vec::new();
        while let Some(response) = paginator.next().await {
            responses.push(response?);
        }

        assert_eq!(
            responses,
            [
                ListSecretsResponse::default()
                    .set_next_page_token("test-page-001")
                    .set_secrets(make_secrets(3, 0)),
                ListSecretsResponse::default()
                    .set_next_page_token("test-page-002")
                    .set_secrets(make_secrets(3, 3)),
                ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
            ]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_items() -> anyhow::Result<()> {
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = SecretManagerService::from_stub(mock);
        let mut paginator = client
            .list_secrets()
            .set_parent("projects/test-project")
            .by_item();
        let mut names = Vec::new();
        while let Some(secret) = paginator.next().await {
            names.push(secret?.name);
        }

        assert_eq!(
            names,
            make_secrets(9, 0)
                .into_iter()
                .map(|s| s.name)
                .collect::<Vec<String>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn list_items_as_stream() -> anyhow::Result<()> {
        use futures::stream::StreamExt;

        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(GaxResponse::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = SecretManagerService::from_stub(mock);
        let mut stream = client
            .list_secrets()
            .set_parent("projects/test-project")
            .by_item()
            .into_stream();
        let mut names = Vec::new();
        while let Some(secret) = stream.next().await {
            names.push(secret?.name);
        }

        assert_eq!(
            names,
            make_secrets(9, 0)
                .into_iter()
                .map(|s| s.name)
                .collect::<Vec<String>>()
        );

        Ok(())
    }

    fn make_secrets(count: i32, start: i32) -> Vec<Secret> {
        (start..(start + count))
            .map(|v| Secret::default().set_name(format!("projects/test-project/secrets/{v}")))
            .collect()
    }
}
