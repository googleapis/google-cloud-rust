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

#[cfg(test)]
mod mocking {
    use gax::paginator::{ItemPaginator, Paginator};
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    mockall::mock! {
        #[derive(Debug)]
        SecretManagerService {}
        impl sm::stub::SecretManagerService for SecretManagerService {
            async fn list_secrets(&self, req: sm::model::ListSecretsRequest, _options: gax::options::RequestOptions) -> gax::Result<gax::response::Response<sm::model::ListSecretsResponse>>;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn paginators_are_send() -> TestResult {
        use sm::model::ListSecretsResponse;
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 0)),
                ))
            });

        async fn other_task(
            client: sm::client::SecretManagerService,
        ) -> gax::Result<Vec<ListSecretsResponse>> {
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

        let client = sm::client::SecretManagerService::from_stub(mock);
        let join = tokio::spawn(async move { other_task(client).await });
        let responses = join.await??;
        assert_eq!(
            responses,
            [ListSecretsResponse::default().set_secrets(make_secrets(3, 0))]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn item_paginators_are_send() -> TestResult {
        use sm::model::ListSecretsResponse;
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    ListSecretsResponse::default().set_secrets(make_secrets(3, 0)),
                ))
            });

        async fn other_task(
            client: sm::client::SecretManagerService,
        ) -> gax::Result<Vec<sm::model::Secret>> {
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

        let client = sm::client::SecretManagerService::from_stub(mock);
        let join = tokio::spawn(async move { other_task(client).await });
        let responses = join.await??;
        assert_eq!(responses, make_secrets(3, 0));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_pages() -> TestResult {
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = sm::client::SecretManagerService::from_stub(mock);
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
                sm::model::ListSecretsResponse::default()
                    .set_next_page_token("test-page-001")
                    .set_secrets(make_secrets(3, 0)),
                sm::model::ListSecretsResponse::default()
                    .set_next_page_token("test-page-002")
                    .set_secrets(make_secrets(3, 3)),
                sm::model::ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
            ]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_pages_as_stream() -> TestResult {
        use futures::stream::StreamExt;

        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = sm::client::SecretManagerService::from_stub(mock);
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
                sm::model::ListSecretsResponse::default()
                    .set_next_page_token("test-page-001")
                    .set_secrets(make_secrets(3, 0)),
                sm::model::ListSecretsResponse::default()
                    .set_next_page_token("test-page-002")
                    .set_secrets(make_secrets(3, 3)),
                sm::model::ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
            ]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_items() -> TestResult {
        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = sm::client::SecretManagerService::from_stub(mock);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn list_items_as_stream() -> TestResult {
        use futures::stream::StreamExt;

        let mut mock = MockSecretManagerService::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token.is_empty())
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-001")
                        .set_secrets(make_secrets(3, 0)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-001")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default()
                        .set_next_page_token("test-page-002")
                        .set_secrets(make_secrets(3, 3)),
                ))
            });
        mock.expect_list_secrets()
            .once()
            .in_sequence(&mut seq)
            .withf(|r, _| r.parent == "projects/test-project" && r.page_token == "test-page-002")
            .return_once(|_, _| {
                Ok(gax::response::Response::from(
                    sm::model::ListSecretsResponse::default().set_secrets(make_secrets(3, 6)),
                ))
            });

        let client = sm::client::SecretManagerService::from_stub(mock);
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

    fn make_secrets(count: i32, start: i32) -> Vec<sm::model::Secret> {
        (start..(start + count))
            .map(|v| {
                sm::model::Secret::default().set_name(format!("projects/test-project/secrets/{v}"))
            })
            .collect()
    }
}
