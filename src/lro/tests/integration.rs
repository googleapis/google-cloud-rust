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
mod fake;

#[cfg(test)]
mod test {
    use super::fake::library::client;
    use super::fake::library::model;
    use super::fake::responses;
    use super::fake::service::*;
    use google_cloud_lro as lro;
    use lro::Poller;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    async fn new_client(endpoint: String) -> gax::Result<client::Client> {
        client::Client::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_endpoint(endpoint)
            .build()
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_immediate_success() -> TestResult {
        let create = vec![responses::success("op/001", "p/test-p/r/r-001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let response = client
            .create_resource("test-p", "r-001")
            .poller()
            .until_done()
            .await?;
        assert_eq!(
            response,
            model::Resource {
                name: "p/test-p/r/r-001".into()
            }
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_immediate_error() -> TestResult {
        let create = vec![responses::operation_error("op/001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let result = client
            .create_resource("test-p", "r-001")
            .poller()
            .until_done()
            .await;
        let error = result.err().unwrap();
        assert_eq!(error.kind(), gax::error::ErrorKind::Other);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_success() -> TestResult {
        let create = vec![responses::pending("op/001", 25)?];
        let poll = vec![
            responses::pending("op/001", 75)?,
            responses::success("op/001", "p/test-p/r/r-001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let response = client
            .create_resource("test-p", "r-001")
            .poller()
            .until_done()
            .await?;
        assert_eq!(
            response,
            model::Resource {
                name: "p/test-p/r/r-001".to_string()
            }
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_error() -> TestResult {
        let create = vec![responses::pending("op/001", 25)?];
        let poll = vec![
            responses::pending("op/001", 75)?,
            responses::operation_error("op/001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let result = client
            .create_resource("test-p", "r-001")
            .poller()
            .until_done()
            .await;
        let error = result.err().unwrap();
        assert_eq!(error.kind(), gax::error::ErrorKind::Other);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_immediate_success() -> TestResult {
        let create = vec![responses::success("op/001", "p/test-p/r/r-001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let mut poller = client.create_resource("test-p", "r-001").poller();
        while let Some(status) = poller.poll().await {
            match status {
                lro::PollingResult::InProgress(_) => {
                    assert!(false, "unexpected InProgress {status:?}")
                }
                lro::PollingResult::PollingError(_) => { /* ignored */ }
                lro::PollingResult::Completed(result) => {
                    let response = result?;
                    assert_eq!(
                        response,
                        model::Resource {
                            name: "p/test-p/r/r-001".into()
                        }
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_immediate_error() -> TestResult {
        let create = vec![responses::operation_error("op/001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let mut poller = client.create_resource("test-p", "r-001").poller();
        while let Some(status) = poller.poll().await {
            match status {
                lro::PollingResult::InProgress(_) => {
                    assert!(false, "unexpected InProgress {status:?}")
                }
                lro::PollingResult::PollingError(_) => { /* ignored */ }
                lro::PollingResult::Completed(result) => {
                    let response = result;
                    assert!(response.is_err(), "{response:?}");
                    let error = response.err().unwrap();
                    assert_eq!(error.kind(), gax::error::ErrorKind::Other);
                }
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_success() -> TestResult {
        let create = vec![responses::pending("op/001", 25)?];
        let poll = vec![
            responses::pending("op/001", 75)?,
            responses::success("op/001", "p/test-p/r/r-001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let mut poller = client.create_resource("test-p", "r-001").poller();
        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(model::CreateResourceMetadata { percent: 25 }))
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(model::CreateResourceMetadata { percent: 75 }))
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        assert!(
            matches!(&status, lro::PollingResult::Completed(_)),
            "{status:?}"
        );
        let response = match status {
            lro::PollingResult::Completed(r) => r.ok(),
            _ => None,
        };
        assert_eq!(
            response,
            Some(model::Resource {
                name: "p/test-p/r/r-001".to_string()
            })
        );

        let status = poller.poll().await;
        assert!(status.is_none(), "{status:?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_error() -> TestResult {
        let create = vec![responses::pending("op/001", 25)?];
        let poll = vec![
            responses::pending("op/001", 75)?,
            responses::operation_error("op/001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let mut poller = client.create_resource("test-p", "r-001").poller();
        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(model::CreateResourceMetadata { percent: 25 }))
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(model::CreateResourceMetadata { percent: 75 }))
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        assert!(
            matches!(&status, lro::PollingResult::Completed(_)),
            "{status:?}"
        );
        let error = match status {
            lro::PollingResult::Completed(r) => r.err(),
            _ => None,
        };
        let error = error.unwrap();
        assert_eq!(error.kind(), gax::error::ErrorKind::Other);

        let status = poller.poll().await;
        assert!(status.is_none(), "{status:?}");

        Ok(())
    }

    // The manual tests are here to validate all the test infrastructure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_immediate_success() -> TestResult {
        let create = vec![responses::success("op/001", "p/test-p/r/r-001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let op = client.create_resource("test-p", "r-001").send().await?;
        assert_eq!(op.name, "op/001", "{op:?}");
        assert!(op.done, "{op:?}");

        let metadata = op
            .metadata
            .map(|any| any.try_into_message::<model::CreateResourceMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(model::CreateResourceMetadata { percent: 100 })
        );

        use longrunning::model::operation;
        match op.result.unwrap() {
            operation::Result::Error(e) => assert!(false, "unexpected error {e:?}"),
            operation::Result::Response(any) => {
                let response = any.try_into_message::<model::Resource>()?;
                assert_eq!(
                    response,
                    model::Resource {
                        name: "p/test-p/r/r-001".into()
                    }
                );
            }
            _ => panic!("longrunning::model::operation::Result has an unexpected branch"),
        };

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_success() -> TestResult {
        let create = vec![responses::pending("op/001", 25)?];
        let poll = vec![
            responses::pending("op/001", 50)?,
            responses::success("op/001", "p/test-p/r/r-001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })
        .await?;

        let client = new_client(endpoint).await?;
        let op = client.create_resource("test-p", "r-001").send().await?;
        assert_eq!(op.name, "op/001", "{op:?}");
        assert_eq!(op.done, false, "{op:?}");

        let metadata = op
            .metadata
            .map(|any| any.try_into_message::<model::CreateResourceMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(model::CreateResourceMetadata { percent: 25 })
        );

        let name = op.name;

        let op = client.get_operation(&name).send().await?;
        assert_eq!(op.name, "op/001", "{op:?}");
        assert_eq!(op.done, false, "{op:?}");
        let metadata = op
            .metadata
            .map(|any| any.try_into_message::<model::CreateResourceMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(model::CreateResourceMetadata { percent: 50 })
        );

        let op = client.get_operation(&name).send().await?;
        assert_eq!(op.name, "op/001", "{op:?}");
        assert_eq!(op.done, true, "{op:?}");
        let metadata = op
            .metadata
            .map(|any| any.try_into_message::<model::CreateResourceMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(model::CreateResourceMetadata { percent: 100 })
        );

        use longrunning::model::operation;
        match op.result.unwrap() {
            operation::Result::Error(e) => assert!(false, "unexpected error {e:?}"),
            operation::Result::Response(any) => {
                let response = any.try_into_message::<model::Resource>()?;
                assert_eq!(
                    response,
                    model::Resource {
                        name: "p/test-p/r/r-001".into()
                    }
                );
            }
            _ => panic!("longrunning::model::operation::Result has an unexpected branch"),
        };

        Ok(())
    }
}
