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

pub mod fake;

#[cfg(test)]
mod tests {
    use super::fake::responses;
    use super::fake::service::*;
    use anyhow::Result;
    use gax::error::rpc::Code;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_longrunning::model::operation::Result as OperationResult;
    use google_cloud_lro as lro;
    use google_cloud_workflows_v1::client::Workflows;
    use google_cloud_workflows_v1::model::{OperationMetadata, Workflow};
    use lro::Poller;

    async fn new_client(endpoint: String) -> Result<Workflows> {
        let client = Workflows::builder()
            .with_credentials(Anonymous::new().build())
            .with_endpoint(endpoint)
            .build()
            .await?;
        Ok(client)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_is_send() -> Result<()> {
        let create = vec![responses::success(
            "op001",
            "projects/p/locations/l/workflows/w01",
        )?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        async fn task(client: Workflows) -> Result<()> {
            let response = client
                .create_workflow()
                .set_parent("projects/p/locations/l")
                .set_workflow_id("w01")
                .poller()
                .until_done()
                .await?;
            assert_eq!(
                response,
                Workflow::new().set_name("projects/p/locations/l/workflows/w01")
            );
            Ok(())
        }

        let client = new_client(endpoint).await?;
        let join = tokio::spawn(async move { task(client).await });
        join.await??;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_is_send() -> Result<()> {
        let create = vec![responses::success(
            "op001",
            "projects/p/locations/l/workflows/w01",
        )?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        async fn task(client: Workflows) -> Result<()> {
            let mut poller = client
                .create_workflow()
                .set_parent("projects/p/locations/l")
                .set_workflow_id("w01")
                .poller();
            while let Some(status) = poller.poll().await {
                match status {
                    lro::PollingResult::InProgress(_) => {
                        panic!("unexpected InProgress {status:?}")
                    }
                    lro::PollingResult::PollingError(_) => { /* ignored */ }
                    lro::PollingResult::Completed(result) => {
                        let response = result?;
                        assert_eq!(
                            response,
                            Workflow::new().set_name("projects/p/locations/l/workflows/w01")
                        );
                    }
                }
            }
            Ok(())
        }

        let client = new_client(endpoint).await?;
        let join = tokio::spawn(async move { task(client).await });
        join.await??;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_immediate_success() -> Result<()> {
        let create = vec![responses::success(
            "op001",
            "projects/p/locations/l/workflows/w01",
        )?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let response = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller()
            .until_done()
            .await?;
        assert_eq!(
            response,
            Workflow::new().set_name("projects/p/locations/l/workflows/w01")
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_immediate_error() -> Result<()> {
        let create = vec![responses::operation_error("op001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let result = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller()
            .until_done()
            .await;
        let error = result.err().unwrap();
        assert_eq!(error.status().map(|s| s.code), Some(Code::AlreadyExists));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_success() -> Result<()> {
        let create = vec![responses::pending("op001", 25)?];
        let poll = vec![
            responses::pending("op001", 75)?,
            responses::success("op001", "projects/p/locations/l/workflows/w01")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let response = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller()
            .until_done()
            .await?;
        assert_eq!(
            response,
            Workflow::new().set_name("projects/p/locations/l/workflows/w01")
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn until_done_error() -> Result<()> {
        let create = vec![responses::pending("op001", 25)?];
        let poll = vec![
            responses::pending("op001", 75)?,
            responses::operation_error("op001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let result = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller()
            .until_done()
            .await;
        let error = result.err().unwrap();
        assert_eq!(error.status().map(|s| s.code), Some(Code::AlreadyExists));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_immediate_success() -> Result<()> {
        let create = vec![responses::success(
            "op001",
            "projects/p/locations/l/workflows/w01",
        )?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let mut poller = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller();
        while let Some(status) = poller.poll().await {
            match status {
                lro::PollingResult::InProgress(_) => {
                    panic!("unexpected InProgress {status:?}")
                }
                lro::PollingResult::PollingError(_) => { /* ignored */ }
                lro::PollingResult::Completed(result) => {
                    let response = result?;
                    assert_eq!(
                        response,
                        Workflow::new().set_name("projects/p/locations/l/workflows/w01")
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_immediate_error() -> Result<()> {
        let create = vec![responses::operation_error("op001")?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let mut poller = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller();
        while let Some(status) = poller.poll().await {
            match status {
                lro::PollingResult::InProgress(_) => {
                    panic!("unexpected InProgress {status:?}")
                }
                lro::PollingResult::PollingError(_) => { /* ignored */ }
                lro::PollingResult::Completed(Ok(_)) => {
                    panic!("expected a completed polling status with an error {status:?}")
                }
                lro::PollingResult::Completed(Err(error)) => {
                    assert_eq!(error.status().map(|s| s.code), Some(Code::AlreadyExists));
                }
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_success() -> Result<()> {
        let create = vec![responses::pending("op001", 25)?];
        let poll = vec![
            responses::pending("op001", 75)?,
            responses::success("op001", "projects/p/locations/l/workflows/w01")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let mut poller = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller();
        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(m)) if m.target == "percent=25"
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(m)) if m.target == "percent=75"
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
            Some(Workflow::new().set_name("projects/p/locations/l/workflows/w01"))
        );

        let status = poller.poll().await;
        assert!(status.is_none(), "{status:?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poller_error() -> Result<()> {
        let create = vec![responses::pending("op001", 25)?];
        let poll = vec![
            responses::pending("op001", 75)?,
            responses::operation_error("op001")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let mut poller = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .poller();
        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(m)) if m.target == "percent=25"
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        assert!(
            matches!(
                &status,
                lro::PollingResult::InProgress(Some(m)) if m.target == "percent=75"
            ),
            "{status:?}"
        );

        let status = poller.poll().await.unwrap();
        let error = match status {
            lro::PollingResult::Completed(Err(e)) => e,
            _ => panic!("expected a completed polling result with an error {status:?}"),
        };
        assert_eq!(error.status().map(|s| s.code), Some(Code::AlreadyExists));

        let status = poller.poll().await;
        assert!(status.is_none(), "{status:?}");

        Ok(())
    }

    // The manual tests are here to validate all the test infrastructure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_immediate_success() -> Result<()> {
        let create = vec![responses::success(
            "op001",
            "projects/p/locations/l/workflows/w01",
        )?];
        let poll = vec![];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let op = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .send()
            .await?;
        assert!(op.name.ends_with("/operations/op001"), "{op:?}");
        assert!(op.done, "{op:?}");

        let metadata = op
            .metadata
            .map(|any| any.to_msg::<OperationMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(OperationMetadata::new().set_target("percent=100"))
        );

        match op.result.unwrap() {
            OperationResult::Error(e) => panic!("unexpected error {e:?}"),
            OperationResult::Response(any) => {
                let response = any.to_msg::<Workflow>()?;
                assert_eq!(
                    response,
                    Workflow::new().set_name("projects/p/locations/l/workflows/w01")
                );
            }
            _ => panic!("longrunning::model::operation::Result has an unexpected branch"),
        };

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_success() -> Result<()> {
        let create = vec![responses::pending("op001", 25)?];
        let poll = vec![
            responses::pending("op001", 50)?,
            responses::success("op001", "projects/p/locations/l/workflows/w01")?,
        ];
        let (endpoint, _server) = start(ServerState {
            create: create.into(),
            poll: poll.into(),
        })?;

        let client = new_client(endpoint).await?;
        let op = client
            .create_workflow()
            .set_parent("projects/p/locations/l")
            .set_workflow_id("w01")
            .send()
            .await?;
        assert!(op.name.ends_with("/operations/op001"), "{op:?}");
        assert!(!op.done, "{op:?}");

        let metadata = op
            .metadata
            .map(|any| any.to_msg::<OperationMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(OperationMetadata::new().set_target("percent=25"))
        );

        let name = op.name;

        let op = client.get_operation().set_name(&name).send().await?;
        assert!(op.name.ends_with("/operations/op001"), "{op:?}");
        assert!(!op.done, "{op:?}");
        let metadata = op
            .metadata
            .map(|any| any.to_msg::<OperationMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(OperationMetadata::new().set_target("percent=50"))
        );

        let op = client.get_operation().set_name(&name).send().await?;
        assert!(op.name.ends_with("/operations/op001"), "{op:?}");
        assert!(op.done, "{op:?}");
        let metadata = op
            .metadata
            .map(|any| any.to_msg::<OperationMetadata>())
            .transpose()?;
        assert_eq!(
            metadata,
            Some(OperationMetadata::new().set_target("percent=100"))
        );

        match op.result.unwrap() {
            OperationResult::Error(e) => panic!("unexpected error {e:?}"),
            OperationResult::Response(any) => {
                let response = any.to_msg::<Workflow>()?;
                assert_eq!(
                    response,
                    Workflow::new().set_name("projects/p/locations/l/workflows/w01")
                );
            }
            _ => panic!("longrunning::model::operation::Result has an unexpected branch"),
        };

        Ok(())
    }
}
