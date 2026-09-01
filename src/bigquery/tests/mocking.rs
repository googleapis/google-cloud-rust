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

#[cfg(test)]
mod tests {
    use google_cloud_bigquery::client::BigQuery;
    use google_cloud_bigquery::stub::JobService;
    use google_cloud_bigquery_v2::model::{
        GetJobRequest, GetQueryResultsRequest, GetQueryResultsResponse, InsertJobRequest, Job,
        JobConfiguration, JobConfigurationQuery, JobReference, PostQueryRequest, QueryResponse,
        TableFieldSchema, TableSchema,
    };
    use google_cloud_gax::Result as GaxResult;
    use google_cloud_gax::error::{
        Error,
        rpc::{Code, Status},
    };
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;
    use mockall::mock;
    use serde_json::{Map, json};
    use std::sync::Arc;

    fn create_test_row(val: &str) -> wkt::Struct {
        Map::from_iter([("f".to_string(), json!([{ "v": val }]))])
    }

    mock! {
        #[derive(Debug)]
        JobService {}
        impl JobService for JobService {
            async fn query(
                &self,
                req: PostQueryRequest,
                options: RequestOptions,
            ) -> GaxResult<Response<QueryResponse>>;

            async fn get_query_results(
                &self,
                req: GetQueryResultsRequest,
                options: RequestOptions,
            ) -> GaxResult<Response<GetQueryResultsResponse>>;

            async fn get_job(
                &self,
                req: GetJobRequest,
                options: RequestOptions,
            ) -> GaxResult<Response<Job>>;

            async fn insert_job(
                &self,
                req: InsertJobRequest,
                options: RequestOptions,
            ) -> GaxResult<Response<Job>>;
        }
    }

    #[tokio::test]
    async fn mock_query_success() -> anyhow::Result<()> {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|req, _| {
            assert_eq!(req.project_id, "test-project");
            assert_eq!(
                req.query_request.as_ref().unwrap().query,
                "SELECT 'hello world' AS greeting"
            );
            let schema = TableSchema::new().set_fields([TableFieldSchema::new()
                .set_name("greeting")
                .set_type("STRING")]);
            let rows = vec![create_test_row("hello world")];
            let response = QueryResponse::new()
                .set_job_complete(true)
                .set_schema(schema)
                .set_rows(rows)
                .set_total_rows(1u64);
            Ok(Response::from(response))
        });

        let client = BigQuery::from_stub(mock);
        let mut rows = client
            .query("SELECT 'hello world' AS greeting")
            .with_project_id("test-project")
            .until_done()
            .await?
            .read();

        let row = rows
            .next()
            .await
            .transpose()?
            .expect("expected at least one row");
        let greeting: String = row.get("greeting")?;
        assert_eq!(greeting, "hello world");
        assert!(rows.next().await.transpose()?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn mock_query_failure() {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|_, _| {
            Err(Error::service(
                Status::default().set_code(Code::InvalidArgument),
            ))
        });

        let client = BigQuery::from_stub(mock);
        let result = client
            .query("INVALID QUERY")
            .with_project_id("test-project")
            .until_done()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn mock_attach_job_success() -> anyhow::Result<()> {
        let mut mock = MockJobService::new();
        mock.expect_get_job().returning(|req, _| {
            assert_eq!(req.project_id, "test-project");
            assert_eq!(req.job_id, "job_123");
            let job = Job::new()
                .set_job_reference(
                    JobReference::new()
                        .set_project_id("test-project")
                        .set_job_id("job_123"),
                )
                .set_configuration(
                    JobConfiguration::new()
                        .set_query(JobConfigurationQuery::new().set_query("SELECT 1")),
                );
            Ok(Response::from(job))
        });

        let client = BigQuery::from_stub(mock);
        let job_ref = JobReference::new()
            .set_project_id("test-project")
            .set_job_id("job_123");
        let query = client.attach_job(job_ref).await?;
        let metadata = query.metadata();
        let attached_ref = metadata.job_reference.as_ref().unwrap();
        assert_eq!(attached_ref.project_id, "test-project");
        assert_eq!(attached_ref.job_id, "job_123");

        Ok(())
    }

    #[tokio::test]
    async fn mock_with_shared_arc() -> anyhow::Result<()> {
        let mut mock = MockJobService::new();
        mock.expect_query().returning(|_, _| {
            let schema = TableSchema::new()
                .set_fields([TableFieldSchema::new().set_name("num").set_type("INTEGER")]);
            let rows = vec![create_test_row("42")];
            let response = QueryResponse::new()
                .set_job_complete(true)
                .set_schema(schema)
                .set_rows(rows)
                .set_total_rows(1u64);
            Ok(Response::from(response))
        });

        let mock_arc = Arc::new(mock);
        let client1 = BigQuery::from_stub::<MockJobService>(mock_arc.clone());
        let client2 = BigQuery::from_stub::<MockJobService>(mock_arc);

        let mut rows1 = client1
            .query("SELECT 42")
            .with_project_id("proj1")
            .until_done()
            .await?
            .read();
        let row1 = rows1.next().await.transpose()?.unwrap();
        let num1: i64 = row1.get("num")?;
        assert_eq!(num1, 42);

        let mut rows2 = client2
            .query("SELECT 42")
            .with_project_id("proj2")
            .until_done()
            .await?
            .read();
        let row2 = rows2.next().await.transpose()?.unwrap();
        let num2: i64 = row2.get("num")?;
        assert_eq!(num2, 42);

        Ok(())
    }
}
