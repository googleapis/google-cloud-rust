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

use super::arrow::WriterBuilder as ArrowWriterBuilder;
use super::client_builder::ClientBuilder;
use super::pool::StreamPool;
use super::proto::WriterBuilder as ProtoWriterBuilder;
use super::retry_policy::RetryOptions;
use super::transport::Transport;
use crate::ClientBuilderResult as BuilderResult;
use crate::model::{ArrowSchema, ProtoSchema};
use std::sync::Arc;

/// A client for BigQuery Storage Write API.
#[derive(Debug)]
pub struct Write {
    inner: Arc<Transport>,
    pool: Arc<StreamPool>,
    retry_options: RetryOptions,
}

impl Write {
    /// Creates a new [ClientBuilder].
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) async fn new(builder: ClientBuilder) -> BuilderResult<Self> {
        let inner = Arc::new(Transport::new(builder.config).await?);
        let pool = Arc::new(StreamPool::new(inner.clone(), builder.pool_options));
        Ok(Self {
            inner,
            pool,
            retry_options: builder.retry_options,
        })
    }

    /// Creates a writer using [Arrow] as the data format.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///   .arrow(schema())
    ///   .default("projects/my-project/datasets/my-dataset/tables/my-table")
    ///   .await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowSchema;
    /// fn schema() -> ArrowSchema {
    ///   todo!("Define your table's schema...")
    /// }
    /// ```
    ///
    /// [arrow]: https://arrow.apache.org/
    pub fn arrow(&self, schema: ArrowSchema) -> ArrowWriterBuilder {
        ArrowWriterBuilder::new(
            self.inner.clone(),
            self.pool.clone(),
            self.retry_options.clone(),
            schema,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn proto(&self, schema: ProtoSchema) -> ProtoWriterBuilder {
        ProtoWriterBuilder::new(self.inner.clone(), self.retry_options.clone(), schema)
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::AppendError;
    use super::*;
    use crate::model::{ArrowRecordBatch, ArrowSchema, ProtoRows, ProtoSchema};
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Status as TonicStatus;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    #[tokio::test]
    async fn arrow() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = Write::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let writer = client
            .arrow(ArrowSchema::new())
            .default("projects/p/datasets/d/tables/t")
            .await?;
        let err = writer
            .append(ArrowRecordBatch::new())
            .send()
            .await
            .expect_err("write should fail");
        assert!(matches!(err, AppendError::Rpc { source: _ }));

        Ok(())
    }

    #[tokio::test]
    async fn proto() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let client = Write::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let writer = client
            .proto(ProtoSchema::new())
            .default("projects/p/datasets/d/tables/t")
            .await?;
        let err = writer
            .append(ProtoRows::new())
            .send()
            .await
            .expect_err("write should fail");
        assert!(matches!(err, AppendError::Rpc { source: _ }));

        Ok(())
    }

    #[tokio::test]
    async fn multiplexing() -> anyhow::Result<()> {
        let client = Write::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let multiplexed_writer = client
            .arrow(ArrowSchema::new())
            .with_multiplexing(true)
            .default("projects/p/datasets/d/tables/t")
            .await?;
        assert!(Arc::ptr_eq(&client.pool, &multiplexed_writer.inner.pool));

        let standalone_writer = client
            .arrow(ArrowSchema::new())
            .with_multiplexing(false)
            .default("projects/p/datasets/d/tables/t")
            .await?;
        assert!(!Arc::ptr_eq(&client.pool, &standalone_writer.inner.pool));

        Ok(())
    }

    #[tokio::test]
    async fn retry_options() -> anyhow::Result<()> {
        let client = Write::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let writer = client
            .arrow(ArrowSchema::new())
            .default("projects/p/datasets/d/tables/t")
            .await?;

        // The writer uses the client's policies, not a fresh set of defaults.
        let options = &writer.inner.options;
        assert!(Arc::ptr_eq(
            &client.retry_options.retry_policy,
            &options.retry_policy
        ));
        assert!(Arc::ptr_eq(
            &client.retry_options.backoff_policy,
            &options.backoff_policy
        ));
        assert_eq!(
            client.retry_options.attempt_timeout,
            options.attempt_timeout
        );

        Ok(())
    }
}
