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

use super::super::generated::gapic_storage::client::BigQueryWrite;
use super::super::pool::StreamPool;
use super::super::transport::Transport;
use super::super::validate::{validate_stream, validate_table};
use super::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter, Writer};
use crate::Result;
use crate::model::write_stream::Type;
use crate::model::{ArrowSchema, WriteStream};
use crate::write::error::{AttachError, AttachResult};
use std::sync::Arc;

/// A builder to create a stream writer
#[derive(Clone, Debug)]
pub struct WriterBuilder {
    inner: Arc<Transport>,
    schema: ArrowSchema,
}

impl WriterBuilder {
    pub(crate) fn new(inner: Arc<Transport>, schema: ArrowSchema) -> Self {
        Self { inner, schema }
    }

    /// Create a writer for the [default stream] for the given table.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///     .arrow(schema())
    ///     .default("projects/my-project/datasets/my-dataset/tables/my-table")
    ///     .await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowSchema;
    /// fn schema() -> ArrowSchema {
    ///   todo!("Define your table's schema...")
    /// }
    /// ```
    ///
    /// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
    pub async fn default<T: Into<String>>(self, table: T) -> Result<DefaultWriter> {
        let table = table.into();
        validate_table(table.as_str())?;
        let mut write_stream = table;
        write_stream.push_str("/streams/_default");
        // TODO(#6765) - use client's pool if multiplexing is enabled
        let pool = Arc::new(StreamPool::new(self.inner, 1));
        Ok(DefaultWriter::new(pool, write_stream, self.schema))
    }

    /// Creates a pending writer for the given table.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///     .arrow(schema())
    ///     .pending("projects/my-project/datasets/my-dataset/tables/my-table")
    ///     .await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowSchema;
    /// fn schema() -> ArrowSchema {
    ///   todo!("Define your table's schema...")
    /// }
    /// ```
    pub async fn pending<T: Into<String>>(self, table: T) -> Result<PendingWriter> {
        let table = table.into();
        validate_table(table.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let write_stream = client
            .create_write_stream()
            .set_parent(table)
            .set_write_stream(WriteStream::new().set_type(Type::Pending))
            .send()
            .await?;

        Ok(PendingWriter::new(
            self.inner,
            write_stream.name,
            self.schema,
        ))
    }

    /// Creates a committed writer for the given table.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///     .arrow(schema())
    ///     .committed("projects/my-project/datasets/my-dataset/tables/my-table")
    ///     .await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowSchema;
    /// fn schema() -> ArrowSchema {
    ///   todo!("Define your table's schema...")
    /// }
    /// ```
    pub async fn committed<T: Into<String>>(self, table: T) -> Result<CommittedWriter> {
        let table = table.into();
        validate_table(table.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let write_stream = client
            .create_write_stream()
            .set_parent(table)
            .set_write_stream(WriteStream::new().set_type(Type::Committed))
            .send()
            .await?;

        Ok(CommittedWriter::new(
            self.inner,
            write_stream.name,
            self.schema,
        ))
    }

    /// Creates a buffered writer for the given table.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///     .arrow(schema())
    ///     .buffered("projects/my-project/datasets/my-dataset/tables/my-table")
    ///     .await?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowSchema;
    /// fn schema() -> ArrowSchema {
    ///   todo!("Define your table's schema...")
    /// }
    /// ```
    pub async fn buffered<T: Into<String>>(self, table: T) -> Result<BufferedWriter> {
        let table = table.into();
        validate_table(table.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let write_stream = client
            .create_write_stream()
            .set_parent(table)
            .set_write_stream(WriteStream::new().set_type(Type::Buffered))
            .send()
            .await?;

        Ok(BufferedWriter::new(
            self.inner,
            write_stream.name,
            self.schema,
        ))
    }

    /// Attaches the builder to an existing stream.
    ///
    /// # Example
    /// ```
    /// use google_cloud_bigquery::write::arrow::CommittedWriter;
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer: CommittedWriter = client
    ///     .arrow(schema())
    ///     .attach("projects/my-project/datasets/my_dataset/tables/my_table/streams/my_stream")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// #
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # fn schema() -> ArrowSchema {
    /// #   todo!("Define your table's schema...")
    /// # }
    /// ```
    pub async fn attach<U: Writer, S: Into<String>>(self, write_stream: S) -> AttachResult<U> {
        let write_stream = write_stream.into();
        validate_stream(write_stream.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .get_write_stream()
            .set_name(&write_stream)
            .send()
            .await?;

        let stream_type = stream.r#type.clone();
        if stream_type != U::STREAM_TYPE {
            return Err(AttachError::TypeMismatch {
                expected: U::STREAM_TYPE,
                actual: stream_type,
            });
        }
        Ok(U::build(self.inner, write_stream, self.schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test::*;
    use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use test_case::test_case;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn pending_success() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_create_write_stream().return_once(|req| {
            let req = req.into_inner();
            assert_eq!(req.parent, "projects/p/datasets/d/tables/t");
            let ws = req.write_stream.expect("write_stream populated");
            assert_eq!(Type::from(ws.r#type), Type::Pending);
            Ok(gaxi::grpc::tonic::Response::new(MockWriteStream {
                name: "projects/p/datasets/d/tables/t/streams/s".to_string(),
                ..Default::default()
            }))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let builder = WriterBuilder::new(transport, schema());
        let writer = builder.pending("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn pending_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, schema());
        let err = builder
            .pending(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn committed_success() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_create_write_stream().return_once(|req| {
            let req = req.into_inner();
            assert_eq!(req.parent, "projects/p/datasets/d/tables/t");
            let ws = req.write_stream.expect("write_stream populated");
            assert_eq!(Type::from(ws.r#type), Type::Committed);
            Ok(gaxi::grpc::tonic::Response::new(MockWriteStream {
                name: "projects/p/datasets/d/tables/t/streams/s".to_string(),
                ..Default::default()
            }))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let builder = WriterBuilder::new(transport, schema());
        let writer = builder.committed("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn committed_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, schema());
        let err = builder
            .committed(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn default() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, schema());
        let writer = builder.default("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/_default"
        );
        assert_eq!(writer.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[test_case("projects/p/instances/i/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams")]
    #[test_case("projects/p/datasets/d/tables/t/streams/_default")]
    #[tokio::test]
    async fn bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, schema());
        let err = builder
            .default(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }
    #[tokio::test]
    async fn buffered_success() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_create_write_stream().return_once(|req| {
            let req = req.into_inner();
            assert_eq!(req.parent, "projects/p/datasets/d/tables/t");
            let ws = req.write_stream.expect("write_stream populated");
            assert_eq!(Type::from(ws.r#type), Type::Buffered);
            Ok(gaxi::grpc::tonic::Response::new(MockWriteStream {
                name: "projects/p/datasets/d/tables/t/streams/s".to_string(),
                ..Default::default()
            }))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let builder = WriterBuilder::new(transport, schema());
        let writer = builder.buffered("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn buffered_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, schema());
        let err = builder
            .buffered(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    async fn attach_mock(stream_type: Type) -> anyhow::Result<(Arc<Transport>, JoinHandle<()>)> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_get_write_stream().return_once(move |req| {
            let req = req.into_inner();
            assert_eq!(req.name, "projects/p/datasets/d/tables/t/streams/s");
            Ok(gaxi::grpc::tonic::Response::new(MockWriteStream {
                name: "projects/p/datasets/d/tables/t/streams/s".to_string(),
                r#type: stream_type.value().expect("known enum value"),
                ..Default::default()
            }))
        });
        let (endpoint, server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        Ok((transport, server))
    }

    #[tokio::test]
    async fn attach_committed_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Committed).await?;
        let builder = WriterBuilder::new(transport, schema());
        let writer: CommittedWriter = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema());
        Ok(())
    }

    #[tokio::test]
    async fn attach_pending_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Pending).await?;
        let builder = WriterBuilder::new(transport, schema());
        let writer: PendingWriter = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema());
        Ok(())
    }

    #[tokio::test]
    async fn attach_buffered_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let builder = WriterBuilder::new(transport, schema());
        let writer: BufferedWriter = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams/")]
    #[tokio::test]
    async fn attach_bad_stream_format(stream: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, schema());
        let err = builder
            .attach::<CommittedWriter, _>(stream)
            .await
            .expect_err("should fail locally on bad format");
        assert!(matches!(err, AttachError::Rpc { source: e } if e.is_binding()));
        Ok(())
    }

    #[tokio::test]
    async fn attach_stream_type_mismatch() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let builder = WriterBuilder::new(transport, schema());
        let err = builder
            .attach::<CommittedWriter, _>("projects/p/datasets/d/tables/t/streams/s")
            .await
            .expect_err("should return type mismatch error");
        assert!(matches!(err, AttachError::TypeMismatch { .. }));
        assert!(err.to_string().contains("stream type mismatch: requested"));
        Ok(())
    }
}
