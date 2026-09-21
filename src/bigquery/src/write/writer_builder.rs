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

use super::DefaultWriter;
use super::format::DataFormat;
use super::generated::gapic_storage::client::BigQueryWrite;
use super::pool::{StreamPool, StreamPoolOptions};
use super::retry_policy::RetryOptions;
use super::transport::Transport;
use super::validate::{validate_stream, validate_table};
use crate::model::WriteStream;
use crate::model::write_stream::Type;
use crate::write::error::WriterBuilderError;
use crate::write::stream_type::sealed::ApplicationCreatedStream as _;
use crate::write::stream_type::{ApplicationCreatedStream, DefaultStream, HasStream, Stream};
use std::sync::Arc;

/// A builder to create a stream writer.
#[derive(Clone, Debug)]
pub struct WriterBuilder<F> {
    pub(crate) inner: Arc<Transport>,
    pub(crate) pool: Arc<StreamPool>,
    pub(crate) retry_options: RetryOptions,
    pub(crate) format: F,
    pub(crate) multiplexing: bool,
}

impl<F> WriterBuilder<F>
where
    F: DataFormat,
{
    pub(crate) fn new(
        inner: Arc<Transport>,
        pool: Arc<StreamPool>,
        retry_options: RetryOptions,
        format: F,
    ) -> Self {
        Self {
            inner,
            pool,
            retry_options,
            format,
            multiplexing: false,
        }
    }

    /// Creates a writer for the [default stream] for the given table.
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
    pub async fn default<T: Into<String>>(
        self,
        table: T,
    ) -> std::result::Result<DefaultWriter<F>, WriterBuilderError> {
        let table = table.into();
        let op = Operation::OpenDefault { table };
        self.build::<DefaultStream>(op).await
    }

    fn open_default(table: String) -> std::result::Result<String, WriterBuilderError> {
        validate_table(table.as_str())?;
        let mut write_stream = table;
        write_stream.push_str("/streams/_default");
        Ok(write_stream)
    }

    pub(crate) fn make_default_writer(self, write_stream: String) -> DefaultWriter<F> {
        let pool = if self.multiplexing {
            self.pool
        } else {
            let options = StreamPoolOptions {
                max_streams: 1,
                ..Default::default()
            };
            Arc::new(StreamPool::new(self.inner, options))
        };
        DefaultWriter::new(pool, self.retry_options, write_stream, self.format)
    }

    /// Returns a writer for a newly created stream for the given table.
    ///
    /// # Example
    /// ```
    /// use google_cloud_bigquery::write::format::Arrow;
    /// use google_cloud_bigquery::write::PendingWriter;
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer: PendingWriter<Arrow> = client
    ///     .arrow(schema())
    ///     .create("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// #
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # fn schema() -> ArrowSchema {
    /// #   todo!("Define your table's schema...")
    /// # }
    /// ```
    pub async fn create<W, S>(self, table: S) -> std::result::Result<W, WriterBuilderError>
    where
        S: Into<String>,
        W: HasStream,
        W::Stream: ApplicationCreatedStream<Writer<F> = W>,
    {
        let table = table.into();
        let stream_type = <W::Stream>::STREAM_TYPE;
        let op = Operation::Create { table, stream_type };
        self.build::<W::Stream>(op).await
    }

    async fn create_stream(
        &self,
        table: String,
        stream_type: Type,
    ) -> std::result::Result<String, WriterBuilderError> {
        validate_table(table.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .create_write_stream()
            .set_parent(table)
            .set_write_stream(WriteStream::new().set_type(stream_type))
            .send()
            .await?;

        Ok(stream.name)
    }

    /// Attaches a writer to an existing stream.
    ///
    /// # Example
    /// ```
    /// use google_cloud_bigquery::write::format::Arrow;
    /// use google_cloud_bigquery::write::CommittedWriter;
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer: CommittedWriter<Arrow> = client
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
    pub async fn attach<W, S>(self, write_stream: S) -> std::result::Result<W, WriterBuilderError>
    where
        S: Into<String>,
        W: HasStream,
        W::Stream: ApplicationCreatedStream<Writer<F> = W>,
    {
        let write_stream = write_stream.into();
        let stream_type = <W::Stream>::STREAM_TYPE;
        let op = Operation::Attach {
            write_stream,
            stream_type,
        };
        self.build::<W::Stream>(op).await
    }

    async fn attach_to_stream(
        &self,
        write_stream: String,
        stream_type: Type,
    ) -> std::result::Result<String, WriterBuilderError> {
        validate_stream(write_stream.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .get_write_stream()
            .set_name(write_stream)
            .send()
            .await?;

        if stream_type != stream.r#type {
            return Err(WriterBuilderError::TypeMismatch {
                expected: stream_type,
                actual: stream.r#type,
            });
        }
        Ok(stream.name)
    }

    /// Enable multiplexing
    ///
    /// Set this option to use the client's shared stream pool.
    ///
    /// This option only applies to the default stream.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///     .arrow(schema())
    ///     .with_multiplexing(true)
    ///     .default("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// #
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # fn schema() -> ArrowSchema {
    /// #   todo!("Define your table's schema...")
    /// # }
    /// ```
    pub fn with_multiplexing(mut self, enable: bool) -> Self {
        self.multiplexing = enable;
        self
    }

    async fn build<S>(self, op: Operation) -> std::result::Result<S::Writer<F>, WriterBuilderError>
    where
        S: Stream,
    {
        let write_stream = match op {
            Operation::OpenDefault { table } => Self::open_default(table)?,
            Operation::Create { table, stream_type } => {
                self.create_stream(table, stream_type).await?
            }
            Operation::Attach {
                write_stream,
                stream_type,
            } => self.attach_to_stream(write_stream, stream_type).await?,
        };
        Ok(S::build(self, write_stream))
    }
}

#[derive(Clone, Debug)]
enum Operation {
    OpenDefault {
        table: String,
    },
    Create {
        table: String,
        stream_type: Type,
    },
    Attach {
        write_stream: String,
        stream_type: Type,
    },
}

#[cfg(test)]
mod tests {
    use super::super::format::Arrow;
    use super::*;
    use crate::model::write_stream::Type;
    use crate::write::test::*;
    use crate::write::{BufferedWriter, CommittedWriter, PendingWriter};
    use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use test_case::test_case;
    use tokio::task::JoinHandle;

    type Result<T> = std::result::Result<T, WriterBuilderError>;

    #[tokio::test]
    async fn default() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = test_builder(transport);
        let writer = builder.default("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/_default"
        );
        assert_eq!(writer.format.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[test_case("projects/p/instances/i/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams")]
    #[test_case("projects/p/datasets/d/tables/t/streams/_default")]
    #[tokio::test]
    async fn default_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = test_builder(transport);
        let err = builder
            .default(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(matches!(err, WriterBuilderError::Rpc { source: e } if e.is_binding()));
        Ok(())
    }

    async fn create_mock(stream_type: Type) -> anyhow::Result<(Arc<Transport>, JoinHandle<()>)> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_create_write_stream().return_once(move |req| {
            let req = req.into_inner();
            assert_eq!(req.parent, "projects/p/datasets/d/tables/t");
            let ws = req.write_stream.expect("write_stream populated");
            assert_eq!(Type::from(ws.r#type), stream_type);
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
    async fn create_committed_success() -> anyhow::Result<()> {
        let (transport, _server) = create_mock(Type::Committed).await?;
        let builder = test_builder(transport);
        let writer: CommittedWriter<Arrow> =
            builder.create("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, schema());
        Ok(())
    }

    #[tokio::test]
    async fn create_pending_success() -> anyhow::Result<()> {
        let (transport, _server) = create_mock(Type::Pending).await?;
        let builder = test_builder(transport);
        let writer: PendingWriter<Arrow> = builder.create("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, schema());
        Ok(())
    }

    #[tokio::test]
    async fn create_buffered_success() -> anyhow::Result<()> {
        let (transport, _server) = create_mock(Type::Buffered).await?;
        let builder = test_builder(transport);
        let writer: BufferedWriter<Arrow> =
            builder.create("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[test_case("projects/p/instances/i/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams")]
    #[test_case("projects/p/datasets/d/tables/t/streams/_default")]
    #[tokio::test]
    async fn create_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = test_builder(transport);
        let res: Result<PendingWriter<Arrow>> = builder.create(table).await;
        let err = res.expect_err("should fail locally on bad format");
        assert!(matches!(err, WriterBuilderError::Rpc { source: e } if e.is_binding()));
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
        let builder = test_builder(transport);
        let writer: CommittedWriter<Arrow> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, schema());
        Ok(())
    }

    #[tokio::test]
    async fn attach_pending_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Pending).await?;
        let builder = test_builder(transport);
        let writer: PendingWriter<Arrow> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, schema());
        Ok(())
    }

    #[tokio::test]
    async fn attach_buffered_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let builder = test_builder(transport);
        let writer: BufferedWriter<Arrow> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams/")]
    #[tokio::test]
    async fn attach_bad_stream_format(stream: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = test_builder(transport);
        let res: Result<CommittedWriter<Arrow>> = builder.attach(stream).await;
        let err = res.expect_err("should fail locally on bad format");
        assert!(matches!(err, WriterBuilderError::Rpc { source: e } if e.is_binding()));
        Ok(())
    }

    #[tokio::test]
    async fn attach_stream_type_mismatch() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let builder = test_builder(transport);
        let res: Result<CommittedWriter<Arrow>> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await;
        let err = res.expect_err("should return type mismatch error");
        assert!(matches!(err, WriterBuilderError::TypeMismatch { .. }));
        assert!(err.to_string().contains("stream type mismatch: requested"));
        Ok(())
    }

    fn test_builder(transport: Arc<Transport>) -> WriterBuilder<Arrow> {
        let pool = Arc::new(StreamPool::new(
            transport.clone(),
            StreamPoolOptions::default(),
        ));
        WriterBuilder::new(transport, pool, test_retry_options(), format())
    }
}
