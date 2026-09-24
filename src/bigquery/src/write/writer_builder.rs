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
use super::format::{Arrow, DataFormat, Proto};
use super::generated::gapic_storage::client::BigQueryWrite;
use super::pool::{StreamPool, StreamPoolOptions};
use super::retry_policy::RetryOptions;
use super::transport::Transport;
use super::validate::{validate_stream, validate_table};
use crate::model::write_stream::Type;
use crate::model::{ArrowSchema, ProtoSchema, WriteStream};
use crate::write::error::WriterBuilderError;
use crate::write::stream_type::{ApplicationCreatedStream, DefaultStream, HasStream, Stream};
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A builder to create a stream writer.
#[derive(Clone, Debug)]
pub struct WriterBuilder<S> {
    pub(crate) inner: Arc<Transport>,
    pub(crate) pools: Arc<Mutex<HashMap<String, Arc<StreamPool>>>>,
    pub(crate) pool_options: StreamPoolOptions,
    pub(crate) retry_options: RetryOptions,
    op: Operation,
    pub(crate) multiplexing: bool,
    _stream: PhantomData<S>,
}

impl WriterBuilder<DefaultStream> {
    pub(crate) fn new_open_default(
        inner: Arc<Transport>,
        pools: Arc<Mutex<HashMap<String, Arc<StreamPool>>>>,
        pool_options: StreamPoolOptions,
        retry_options: RetryOptions,
        table: String,
    ) -> Self {
        Self {
            inner,
            pools,
            pool_options,
            retry_options,
            op: Operation::OpenDefault { table },
            multiplexing: false,
            _stream: PhantomData,
        }
    }

    /// Enable multiplexing.
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
    ///     .open_default_stream("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .with_multiplexing(true)
    ///     .build_arrow(schema())
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

    /// Configure the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// use google_cloud_bigquery::write::retry_policy::RetryableErrors;
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// let writer = client
    ///     .open_default_stream("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .with_retry_policy(RetryableErrors.with_attempt_limit(3))
    ///     .build_arrow(schema())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// #
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # fn schema() -> ArrowSchema {
    /// #   todo!("Define your table's schema...")
    /// # }
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.retry_options.retry_policy = v.into().into();
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let policy = ExponentialBackoff::default();
    /// let writer = client
    ///     .open_default_stream("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .with_backoff_policy(policy)
    ///     .build_arrow(schema())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// #
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # fn schema() -> ArrowSchema {
    /// #   todo!("Define your table's schema...")
    /// # }
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.retry_options.backoff_policy = v.into().into();
        self
    }

    /// Configure the timeout for a single write attempt.
    ///
    /// Without this limit, a write can block forever if the service accepts
    /// the stream but never responds. On a timeout, the client abandons the
    /// stream and the retry policy decides whether to make another attempt.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// let writer = client
    ///     .open_default_stream("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .with_attempt_timeout(Duration::from_secs(10))
    ///     .build_arrow(schema())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// #
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # fn schema() -> ArrowSchema {
    /// #   todo!("Define your table's schema...")
    /// # }
    /// ```
    pub fn with_attempt_timeout(mut self, v: Duration) -> Self {
        self.retry_options.attempt_timeout = Some(v);
        self
    }

    pub(crate) fn make_default_writer<F: DataFormat>(
        self,
        write_stream: String,
        location: String,
        format: F,
    ) -> DefaultWriter<F> {
        let pool = if self.multiplexing {
            let key = format!("{}-{}", location, format.format_name());
            let mut pools = self.pools.lock().expect("pools lock poisoned");
            pools
                .entry(key)
                .or_insert_with(|| {
                    Arc::new(StreamPool::new(
                        self.inner.clone(),
                        self.pool_options.clone(),
                    ))
                })
                .clone()
        } else {
            let options = StreamPoolOptions {
                max_streams: 1,
                ..Default::default()
            };
            Arc::new(StreamPool::new(self.inner, options))
        };
        DefaultWriter::new(pool, self.retry_options, write_stream, format)
    }
}

impl<S: ApplicationCreatedStream> WriterBuilder<S> {
    pub(crate) fn new_create(
        inner: Arc<Transport>,
        retry_options: RetryOptions,
        table: String,
    ) -> Self {
        Self {
            inner,
            pools: Arc::new(Mutex::new(HashMap::new())),
            pool_options: StreamPoolOptions::default(),
            retry_options,
            op: Operation::Create {
                table,
                stream_type: S::STREAM_TYPE,
            },
            multiplexing: false,
            _stream: PhantomData,
        }
    }

    pub(crate) fn new_attach(
        inner: Arc<Transport>,
        retry_options: RetryOptions,
        write_stream: String,
    ) -> Self {
        Self {
            inner,
            pools: Arc::new(Mutex::new(HashMap::new())),
            pool_options: StreamPoolOptions::default(),
            retry_options,
            op: Operation::Attach {
                write_stream,
                stream_type: S::STREAM_TYPE,
            },
            multiplexing: false,
            _stream: PhantomData,
        }
    }
}

impl<S: Stream> WriterBuilder<S> {
    /// Consumes the builder and creates a writer using [Arrow] as the data format.
    ///
    /// Returns the writer `W` corresponding to the stream type `S`:
    /// - [`DefaultStream`] -> [`DefaultWriter<Arrow>`][crate::write::DefaultWriter]
    /// - [`PendingStream`][crate::write::stream_type::PendingStream] -> [`PendingWriter<Arrow>`][crate::write::PendingWriter]
    /// - [`CommittedStream`][crate::write::stream_type::CommittedStream] -> [`CommittedWriter<Arrow>`][crate::write::CommittedWriter]
    /// - [`BufferedStream`][crate::write::stream_type::BufferedStream] -> [`BufferedWriter<Arrow>`][crate::write::BufferedWriter]
    ///
    /// # Example
    /// ```
    /// # use google_cloud_bigquery::client::Write;
    /// # use google_cloud_bigquery::model::ArrowSchema;
    /// # async fn sample(client: Write) -> anyhow::Result<()> {
    /// let writer = client
    ///     .open_default_stream("projects/my-project/datasets/my_dataset/tables/my_table")
    ///     .build_arrow(ArrowSchema::new())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [Arrow]: https://arrow.apache.org/
    pub async fn build_arrow<W>(
        self,
        schema: ArrowSchema,
    ) -> std::result::Result<W, WriterBuilderError>
    where
        S: Stream<Writer<Arrow> = W>,
        W: HasStream<Stream = S>,
    {
        self.build(Arrow { schema }).await
    }

    /// Consumes the builder and creates a writer using Protobuf as the data format.
    #[allow(dead_code)]
    pub(crate) async fn build_proto<W>(
        self,
        schema: ProtoSchema,
    ) -> std::result::Result<W, WriterBuilderError>
    where
        S: Stream<Writer<Proto> = W>,
        W: HasStream<Stream = S>,
    {
        self.build(Proto { schema }).await
    }

    async fn build<F>(self, format: F) -> std::result::Result<S::Writer<F>, WriterBuilderError>
    where
        F: DataFormat,
    {
        let (write_stream, location) = match &self.op {
            Operation::OpenDefault { table } => self.open_default(table).await?,
            Operation::Create { table, stream_type } => {
                let stream = self.create_stream(table, stream_type.clone()).await?;
                (stream, String::new())
            }
            Operation::Attach {
                write_stream,
                stream_type,
            } => {
                let stream = self
                    .attach_to_stream(write_stream, stream_type.clone())
                    .await?;
                (stream, String::new())
            }
        };
        Ok(S::build(self, write_stream, location, format))
    }

    async fn open_default(
        &self,
        table: &str,
    ) -> std::result::Result<(String, String), WriterBuilderError> {
        validate_table(table)?;
        let write_stream = format!("{table}/streams/_default");
        let location = if self.multiplexing {
            let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
            let stream = client
                .get_write_stream()
                .set_name(&write_stream)
                .send()
                .await?;
            stream.location
        } else {
            String::new()
        };
        Ok((write_stream, location))
    }

    async fn create_stream(
        &self,
        table: &str,
        stream_type: Type,
    ) -> std::result::Result<String, WriterBuilderError> {
        validate_table(table)?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .create_write_stream()
            .set_parent(table)
            .set_write_stream(WriteStream::new().set_type(stream_type))
            .send()
            .await?;

        Ok(stream.name)
    }

    async fn attach_to_stream(
        &self,
        write_stream: &str,
        stream_type: Type,
    ) -> std::result::Result<String, WriterBuilderError> {
        validate_stream(write_stream)?;

        if write_stream.ends_with("/streams/_default") {
            return Err(WriterBuilderError::TypeMismatch {
                expected: format!("{stream_type:?}"),
                actual: "Default (use `open_default_stream` instead)".to_string(),
            });
        }

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .get_write_stream()
            .set_name(write_stream)
            .send()
            .await?;

        if stream_type != stream.r#type {
            return Err(WriterBuilderError::TypeMismatch {
                expected: format!("{stream_type:?}"),
                actual: format!("{:?}", stream.r#type),
            });
        }
        Ok(stream.name)
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
    use google_cloud_gax::retry_policy::AlwaysRetry;
    use test_case::test_case;
    use tokio::task::JoinHandle;

    type Result<T> = std::result::Result<T, WriterBuilderError>;

    #[tokio::test]
    async fn default_stream_options() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = test_open_default(transport, "projects/p/datasets/d/tables/t");
        assert!(!builder.multiplexing);
        assert_eq!(builder.retry_options.attempt_timeout, None);

        let builder = builder
            .with_retry_policy(AlwaysRetry)
            .with_backoff_policy(NoBackoff)
            .with_attempt_timeout(Duration::from_secs(10));
        assert_eq!(
            builder.retry_options.attempt_timeout,
            Some(Duration::from_secs(10))
        );

        let fmt = format!("{:?}", builder.retry_options);
        assert!(fmt.contains("AlwaysRetry"), "{fmt}");
        assert!(fmt.contains("NoBackoff"), "{fmt}");

        Ok(())
    }

    #[tokio::test]
    async fn default() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let writer = test_open_default(transport, "projects/p/datasets/d/tables/t")
            .build_arrow(schema())
            .await?;
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
        let err = test_open_default(transport, table)
            .build_arrow(schema())
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
        let writer: CommittedWriter<Arrow> =
            test_create(transport, "projects/p/datasets/d/tables/t")
                .build_arrow(schema())
                .await?;
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
        let writer: PendingWriter<Arrow> = test_create(transport, "projects/p/datasets/d/tables/t")
            .build_arrow(schema())
            .await?;
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
        let writer: BufferedWriter<Arrow> =
            test_create(transport, "projects/p/datasets/d/tables/t")
                .build_arrow(schema())
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
    #[test_case("projects/p/datasets/d/tables/")]
    #[test_case("projects/p/instances/i/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams")]
    #[test_case("projects/p/datasets/d/tables/t/streams/_default")]
    #[tokio::test]
    async fn create_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let res: Result<PendingWriter<Arrow>> =
            test_create(transport, table).build_arrow(schema()).await;
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
        let writer: CommittedWriter<Arrow> =
            test_attach(transport, "projects/p/datasets/d/tables/t/streams/s")
                .build_arrow(schema())
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
        let writer: PendingWriter<Arrow> =
            test_attach(transport, "projects/p/datasets/d/tables/t/streams/s")
                .build_arrow(schema())
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
        let writer: BufferedWriter<Arrow> =
            test_attach(transport, "projects/p/datasets/d/tables/t/streams/s")
                .build_arrow(schema())
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
        let res: Result<CommittedWriter<Arrow>> =
            test_attach(transport, stream).build_arrow(schema()).await;
        let err = res.expect_err("should fail locally on bad format");
        assert!(matches!(err, WriterBuilderError::Rpc { source: e } if e.is_binding()));
        Ok(())
    }

    #[tokio::test]
    async fn attach_stream_type_mismatch() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let res: Result<CommittedWriter<Arrow>> =
            test_attach(transport, "projects/p/datasets/d/tables/t/streams/s")
                .build_arrow(schema())
                .await;
        let err = res.expect_err("should return type mismatch error");
        assert!(matches!(err, WriterBuilderError::TypeMismatch { .. }));
        assert!(err.to_string().contains("stream type mismatch: requested"));
        Ok(())
    }

    #[tokio::test]
    async fn attach_default_stream_rejected() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let default_stream = "projects/p/datasets/d/tables/t/streams/_default";

        let res: Result<CommittedWriter<Arrow>> = test_attach(transport.clone(), default_stream)
            .build_arrow(schema())
            .await;
        let err = res.expect_err("should reject attaching CommittedWriter to _default");
        assert!(
            matches!(
                &err,
                WriterBuilderError::TypeMismatch { expected, actual }
                    if expected == "Committed" && actual.contains("Default")
            ),
            "unexpected error: {err:?}"
        );
        assert!(
            err.to_string().contains(
                "stream type mismatch: requested Committed, but matched resource yields Default"
            ),
            "unexpected display: {err}"
        );

        let res: Result<PendingWriter<Arrow>> = test_attach(transport.clone(), default_stream)
            .build_arrow(schema())
            .await;
        let err = res.expect_err("should reject attaching PendingWriter to _default");
        assert!(
            matches!(
                &err,
                WriterBuilderError::TypeMismatch { expected, actual }
                    if expected == "Pending" && actual.contains("Default")
            ),
            "unexpected error: {err:?}"
        );

        let res: Result<BufferedWriter<Arrow>> = test_attach(transport, default_stream)
            .build_arrow(schema())
            .await;
        let err = res.expect_err("should reject attaching BufferedWriter to _default");
        assert!(
            matches!(
                &err,
                WriterBuilderError::TypeMismatch { expected, actual }
                    if expected == "Buffered" && actual.contains("Default")
            ),
            "unexpected error: {err:?}"
        );

        Ok(())
    }

    fn test_open_default(transport: Arc<Transport>, table: &str) -> WriterBuilder<DefaultStream> {
        let pools = Arc::new(Mutex::new(HashMap::new()));
        WriterBuilder::new_open_default(
            transport,
            pools,
            StreamPoolOptions::default(),
            test_retry_options(),
            table.to_string(),
        )
    }

    fn test_create<S: ApplicationCreatedStream>(
        transport: Arc<Transport>,
        table: &str,
    ) -> WriterBuilder<S> {
        WriterBuilder::new_create(transport, test_retry_options(), table.to_string())
    }

    fn test_attach<S: ApplicationCreatedStream>(
        transport: Arc<Transport>,
        write_stream: &str,
    ) -> WriterBuilder<S> {
        WriterBuilder::new_attach(transport, test_retry_options(), write_stream.to_string())
    }
}
