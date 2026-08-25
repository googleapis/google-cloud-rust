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
use super::super::transport::Transport;
use super::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter, TryFromStream};
use crate::model::write_stream::Type;
use crate::model::{ArrowSchema, WriteStream};
use crate::{Error, Result};
use gaxi::path_parameter::{PathMismatchBuilder, try_match};
use gaxi::routing_parameter::Segment;
use google_cloud_gax::error::binding::BindingError;
use google_cloud_gax::error::rpc::{Code, Status};
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
    ///     .default("projects/my-project/datasets/my-dataset/tables/my-table")?;
    /// # Ok(()) }
    ///
    /// use google_cloud_bigquery::model::ArrowSchema;
    /// fn schema() -> ArrowSchema {
    ///   todo!("Define your table's schema...")
    /// }
    /// ```
    ///
    /// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
    pub fn default<T: Into<String>>(self, table: T) -> Result<DefaultWriter> {
        let table = table.into();
        validate_table(table.as_str())?;
        let mut write_stream = table;
        write_stream.push_str("/streams/_default");
        Ok(DefaultWriter::new(self.inner, write_stream, self.schema))
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

    /// Attaches to an existing stream.
    pub async fn attach<U: TryFromStream>(self, write_stream: impl Into<String>) -> Result<U> {
        let write_stream = write_stream.into();
        validate_stream(write_stream.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .get_write_stream()
            .set_name(&write_stream)
            .send()
            .await?;

        if stream.r#type != U::EXPECTED_TYPE {
            let msg = format!(
                "stream type mismatch. expected {:?}, got {:?}",
                U::EXPECTED_TYPE,
                stream.r#type
            );
            return Err(Error::service(
                Status::default()
                    .set_code(Code::InvalidArgument)
                    .set_message(msg),
            ));
        }

        Ok(U::build(self.inner, write_stream, self.schema))
    }
}

fn validate_table(table: &str) -> Result<()> {
    let segments = &[
        Segment::Literal("projects/"),
        Segment::SingleWildcard,
        Segment::Literal("/datasets/"),
        Segment::SingleWildcard,
        Segment::Literal("/tables/"),
        Segment::SingleWildcard,
    ];
    try_match(Some(table), segments)
        .ok_or_else(|| {
            let builder = PathMismatchBuilder::default().maybe_add(
                Some(table),
                segments,
                "table",
                "projects/*/datasets/*/tables/*",
            );
            Error::binding(BindingError {
                paths: vec![builder.build()],
            })
        })
        .map(|_| ())
}

fn validate_stream(stream: &str) -> Result<()> {
    let segments = &[
        Segment::Literal("projects/"),
        Segment::SingleWildcard,
        Segment::Literal("/datasets/"),
        Segment::SingleWildcard,
        Segment::Literal("/tables/"),
        Segment::SingleWildcard,
        Segment::Literal("/streams/"),
        Segment::SingleWildcard,
    ];
    try_match(Some(stream), segments)
        .ok_or_else(|| {
            let builder = PathMismatchBuilder::default().maybe_add(
                Some(stream),
                segments,
                "write_stream",
                "projects/*/datasets/*/tables/*/streams/*",
            );
            Error::binding(BindingError {
                paths: vec![builder.build()],
            })
        })
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::super::super::transport::tests::test_transport;
    use super::*;
    use test_case::test_case;

    #[tokio::test]
    async fn pending_success() -> anyhow::Result<()> {
        use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
        use bigquery_grpc_mock::{MockBigQueryWrite, start};
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
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let writer = builder.pending("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema);
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn pending_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let err = builder
            .pending(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn committed_success() -> anyhow::Result<()> {
        use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
        use bigquery_grpc_mock::{MockBigQueryWrite, start};
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
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let writer = builder.committed("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema);
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn committed_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let err = builder
            .committed(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn default() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let writer = builder.default("projects/p/datasets/d/tables/t")?;
        assert_eq!(
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/_default"
        );
        assert_eq!(writer.schema, schema);
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
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let err = builder
            .default(table)
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }
    #[tokio::test]
    async fn buffered_success() -> anyhow::Result<()> {
        use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
        use bigquery_grpc_mock::{MockBigQueryWrite, start};
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
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let writer = builder.buffered("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, schema);
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn buffered_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let err = builder
            .buffered(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    macro_rules! test_attach_success {
        ($name:ident, $writer_type:ident, $stream_type:expr) => {
            #[tokio::test]
            async fn $name() -> anyhow::Result<()> {
                use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
                use bigquery_grpc_mock::{MockBigQueryWrite, start};
                let mut mock = MockBigQueryWrite::new();
                mock.expect_get_write_stream().return_once(|req| {
                    let req = req.into_inner();
                    assert_eq!(req.name, "projects/p/datasets/d/tables/t/streams/s");
                    Ok(gaxi::grpc::tonic::Response::new(MockWriteStream {
                        name: "projects/p/datasets/d/tables/t/streams/s".to_string(),
                        r#type: $stream_type,
                        ..Default::default()
                    }))
                });
                let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
                let transport = Arc::new(test_transport(endpoint).await?);
                let schema = ArrowSchema::new().set_serialized_schema("test");
                let builder = WriterBuilder::new(transport, schema.clone());
                let writer: $writer_type = builder
                    .attach("projects/p/datasets/d/tables/t/streams/s")
                    .await?;
                assert_eq!(
                    writer.inner.write_stream,
                    "projects/p/datasets/d/tables/t/streams/s"
                );
                assert_eq!(writer.inner.schema, schema);
                Ok(())
            }
        };
    }

    test_attach_success!(attach_committed_success, CommittedWriter, 1);
    test_attach_success!(attach_pending_success, PendingWriter, 2);
    test_attach_success!(attach_buffered_success, BufferedWriter, 3);

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t")]
    #[test_case("projects/p/datasets/d/tables/t/streams/")]
    #[tokio::test]
    async fn attach_bad_stream_format(stream: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let err = builder
            .attach::<CommittedWriter>(stream)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn attach_stream_type_mismatch() -> anyhow::Result<()> {
        use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
        use bigquery_grpc_mock::{MockBigQueryWrite, start};
        let mut mock = MockBigQueryWrite::new();
        mock.expect_get_write_stream().return_once(|req| {
            let req = req.into_inner();
            assert_eq!(req.name, "projects/p/datasets/d/tables/t/streams/s");
            // Return buffered type (3) when they requested a CommittedWriter (1)
            Ok(gaxi::grpc::tonic::Response::new(MockWriteStream {
                name: "projects/p/datasets/d/tables/t/streams/s".to_string(),
                r#type: 3,
                ..Default::default()
            }))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let err = builder
            .attach::<CommittedWriter>("projects/p/datasets/d/tables/t/streams/s")
            .await
            .expect_err("should return type mismatch error");
        assert!(err.is_io(), "{err:?}");
        assert!(err.to_string().contains("stream type mismatch"));
        Ok(())
    }
}
