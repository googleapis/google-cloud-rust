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
use super::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter};
use crate::model::write_stream::Type;
use crate::model::{ArrowSchema, WriteStream};
use crate::{Error, Result};
use gaxi::path_parameter::{PathMismatchBuilder, try_match};
use gaxi::routing_parameter::Segment;
use google_cloud_gax::error::binding::BindingError;
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
    /// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
    pub fn default<T: Into<String>>(self, table: T) -> Result<DefaultWriter> {
        let table = table.into();
        validate_table(table.as_str())?;
        let mut write_stream = table;
        write_stream.push_str("/streams/_default");
        Ok(DefaultWriter::new(self.inner, write_stream, self.schema))
    }

    /// Creates a pending writer for the given table.
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
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.schema, schema);
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
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.schema, schema);
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
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.schema, schema);
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
}
