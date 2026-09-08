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
use super::super::validate::validate_table;
use super::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter};
use crate::Result;
use crate::model::write_stream::Type;
use crate::model::{ProtoSchema, WriteStream};
use std::sync::Arc;

/// A builder to create a protobuf stream writer
#[derive(Clone, Debug)]
pub struct WriterBuilder {
    inner: Arc<Transport>,
    schema: ProtoSchema,
}

impl WriterBuilder {
    pub(crate) fn new(inner: Arc<Transport>, schema: ProtoSchema) -> Self {
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

    /// Creates a writer for a [pending stream] for the given table.
    ///
    /// [pending stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#pending_type
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

    /// Creates a writer for a [committed stream] for the given table.
    ///
    /// [committed stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#committed_type
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

    /// Creates a writer for a [buffered stream] for the given table.
    ///
    /// [buffered stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#buffered_type
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test::*;
    use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use test_case::test_case;

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
        let builder = WriterBuilder::new(transport, proto_schema());
        let writer = builder.pending("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, proto_schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn pending_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, proto_schema());
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
        let builder = WriterBuilder::new(transport, proto_schema());
        let writer = builder.committed("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, proto_schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn committed_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, proto_schema());
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
        let builder = WriterBuilder::new(transport, proto_schema());
        let writer = builder.default("projects/p/datasets/d/tables/t")?;
        assert_eq!(
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/_default"
        );
        assert_eq!(writer.schema, proto_schema());
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
        let builder = WriterBuilder::new(transport, proto_schema());
        let err = builder
            .default(table)
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
        let builder = WriterBuilder::new(transport, proto_schema());
        let writer = builder.buffered("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.schema, proto_schema());
        Ok(())
    }

    #[test_case("projects/p")]
    #[test_case("projects/p/tables/t")]
    #[test_case("projects/p/datasets/d/tables/")]
    #[tokio::test]
    async fn buffered_bad_table_format(table: &str) -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, proto_schema());
        let err = builder
            .buffered(table)
            .await
            .expect_err("should fail locally on bad format");
        assert!(err.is_binding(), "{err:?}");
        Ok(())
    }
}
