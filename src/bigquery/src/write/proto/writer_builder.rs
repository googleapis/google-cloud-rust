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

use super::super::format::Proto;
use super::super::generated::gapic_storage::client::BigQueryWrite;
use super::super::pool::{StreamPool, StreamPoolOptions};
use super::super::retry_policy::RetryOptions;
use super::super::transport::Transport;
use super::super::validate::{validate_stream, validate_table};
use super::{DefaultWriter, Writer};
use crate::model::{ProtoSchema, WriteStream};
use crate::write::error::WriterBuilderError;
use std::sync::Arc;

/// A builder to create a protobuf stream writer.
#[derive(Clone, Debug)]
pub struct WriterBuilder {
    inner: Arc<Transport>,
    retry_options: RetryOptions,
    schema: ProtoSchema,
}

impl WriterBuilder {
    pub(crate) fn new(
        inner: Arc<Transport>,
        retry_options: RetryOptions,
        schema: ProtoSchema,
    ) -> Self {
        Self {
            inner,
            retry_options,
            schema,
        }
    }

    /// Creates a writer for the [default stream] for the given table.
    ///
    /// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
    pub async fn default<T: Into<String>>(
        self,
        table: T,
    ) -> std::result::Result<DefaultWriter, WriterBuilderError> {
        let table = table.into();
        validate_table(table.as_str())?;
        let mut write_stream = table;
        write_stream.push_str("/streams/_default");
        // TODO(#6765) - use client's pool if multiplexing is enabled
        let options = StreamPoolOptions {
            max_streams: 1,
            ..Default::default()
        };
        let pool = Arc::new(StreamPool::new(self.inner, options));
        let format = Proto {
            schema: self.schema,
        };
        Ok(DefaultWriter::new(
            pool,
            self.retry_options,
            write_stream,
            format,
        ))
    }

    /// Returns a writer for a newly created stream for the given table.
    pub async fn create<U: Writer, T: Into<String>>(
        self,
        table: T,
    ) -> std::result::Result<U, WriterBuilderError> {
        let table = table.into();
        validate_table(table.as_str())?;

        let client = BigQueryWrite::from_stub::<Transport>(self.inner.clone());
        let stream = client
            .create_write_stream()
            .set_parent(table)
            .set_write_stream(WriteStream::new().set_type(U::STREAM_TYPE))
            .send()
            .await?;

        Ok(U::build(self.inner, stream.name, self.schema))
    }

    /// Attaches the builder to an existing stream.
    pub async fn attach<U: Writer, S: Into<String>>(
        self,
        write_stream: S,
    ) -> std::result::Result<U, WriterBuilderError> {
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
            return Err(WriterBuilderError::TypeMismatch {
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
    use crate::model::write_stream::Type;
    use crate::write::test::*;
    use crate::write::{BufferedWriter, CommittedWriter, PendingWriter};
    use bigquery_grpc_mock::google::cloud::bigquery::storage::v1::WriteStream as MockWriteStream;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use test_case::test_case;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn default() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let builder = WriterBuilder::new(transport, test_retry_options(), proto_schema());
        let writer = builder.default("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.write_stream,
            "projects/p/datasets/d/tables/t/streams/_default"
        );
        assert_eq!(writer.format.schema, proto_schema());
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
        let builder = WriterBuilder::new(transport, test_retry_options(), proto_schema());
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
        let writer: CommittedWriter<Proto> =
            builder.create("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, proto_schema());
        Ok(())
    }

    #[tokio::test]
    async fn create_pending_success() -> anyhow::Result<()> {
        let (transport, _server) = create_mock(Type::Pending).await?;
        let builder = test_builder(transport);
        let writer: PendingWriter<Proto> = builder.create("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, proto_schema());
        Ok(())
    }

    #[tokio::test]
    async fn create_buffered_success() -> anyhow::Result<()> {
        let (transport, _server) = create_mock(Type::Buffered).await?;
        let builder = test_builder(transport);
        let writer: BufferedWriter<Proto> =
            builder.create("projects/p/datasets/d/tables/t").await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, proto_schema());
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
        let err = builder
            .create::<PendingWriter<Proto>, _>(table)
            .await
            .expect_err("should fail locally on bad format");
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
        let writer: CommittedWriter<Proto> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, proto_schema());
        Ok(())
    }

    #[tokio::test]
    async fn attach_pending_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Pending).await?;
        let builder = test_builder(transport);
        let writer: PendingWriter<Proto> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, proto_schema());
        Ok(())
    }

    #[tokio::test]
    async fn attach_buffered_success() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let builder = test_builder(transport);
        let writer: BufferedWriter<Proto> = builder
            .attach("projects/p/datasets/d/tables/t/streams/s")
            .await?;
        assert_eq!(
            writer.inner.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(writer.inner.format.schema, proto_schema());
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
        let err = builder
            .attach::<CommittedWriter<Proto>, _>(stream)
            .await
            .expect_err("should fail locally on bad format");
        assert!(matches!(err, WriterBuilderError::Rpc { source: e } if e.is_binding()));
        Ok(())
    }

    #[tokio::test]
    async fn attach_stream_type_mismatch() -> anyhow::Result<()> {
        let (transport, _server) = attach_mock(Type::Buffered).await?;
        let builder = test_builder(transport);
        let err = builder
            .attach::<CommittedWriter<Proto>, _>("projects/p/datasets/d/tables/t/streams/s")
            .await
            .expect_err("should return type mismatch error");
        assert!(matches!(err, WriterBuilderError::TypeMismatch { .. }));
        assert!(err.to_string().contains("stream type mismatch: requested"));
        Ok(())
    }

    fn test_builder(transport: Arc<Transport>) -> WriterBuilder {
        WriterBuilder::new(transport, test_retry_options(), proto_schema())
    }
}
