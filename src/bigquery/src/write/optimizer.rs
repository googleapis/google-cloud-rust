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

use crate::google::cloud::bigquery::storage::v1::{
    AppendRowsRequest, ArrowSchema, ProtoSchema, append_rows_request::Rows,
};

/// Optimizes outgoing `AppendRowsRequest` messages on a single `AppendRows`
/// stream connection by redacting redundant `write_stream` and `writer_schema`
/// fields.
///
/// Per the `AppendRowsRequest` specification:
/// - The initial request on a stream must include `trace_id`, `write_stream`,
///   and `writer_schema`.
/// - Subsequent requests to the same `write_stream` and `writer_schema` omit
///   both `write_stream` and `writer_schema`, unless the stream has previously
///   switched `write_stream`s or changed `writer_schema`.
/// - Once a stream switches `write_stream` or changes `writer_schema`, that
///   request must include both `write_stream` and `writer_schema`, and all
///   subsequent requests on the stream must continue to populate `write_stream`
///   (while still omitting `writer_schema` when consecutive requests share the
///   same `write_stream` and `writer_schema`).
#[derive(Debug)]
pub(super) struct SendOptimizer {
    prev_write_stream: String,
    prev_schema: Option<WriterSchema>,
    keep_write_stream: bool,
}

impl SendOptimizer {
    pub(super) fn new(initial_req: &AppendRowsRequest) -> Self {
        Self {
            prev_write_stream: initial_req.write_stream.clone(),
            prev_schema: extract_schema(initial_req),
            keep_write_stream: false,
        }
    }

    pub(super) fn optimize(&mut self, req: &mut AppendRowsRequest) {
        if req.write_stream == self.prev_write_stream && same_schema(req, &self.prev_schema) {
            if !self.keep_write_stream {
                req.write_stream.clear();
            }
            clear_schema(req);
        } else {
            self.keep_write_stream = true;
            self.prev_write_stream.clone_from(&req.write_stream);
            self.prev_schema = extract_schema(req);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum WriterSchema {
    Arrow(ArrowSchema),
    Proto(Box<ProtoSchema>),
}

fn extract_schema(req: &AppendRowsRequest) -> Option<WriterSchema> {
    match req.rows.as_ref()? {
        Rows::ArrowRows(data) => data.writer_schema.clone().map(WriterSchema::Arrow),
        Rows::ProtoRows(data) => data
            .writer_schema
            .clone()
            .map(Box::new)
            .map(WriterSchema::Proto),
    }
}

fn same_schema(req: &AppendRowsRequest, prev: &Option<WriterSchema>) -> bool {
    match (req.rows.as_ref(), prev.as_ref()) {
        (Some(Rows::ArrowRows(d)), Some(WriterSchema::Arrow(s))) => {
            d.writer_schema.as_ref() == Some(s)
        }
        (Some(Rows::ProtoRows(d)), Some(WriterSchema::Proto(s))) => {
            d.writer_schema.as_ref() == Some(s.as_ref())
        }
        _ => false,
    }
}

fn clear_schema(req: &mut AppendRowsRequest) {
    match req.rows.as_mut() {
        Some(Rows::ArrowRows(data)) => data.writer_schema = None,
        Some(Rows::ProtoRows(data)) => data.writer_schema = None,
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::cloud::bigquery::storage::v1::{
        ArrowRecordBatch, ProtoRows,
        append_rows_request::{ArrowData, ProtoData},
    };
    use bytes::Bytes;

    #[test]
    fn simplex_arrow() {
        let r1 = arrow_req("stream_1", "schema_1");
        let mut optimizer = SendOptimizer::new(&r1);

        let mut r2 = arrow_req("stream_1", "schema_1");
        optimizer.optimize(&mut r2);
        assert_eq!(
            extract_arrow(&r2),
            ArrowFields {
                write_stream: "".into(),
                schema: None,
            }
        );

        let mut r3 = arrow_req("stream_1", "schema_1");
        optimizer.optimize(&mut r3);
        assert_eq!(
            extract_arrow(&r3),
            ArrowFields {
                write_stream: "".into(),
                schema: None,
            }
        );
    }

    #[test]
    fn multiplex_destination_switch() {
        let r1 = arrow_req("stream_1", "schema_1");
        let mut optimizer = SendOptimizer::new(&r1);

        let mut r2 = arrow_req("stream_1", "schema_1");
        optimizer.optimize(&mut r2);
        assert_eq!(
            extract_arrow(&r2),
            ArrowFields {
                write_stream: "".into(),
                schema: None,
            }
        );

        let mut r3 = arrow_req("stream_1", "schema_1");
        optimizer.optimize(&mut r3);
        assert_eq!(
            extract_arrow(&r3),
            ArrowFields {
                write_stream: "".into(),
                schema: None,
            }
        );

        let mut r4 = arrow_req("stream_2", "schema_1");
        optimizer.optimize(&mut r4);
        assert_eq!(
            extract_arrow(&r4),
            ArrowFields {
                write_stream: "stream_2".into(),
                schema: Some("schema_1".into()),
            }
        );

        let mut r5 = arrow_req("stream_2", "schema_1");
        optimizer.optimize(&mut r5);
        assert_eq!(
            extract_arrow(&r5),
            ArrowFields {
                write_stream: "stream_2".into(),
                schema: None,
            }
        );
    }

    #[test]
    fn schema_evolution_proto() {
        let r1 = proto_req("stream_1", "schema_1");
        let mut optimizer = SendOptimizer::new(&r1);

        let mut r2 = proto_req("stream_1", "schema_1");
        optimizer.optimize(&mut r2);
        assert_eq!(
            extract_proto(&r2),
            ProtoFields {
                write_stream: "".into(),
                schema: None,
            }
        );

        let mut r3 = proto_req("stream_1", "schema_2");
        optimizer.optimize(&mut r3);
        assert_eq!(
            extract_proto(&r3),
            ProtoFields {
                write_stream: "stream_1".into(),
                schema: Some("schema_2".into()),
            }
        );

        let mut r4 = proto_req("stream_1", "schema_2");
        optimizer.optimize(&mut r4);
        assert_eq!(
            extract_proto(&r4),
            ProtoFields {
                write_stream: "stream_1".into(),
                schema: None,
            }
        );
    }

    #[derive(Debug, PartialEq)]
    struct ArrowFields {
        write_stream: String,
        schema: Option<Bytes>,
    }

    #[derive(Debug, PartialEq)]
    struct ProtoFields {
        write_stream: String,
        schema: Option<String>,
    }

    fn arrow_req(stream: &str, schema: &'static str) -> AppendRowsRequest {
        AppendRowsRequest {
            write_stream: stream.to_string(),
            rows: Some(Rows::ArrowRows(ArrowData {
                writer_schema: Some(ArrowSchema {
                    serialized_schema: schema.into(),
                }),
                rows: Some(ArrowRecordBatch {
                    serialized_record_batch: Bytes::from_static(b"batch"),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        }
    }

    fn proto_req(stream: &str, msg_name: &str) -> AppendRowsRequest {
        AppendRowsRequest {
            write_stream: stream.to_string(),
            rows: Some(Rows::ProtoRows(ProtoData {
                writer_schema: Some(ProtoSchema {
                    proto_descriptor: Some(prost_types::DescriptorProto {
                        name: Some(msg_name.to_string()),
                        ..Default::default()
                    }),
                }),
                rows: Some(ProtoRows {
                    serialized_rows: vec![Bytes::from_static(b"row")],
                }),
            })),
            ..Default::default()
        }
    }

    fn extract_arrow(req: &AppendRowsRequest) -> ArrowFields {
        let Some(Rows::ArrowRows(data)) = &req.rows else {
            panic!("expected ArrowRows, got: {:?}", req.rows);
        };
        assert!(data.rows.is_some(), "rows should be preserved");
        ArrowFields {
            write_stream: req.write_stream.clone(),
            schema: data
                .writer_schema
                .as_ref()
                .map(|s| s.serialized_schema.clone()),
        }
    }

    fn extract_proto(req: &AppendRowsRequest) -> ProtoFields {
        let Some(Rows::ProtoRows(data)) = &req.rows else {
            panic!("expected ProtoRows, got: {:?}", req.rows);
        };
        assert!(data.rows.is_some(), "rows should be preserved");
        ProtoFields {
            write_stream: req.write_stream.clone(),
            schema: data
                .writer_schema
                .as_ref()
                .and_then(|s| s.proto_descriptor.as_ref())
                .and_then(|d| d.name.clone()),
        }
    }
}
