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

use crate::model::append_rows_request::ProtoData;
use crate::model::{AppendRowsRequest, ProtoRows, ProtoSchema};

/// Represents the Protobuf data format for a writer.
#[derive(Debug)]
pub(crate) struct Proto {
    pub(crate) schema: ProtoSchema,
}

impl super::DataFormat for Proto {
    type Rows = ProtoRows;
}

impl super::sealed::DataFormat for Proto {
    fn format_name(&self) -> &'static str {
        "proto"
    }

    fn make_request(&self, write_stream: &str, rows: ProtoRows) -> AppendRowsRequest {
        AppendRowsRequest::new()
            .set_write_stream(write_stream)
            .set_proto_rows(
                ProtoData::new()
                    .set_writer_schema(self.schema.clone())
                    .set_rows(rows),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::super::sealed::DataFormat;
    use super::*;
    use crate::write::test::*;

    #[test]
    fn format_name() {
        let f = Proto {
            schema: proto_schema(),
        };
        assert_eq!(f.format_name(), "proto");
    }

    #[test]
    fn request() {
        let f = Proto {
            schema: proto_schema(),
        };

        let req = f.make_request(&write_stream(), rows(1));
        let data = req.proto_rows().expect("proto rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.proto_descriptor.as_ref().unwrap().name, "TestMessage");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_rows, vec![bytes::Bytes::from("1")]);

        let req = f.make_request(&write_stream(), rows(2));
        let data = req.proto_rows().expect("proto rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.proto_descriptor.as_ref().unwrap().name, "TestMessage");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_rows, vec![bytes::Bytes::from("2")]);
    }

    fn rows(id: i64) -> ProtoRows {
        ProtoRows::new().set_serialized_rows(vec![bytes::Bytes::from(id.to_string())])
    }
}
