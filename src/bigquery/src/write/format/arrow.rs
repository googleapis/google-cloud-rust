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

use crate::model::append_rows_request::ArrowData;
use crate::model::{AppendRowsRequest, ArrowRecordBatch, ArrowSchema};

/// Marker struct for [Arrow] data.
///
/// [Arrow]: https://arrow.apache.org/
#[derive(Debug)]
pub struct Arrow {
    pub(crate) schema: ArrowSchema,
}

impl super::DataFormat for Arrow {
    type Rows = ArrowRecordBatch;
}

impl super::sealed::DataFormat for Arrow {
    fn make_request(&self, write_stream: &str, rows: ArrowRecordBatch) -> AppendRowsRequest {
        AppendRowsRequest::new()
            .set_write_stream(write_stream)
            .set_arrow_rows(
                ArrowData::new()
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
    fn request() {
        let f = Arrow { schema: schema() };

        let req = f.make_request(&write_stream(), rows(1));
        let data = req.arrow_rows().expect("arrow rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.serialized_schema, "test");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_record_batch, "1");

        let req = f.make_request(&write_stream(), rows(2));
        let data = req.arrow_rows().expect("arrow rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.serialized_schema, "test");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_record_batch, "2");
    }

    fn rows(id: i64) -> ArrowRecordBatch {
        ArrowRecordBatch::new().set_serialized_record_batch(id.to_string())
    }
}
