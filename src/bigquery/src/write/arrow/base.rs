use crate::Result;
use crate::model::append_rows_request::ArrowData;
use crate::model::{AppendRowsRequest, ArrowRecordBatch, ArrowSchema, FinalizeWriteStreamResponse};

use crate::write::generated::gapic_storage::client::BigQueryWrite;
use crate::write::runner::Runner;
use crate::write::transport::Transport;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct BaseWriter {
    pub(crate) runner: Runner,
    pub(crate) write_stream: String,
    pub(crate) schema: ArrowSchema,
    pub(crate) client: BigQueryWrite,
}

impl BaseWriter {
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self {
        let runner = Runner::new(inner.clone());
        let client = BigQueryWrite::from_stub::<Transport>(inner);
        Self {
            runner,
            write_stream,
            schema,
            client,
        }
    }

    pub(crate) fn append_request(&self, rows: ArrowRecordBatch) -> AppendRowsRequest {
        AppendRowsRequest::new()
            .set_write_stream(&self.write_stream)
            .set_arrow_rows(
                ArrowData::new()
                    .set_writer_schema(self.schema.clone())
                    .set_rows(rows),
            )
    }

    pub(crate) async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.client
            .finalize_write_stream()
            .set_name(&self.write_stream)
            .send()
            .await
    }
}
