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

use crate::Result;
use crate::arrow::DefaultWriter;
use crate::model::ArrowSchema;
use crate::transport::Transport;
use std::sync::Arc;

/// A builder to create a stream writer
#[derive(Clone, Debug)]
pub struct WriterBuilder {
    inner: Arc<Transport>,
    schema: ArrowSchema,
}

impl WriterBuilder {
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn new(inner: Arc<Transport>, schema: ArrowSchema) -> Self {
        Self { inner, schema }
    }

    // TODO(#6224) - add an example showing the format of `table`
    /// Create a writer for the [default stream] for the given table.
    ///
    /// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
    pub fn default<T: Into<String>>(self, table: T) -> Result<DefaultWriter> {
        // TODO(#6249) - validate table resource format
        let mut write_stream = table.into();
        write_stream.push_str("/streams/_default");
        Ok(DefaultWriter::new(self.inner, write_stream, self.schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tests::test_transport;

    #[tokio::test]
    async fn default() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let schema = ArrowSchema::new().set_serialized_schema("test");
        let builder = WriterBuilder::new(transport, schema.clone());
        let writer = builder.default("projects/p/tables/t")?;
        assert_eq!(writer.write_stream, "projects/p/tables/t/streams/_default");
        assert_eq!(writer.schema, schema);
        Ok(())
    }
}
