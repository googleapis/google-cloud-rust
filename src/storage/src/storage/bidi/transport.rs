// Copyright 2025 Google LLC
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

use super::pending_range::PendingRange;
use crate::error::ReadError;
use crate::google::storage::v2::BidiReadObjectResponse;
use crate::model::Object;
use crate::model_ext::ReadRange;
use crate::read_object::dynamic::ReadObjectResponse;
use crate::storage::bidi::RangeReader;
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

type ReadResult<T> = std::result::Result<T, ReadError>;

#[derive(Debug)]
pub(super) struct ObjectDescriptorTransport {
    object: Arc<Object>,
    tx: Sender<PendingRange>,
}

impl ObjectDescriptorTransport {
    pub async fn new<T>(mut connector: super::connector::Connector<T>) -> Result<Self>
    where
        T: super::connector::Client<Stream = tonic::Streaming<BidiReadObjectResponse>>
            + Clone
            + Sync,
    {
        use gaxi::prost::FromProto;

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let (initial, connection) = connector.connect(Vec::new()).await?;
        let object = initial
            .metadata
            .map(FromProto::cnv)
            .transpose()
            .map_err(Error::deser)?
            .ok_or_else(|| Error::deser("bidi_read_object is missing the object metadata value"))?;
        let object = Arc::new(object);
        let worker = super::worker::Worker::new(connection);
        let _handle = tokio::spawn(worker.run(connector, rx));
        Ok(Self { object, tx })
    }
}

impl super::stub::ObjectDescriptor for ObjectDescriptorTransport {
    fn object(&self) -> &Object {
        self.object.as_ref()
    }

    async fn read_range(&self, range: ReadRange) -> Box<dyn ReadObjectResponse + Send> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let range = PendingRange::new(tx, range, self.object.size);
        let _error = self.tx.send(range).await;
        println!("DEBUG DEBUG - ObjectDescriptor::read_range() - error = {_error:?}");
        Box::new(RangeReader::new(rx, self.object.clone(), self.tx.clone()))
    }
}
