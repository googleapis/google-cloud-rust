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

//! Test helpers for the `Write` client internals

use super::dispatcher::Dispatcher;
use super::entry::StreamEntry;
use super::pool::StreamPool;
use super::runner::WriteRequest;
use super::transport::Transport;
use crate::google::cloud::bigquery::storage::v1::append_rows_response::{AppendResult, Response};
use crate::google::cloud::bigquery::storage::v1::{AppendRowsRequest, AppendRowsResponse};
use crate::model::{ArrowSchema, ProtoSchema};
use bigquery_grpc_mock::google::cloud::bigquery::storage::v1;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(super) fn write_stream() -> String {
    "projects/p/datasets/d/tables/t/streams/s".to_string()
}

pub(super) fn schema() -> ArrowSchema {
    ArrowSchema::new().set_serialized_schema("test")
}

pub(super) fn proto_schema() -> ProtoSchema {
    let descriptor = wkt::DescriptorProto::default().set_name("TestMessage".to_string());
    ProtoSchema::new().set_proto_descriptor(descriptor)
}

pub(super) async fn test_transport<T: Into<String>>(endpoint: T) -> anyhow::Result<Transport> {
    let mut config = gaxi::options::ClientConfig::default();
    config.cred = Some(Anonymous::new().build());
    config.endpoint = Some(endpoint.into());
    Ok(Transport::new(config).await?)
}

// Both crates have their own copies of the protos. We can just serialize
// then deserialize to convert between the two, as performance is not a
// concern for these unit tests.
pub(super) fn convert(pb: &AppendRowsResponse) -> v1::AppendRowsResponse {
    use prost::Message;
    let v = pb.encode_to_vec();
    v1::AppendRowsResponse::decode(v.as_slice()).expect("encoding is always valid.")
}

pub(super) fn test_request(index: i64) -> AppendRowsRequest {
    AppendRowsRequest {
        write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
        offset: Some(index),
        ..Default::default()
    }
}

pub(super) fn test_response(index: i64) -> AppendRowsResponse {
    AppendRowsResponse {
        response: Some(Response::AppendResult(AppendResult {
            offset: Some(index),
        })),
        write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
        ..Default::default()
    }
}

// Return a dispatcher that sends requests on the provided channel.
pub(super) async fn test_dispatcher(
    req_tx: mpsc::UnboundedSender<WriteRequest>,
) -> anyhow::Result<Arc<Dispatcher>> {
    let transport = Arc::new(test_transport("http://ignored:1").await?);
    let pool = Arc::new(StreamPool::new(transport, 1));
    let dispatcher = Arc::new(Dispatcher::new(pool));

    // Override the stream entry's channel with the provided channel.
    let current = dispatcher.entry.load();
    let updated = StreamEntry {
        req_tx,
        ..(**current).clone()
    };
    dispatcher.entry.store(Arc::new(updated));
    Ok(dispatcher)
}
