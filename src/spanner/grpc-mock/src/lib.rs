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

//! End-to-end mocks for the `google.spanner.v1.Spanner` gRPC service.
//!
//! Use this crate for end-to-end client library tests. Start a local server
//! implementing the `google.spanner.v1.Spanner` API, with the implementation
//! defined by a mock. Then test the client library against this mock.

mod mocks;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

pub use mocks::MockSpanner;

/// Starts a mock `google.spanner.v1.Spanner` gRPC service.
pub async fn start<T>(address: &str, service: T) -> anyhow::Result<(String, JoinHandle<()>)>
where
    T: google::spanner::v1::spanner_server::Spanner,
{
    let listener = tokio::net::TcpListener::bind(address).await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = tonic::transport::Server::builder()
            .add_service(google::spanner::v1::spanner_server::SpannerServer::new(service))
            .serve_with_incoming(stream)
            .await;
    });

    Ok((to_uri(addr), server))
}

fn to_uri(addr: SocketAddr) -> String {
    if addr.is_ipv6() {
        format!("http://[{}]:{}", addr.ip(), addr.port())
    } else {
        format!("http://{}:{}", addr.ip(), addr.port())
    }
}

#[allow(clippy::large_enum_variant)]
#[allow(clippy::enum_variant_names)]
pub mod google {
    pub mod api {
        include!("generated/protos/google.api.rs");
    }
    pub mod rpc {
        include!("generated/protos/google.rpc.rs");
    }
    pub mod spanner {
        pub mod v1 {
            include!("generated/protos/google.spanner.v1.rs");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::transport::Channel;
    use google::spanner::v1::spanner_client::SpannerClient;

    macro_rules! stub_tests {
        ($(($method:ident, $request:path)),*) => {
            $( pastey::paste! {
                #[tokio::test]
                async fn [<mock_stub_$method>]() -> anyhow::Result<()> {
                    let mut mock = MockSpanner::new();
                    mock.[<expect_$method>]()
                        .once()
                        .returning(|_| Err(tonic::Status::unimplemented("test-only")));

                    let (address, _server) = start("0.0.0.0:0", mock).await?;
                    let endpoint = Channel::from_shared(address.clone())?.connect().await?;
                    let mut client = SpannerClient::new(endpoint);
                    let status = client
                        .$method($request::default())
                        .await
                        .unwrap_err();
                    assert_eq!(status.code(), tonic::Code::Unimplemented);
                    assert_eq!(status.message(), "test-only");
                    Ok(())
                }
            })*
        };
    }

    stub_tests!(
        (create_session, google::spanner::v1::CreateSessionRequest),
        (batch_create_sessions, google::spanner::v1::BatchCreateSessionsRequest),
        (get_session, google::spanner::v1::GetSessionRequest),
        (list_sessions, google::spanner::v1::ListSessionsRequest),
        (delete_session, google::spanner::v1::DeleteSessionRequest),
        (execute_sql, google::spanner::v1::ExecuteSqlRequest),
        (execute_batch_dml, google::spanner::v1::ExecuteBatchDmlRequest),
        (read, google::spanner::v1::ReadRequest),
        (begin_transaction, google::spanner::v1::BeginTransactionRequest),
        (commit, google::spanner::v1::CommitRequest),
        (rollback, google::spanner::v1::RollbackRequest),
        (partition_query, google::spanner::v1::PartitionQueryRequest),
        (partition_read, google::spanner::v1::PartitionReadRequest)
    );
}
