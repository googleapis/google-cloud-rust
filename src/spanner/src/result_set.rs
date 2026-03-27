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

use crate::error::internal_error;
use crate::precommit::PrecommitTokenTracker;
use crate::result_set_metadata::ResultSetMetadata;
use crate::row::Row;
use crate::server_streaming::stream::PartialResultSetStream;
use gaxi::prost::FromProto;
use std::collections::VecDeque;

#[cfg(feature = "unstable-stream")]
use futures::Stream;

/// `ResultSet` contains the rows of a query result.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::{ResultSet, Row};
/// # async fn process_result_set(mut rs: ResultSet) -> Result<(), google_cloud_spanner::Error> {
/// while let Some(row) = rs.next().await {
///     let row: Row = row?;
///     // Process the row
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ResultSet {
    stream: PartialResultSetStream,
    buffered_values: Vec<prost_types::Value>,
    chunked: bool,
    ready_rows: VecDeque<Row>,
    metadata: Option<ResultSetMetadata>,
    precommit_token_tracker: PrecommitTokenTracker,
}

/// Errors that can occur when interacting with a [`ResultSet`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ResultSetError {
    /// The metadata was requested before the first row was fetched.
    #[error("metadata called before first row was fetched")]
    MetadataNotAvailable,
}

impl ResultSet {
    /// Creates a new result set.
    pub(crate) fn new(
        stream: PartialResultSetStream,
        precommit_token_tracker: PrecommitTokenTracker,
    ) -> Self {
        Self {
            stream,
            buffered_values: Vec::new(),
            chunked: false,
            ready_rows: VecDeque::new(),
            metadata: None,
            precommit_token_tracker,
        }
    }

    /// Returns the metadata of the result set.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ResultSet, Row};
    /// # async fn fetch_metadata(mut rs: ResultSet) -> Result<(), Box<dyn std::error::Error>> {
    /// if let Some(row) = rs.next().await.transpose()? {
    ///     let metadata = rs.metadata()?;
    ///     for column in metadata.column_names() {
    ///         println!("Column name: {}", column);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The metadata is only available after the first call to [`next`](Self::next).
    /// If called before the first `next()` call, it returns a [`ResultSetError::MetadataNotAvailable`] error.
    pub fn metadata(&self) -> Result<ResultSetMetadata, ResultSetError> {
        self.metadata
            .clone()
            .ok_or(ResultSetError::MetadataNotAvailable)
    }

    /// Fetches the next row from the result set.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{ResultSet, Row};
    /// # async fn fetch_next(mut rs: ResultSet) -> Result<(), google_cloud_spanner::Error> {
    /// if let Some(row) = rs.next().await.transpose()? {
    ///     // Process the row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Returns `None` when all rows have been retrieved.
    pub async fn next(&mut self) -> Option<crate::Result<Row>> {
        if let Some(row) = self.ready_rows.pop_front() {
            return Some(Ok(row));
        }

        while let Some(prs_result) = self.stream.next_message().await {
            let prs = match prs_result {
                Ok(prs) => prs,
                Err(e) => return Some(Err(e)),
            };
            self.precommit_token_tracker.update(
                prs.precommit_token
                    .map(|t| t.cnv().expect("failed to convert precommit token")),
            );

            match (self.metadata.as_ref(), prs.metadata) {
                (Some(_), None) => {}
                (None, None) => {
                    return Some(Err(internal_error(
                        "First PartialResultSet did not contain metadata",
                    )));
                }
                (Some(_), Some(_)) => {
                    return Some(Err(internal_error(
                        "Additional metadata after first result set",
                    )));
                }
                (None, Some(m)) => {
                    self.metadata = Some(ResultSetMetadata::new(Some(m)));
                }
            }

            if prs.values.is_empty() {
                continue;
            }
            let metadata = self.metadata.as_ref().unwrap();
            if metadata.column_types.is_empty() {
                return Some(Err(internal_error(
                    "PartialResultSet contained values but no column metadata was provided",
                )));
            }

            let mut values_iter = prs.values.into_iter();
            if self.chunked {
                if let Some(last_val) = self.buffered_values.last_mut() {
                    if let Some(first_new) = values_iter.next() {
                        if let Err(e) = merge_values(last_val, first_new) {
                            return Some(Err(e));
                        }
                    }
                }
            }

            self.buffered_values.extend(values_iter);
            self.chunked = prs.chunked_value;

            while self.buffered_values.len() >= metadata.column_types.len() {
                let column_count = metadata.column_types.len();
                if self.buffered_values.len() == column_count && self.chunked {
                    break;
                }

                let row_values: Vec<crate::value::Value> = self
                    .buffered_values
                    .drain(..column_count)
                    .map(crate::value::Value)
                    .collect();
                self.ready_rows.push_back(Row {
                    values: row_values,
                    metadata: metadata.clone(),
                });
            }

            if let Some(row) = self.ready_rows.pop_front() {
                return Some(Ok(row));
            }
        }

        if self.chunked {
            // This is not expected to happen and indicates that Spanner returned data that
            // violates the contract. In this case we return a service error with error code
            // Internal.
            return Some(Err(internal_error("Stream ended with chunked_value=true")));
        }

        None
    }

    /// Converts the [`ResultSet`] into a [`Stream`].
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_spanner::client::ResultSet;
    /// # use futures::TryStreamExt;
    /// # use std::future::ready;
    /// # async fn example(result_set: ResultSet) -> Result<(), google_cloud_spanner::Error> {
    /// let rows: Vec<_> = result_set
    ///     .into_stream()
    ///     .try_filter(|row| {
    ///         let id = row.get::<String, _>("Id");
    ///         ready(id == "id1")
    ///     })
    ///     .try_collect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This consumes the [`ResultSet`] and returns a stream of rows.
    #[cfg(feature = "unstable-stream")]
    pub fn into_stream(self) -> impl Stream<Item = crate::Result<Row>> {
        use futures::stream::unfold;
        unfold(self, |mut result_set| async move {
            result_set.next().await.map(|row| (row, result_set))
        })
    }
}

/// Merges two values from successive `PartialResultSet`s into a single value.
///
/// Cloud Spanner can return a single logical row or column value split across multiple
/// `PartialResultSet` messages. This occurs when a value (especially large strings or
/// arrays) exceeds the message size limits of the underlying stream. In these cases,
/// the `chunked_value` flag is set on the first `PartialResultSet`, indicating that the
/// final value in the message's `values` array is incomplete and must be combined with
/// the first value in the `values` array of the subsequent `PartialResultSet`.
///
/// This function handles the concatenation of split `StringValue` and `ListValue` types.
fn merge_values(target: &mut prost_types::Value, source: prost_types::Value) -> crate::Result<()> {
    use prost_types::value::Kind;
    match (&mut target.kind, source.kind) {
        (Some(Kind::StringValue(s)), Some(Kind::StringValue(source_s))) => {
            s.push_str(&source_s);
            Ok(())
        }
        (Some(Kind::ListValue(target_list)), Some(Kind::ListValue(mut source_list))) => {
            if source_list.values.is_empty() {
                return Ok(());
            }
            if target_list.values.is_empty() {
                target_list.values = source_list.values;
                return Ok(());
            }

            let source_first = source_list.values.remove(0);
            if let Some(target_last) = target_list.values.last_mut() {
                match (&target_last.kind, &source_first.kind) {
                    (Some(Kind::StringValue(_)), Some(Kind::StringValue(_)))
                    | (Some(Kind::ListValue(_)), Some(Kind::ListValue(_))) => {
                        merge_values(target_last, source_first)?;
                    }
                    _ => {
                        target_list.values.push(source_first);
                    }
                }
            } else {
                target_list.values.push(source_first);
            }
            target_list.values.extend(source_list.values);
            Ok(())
        }
        // This is not expected to happen and indicates that Spanner returned data that
        // violates the contract. In this case we return a service error with error code
        // Internal.
        _ => Err(internal_error(
            "Incompatible types for merging chunked values",
        )),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::client::Spanner;
    use gaxi::grpc::tonic::Response;
    use prost_types::Value;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner as SpannerTrait;
    use spanner_grpc_mock::google::spanner::v1::struct_type::Field;
    use spanner_grpc_mock::google::spanner::v1::{PartialResultSet, ResultSetMetadata, StructType};
    use spanner_grpc_mock::start;

    pub(crate) fn string_val(s: &str) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::StringValue(s.to_string())),
        }
    }

    fn list_val(vals: Vec<Value>) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue { values: vals },
            )),
        }
    }

    fn metadata(cols: usize) -> Option<ResultSetMetadata> {
        let mut fields = vec![];
        for i in 0..cols {
            fields.push(Field {
                name: format!("col{}", i),
                r#type: None,
            });
        }
        Some(ResultSetMetadata {
            row_type: Some(StructType { fields }),
            transaction: None,
            undeclared_parameters: None,
        })
    }

    async fn run_mock_query(results: Vec<PartialResultSet>) -> ResultSet {
        let mut mock = MockSpanner::new();
        mock.expect_execute_streaming_sql()
            .returning(move |_request| {
                let res = results.clone();
                let stream = tokio_stream::iter(res.into_iter().map(Ok));
                Ok(Response::new(
                    Box::pin(stream) as <MockSpanner as SpannerTrait>::ExecuteStreamingSqlStream,
                ))
            });

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "session".to_string(),
                    labels: std::collections::HashMap::new(),
                    create_time: None,
                    approximate_last_use_time: None,
                    creator_role: "".to_string(),
                    multiplexed: true,
                },
            ))
        });

        let (address, _server) = start("127.0.0.1:0", mock)
            .await
            .expect("Failed to start mock server");

        let client: Spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client: crate::database_client::DatabaseClient =
            client.database_client("db").build().await.unwrap();
        let tx: crate::read_only_transaction::SingleUseReadOnlyTransaction =
            db_client.single_use().build();
        let rs: ResultSet = tx.execute_query("SELECT 1").await.unwrap();
        rs
    }

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(ResultSet: std::fmt::Debug, Send, Sync);
    }

    #[tokio::test]
    async fn test_result_set_zero_rows() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            last: true,
            cache_update: None,
        }])
        .await;

        let next = rs.next().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn test_result_set_one_row() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![string_val("a"), string_val("b")],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            last: true,
            cache_update: None,
        }])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values().len(), 2);
        assert_eq!(row.raw_values()[0].0, string_val("a"));
        assert_eq!(row.raw_values()[1].0, string_val("b"));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_chunked_values_string() {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![string_val("hello ")],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: false,
                cache_update: None,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val("world")],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: true,
                cache_update: None,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values().len(), 1);
        if let Some(prost_types::value::Kind::StringValue(ref s)) = row.raw_values()[0].0.kind {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected StringValue");
        }
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_chunked_values_list() {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![list_val(vec![string_val("A")])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: false,
                cache_update: None,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![string_val("B")])],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                last: true,
                cache_update: None,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values().len(), 1);
        if let Some(prost_types::value::Kind::ListValue(ref l)) = row.raw_values()[0].0.kind {
            assert_eq!(l.values.len(), 1);
            if let Some(prost_types::value::Kind::StringValue(ref s)) = l.values[0].kind {
                assert_eq!(s, "AB");
            } else {
                panic!("Expected StringValue");
            }
        } else {
            panic!("Expected ListValue");
        }
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_bool_array() {
        fn bool_val(b: bool) -> Value {
            Value {
                kind: Some(prost_types::value::Kind::BoolValue(b)),
            }
        }
        fn null_val() -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }
        }

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![
                    list_val(vec![bool_val(true)]),
                    list_val(vec![bool_val(false), null_val(), bool_val(true)]),
                ],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![bool_val(true), bool_val(true)])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![
                    list_val(vec![null_val(), null_val(), bool_val(false)]),
                    list_val(vec![bool_val(true)]),
                ],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row1 = rs.next().await.unwrap().unwrap();
        assert_eq!(row1.raw_values()[0].0, list_val(vec![bool_val(true)]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values()[0].0,
            list_val(vec![
                bool_val(false),
                null_val(),
                bool_val(true),
                bool_val(true),
                bool_val(true),
                null_val(),
                null_val(),
                bool_val(false)
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values()[0].0, list_val(vec![bool_val(true)]));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_int64_array() {
        fn null_val() -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }
        }

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![
                    list_val(vec![string_val("10")]),
                    list_val(vec![string_val("1"), string_val("2"), null_val()]),
                ],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![null_val(), string_val("5")])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![
                    list_val(vec![null_val(), string_val("7"), string_val("8")]),
                    list_val(vec![string_val("20")]),
                ],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row1 = rs.next().await.unwrap().unwrap();
        assert_eq!(row1.raw_values()[0].0, list_val(vec![string_val("10")]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values()[0].0,
            list_val(vec![
                string_val("1"),
                string_val("2"),
                null_val(),
                null_val(),
                string_val("5"),
                null_val(),
                string_val("7"),
                string_val("8")
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values()[0].0, list_val(vec![string_val("20")]));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_precommit_token_tracked() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(1),
            precommit_token: Some(
                spanner_grpc_mock::google::spanner::v1::MultiplexedSessionPrecommitToken {
                    precommit_token: b"test_token".to_vec(),
                    seq_num: 99,
                },
            ),
            ..Default::default()
        }])
        .await;

        // Force tracking mode since run_mock_query uses a ReadOnly transaction (NoOp).
        rs.precommit_token_tracker = PrecommitTokenTracker::new();

        // Read a row to trigger precommit token extraction
        assert!(
            rs.next().await.is_none(),
            "Expected no rows, but received one"
        );

        // Validate the tracker correctly intercepted and preserved the token
        let token = rs
            .precommit_token_tracker
            .get()
            .expect("token should be tracked");
        assert_eq!(token.seq_num, 99);
        assert_eq!(token.precommit_token, bytes::Bytes::from("test_token"));
    }
}
