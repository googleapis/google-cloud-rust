use crate::google::spanner::v1::ResultSetMetadata;
use gaxi::grpc::tonic::Status;
use prost_types::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use crate::google::spanner::v1::Type;
use crate::row::Row;

pub type TransactionCallback = Box<dyn FnOnce(Result<Vec<u8>, Status>) + Send + Sync>;


/// A query result from Spanner.
///
/// Use `ResultSet` to iterate over the rows of a query result.
///
/// # Examples
///
/// ```rust,no_run
/// use google_cloud_spanner::row::Row;
/// use google_cloud_spanner::result_set::ResultSet;
///
/// async fn process_result_set(mut rs: ResultSet) -> Result<(), Box<dyn std::error::Error>> {
///     while let Some(row) = rs.next().await {
///         let row = row?;
///         // use try_get to handle potential errors safely
///         let id: i64 = row.try_get("id")?;
///         let name: Option<String> = row.try_get("name")?;
///         
///         println!("id: {}, name: {:?}", id, name);
///     }
///     Ok(())
/// }
/// ```
///
/// # Navigation
///
/// The [next](ResultSet::next) method should be used to move the result set to the next row.
/// It returns `Ok(Some(Row))` if there is another row, `Ok(None)` if the result set is exhausted,
/// or `Err(Status)` if an error occurs.
///
/// # Accessing Data
///
/// Data within a [Row](crate::row::Row) can be accessed using the `get` or `try_get` methods.
///
/// *   **Column Selection**: You can access columns by their name (string) or their zero-based index (integer).
/// *   **Panics**: The `get` method will panic if:
///     *   The column name or index is invalid.
///     *   The requested type does not match the column type.
///     *   The column contains a `NULL` value and the requested type is not an `Option<T>`.
/// *   **Safe Access**: Use `try_get` if you want to handle these cases as errors instead of panics.
/// *   **Null Handling**: Spanner columns can contain `NULL` values. It is recommended to use `Option<T>`
///     for any field that might be nullable. Failing to use `Option<T>` for a null value with `get` will
///     panic, and with `try_get` will return an error.
pub struct ResultSet {
    pub metadata: Option<ResultSetMetadata>,
    stream: crate::client::stream::ServerStream,
    ready_rows: VecDeque<Row>,
    row_values: Vec<Value>,
    column_names: Arc<HashMap<String, usize>>,
    column_types: Arc<Vec<Type>>,
    chunked: bool,
    transaction_callback: Option<TransactionCallback>,
    read_timestamp_out: Option<Arc<std::sync::OnceLock<chrono::DateTime<chrono::Utc>>>>,
    precommit_token_callback: Option<Box<dyn FnMut(crate::model::MultiplexedSessionPrecommitToken) + Send + Sync>>,
}



impl ResultSet {
    pub(crate) fn new(stream: crate::client::stream::ServerStream) -> Self {
        Self {
            metadata: None,
            stream,
            ready_rows: VecDeque::new(),
            row_values: Vec::new(),
            column_names: Arc::new(HashMap::new()),
            column_types: Arc::new(Vec::new()),
            chunked: false,
            transaction_callback: None,
            read_timestamp_out: None,
            precommit_token_callback: None,
        }


    }

    pub(crate) fn new_with_callback(
        stream: crate::client::stream::ServerStream,
        callback: TransactionCallback,
    ) -> Self {
        Self {
            metadata: None,
            stream,
            ready_rows: VecDeque::new(),
            row_values: Vec::new(),
            column_names: Arc::new(HashMap::new()),
            column_types: Arc::new(Vec::new()),
            chunked: false,
            transaction_callback: Some(callback),
            read_timestamp_out: None,
            precommit_token_callback: None,
        }
    }

    pub(crate) fn with_read_timestamp(mut self, read_timestamp: Arc<std::sync::OnceLock<chrono::DateTime<chrono::Utc>>>) -> Self {
        self.read_timestamp_out = Some(read_timestamp);
        self
    }

    pub(crate) fn set_precommit_token_callback(
        &mut self,
        callback: Box<dyn FnMut(crate::model::MultiplexedSessionPrecommitToken) + Send + Sync>,
    ) {
        self.precommit_token_callback = Some(callback);
    }




    /// Advances the stream to the next row.
    ///
    /// Returns:
    /// * `Some(Ok(Row))` if a new row is available.
    /// * `None` if the stream has finished.
    /// * `Some(Err(crate::Error))` if an error occurred.
    pub async fn next(&mut self) -> Option<crate::Result<Row>> {
        // If we have rows already fully assembled from a previous stream chunk, return them.
        if let Some(row) = self.ready_rows.pop_front() {
            return Some(Ok(row));
        }

        loop {
            let prs = match self.stream.next_message().await {
                Some(Ok(p)) => p,
                None => break,
                Some(Err(e)) => {
                    if let Some(cb) = self.transaction_callback.take() {
                        let status = e.status().cloned().unwrap_or_else(|| google_cloud_gax::error::rpc::Status::default().set_message(e.to_string()));
                        let tonic_status = Status::new(
                            gaxi::grpc::tonic::Code::from(status.code as i32),
                            status.message,
                        );
                        cb(Err(tonic_status));
                    }
                    return Some(Err(e));
                }
            };

            if let Some(token) = prs.precommit_token {
                if let Some(cb) = &mut self.precommit_token_callback {
                    use gaxi::prost::FromProto;
                    if let Ok(model_token) = token.cnv() {
                         cb(model_token);
                    }
                }
            }

            if self.metadata.is_none() {
                if let Some(meta) = &prs.metadata {
                    self.consume_metadata(meta);
                }
            }

            if prs.values.is_empty() {
                continue;
            }

            let mut values_iter = prs.values.into_iter();
            if self.chunked {
                if let Some(last_val) = self.row_values.last_mut() {
                    if let Some(first_new) = values_iter.next() {
                         if let Err(e) = Self::merge_values(last_val, first_new).map_err(to_error) {
                             return Some(Err(e));
                         }
                    }
                }
            }

            self.row_values.extend(values_iter);

            // Yield fully completed rows.
            if let Some(meta) = &self.metadata {
                let columns = meta
                    .row_type
                    .as_ref()
                    .map(|rt| rt.fields.len())
                    .unwrap_or(0);

                if columns > 0 {
                    while self.row_values.len() >= columns {
                        // Check if the _current_ boundary is hitting the end of partial_result_set
                        // and it's flagged as chunked.
                        if self.row_values.len() == columns && prs.chunked_value {
                            break;
                        }

                        let row: Vec<Value> = self.row_values.drain(..columns).collect();
                        self.ready_rows.push_back(Row {
                            raw_values: row,
                            column_names: self.column_names.clone(),
                            column_types: self.column_types.clone(),
                        });
                    }
                }
            }

            self.chunked = prs.chunked_value;

            if let Some(row) = self.ready_rows.pop_front() {
                return Some(Ok(row));
            }
        }

        // Stream has ended.
        if let Some(cb) = self.transaction_callback.take() {
            cb(Err(Status::internal(
                "Stream ended without returning a transaction ID",
            )));
        }

        if !self.row_values.is_empty() && !self.chunked {
            // we have lingering elements making up a full row
            let row: Vec<Value> = self.row_values.drain(..).collect();
            return Some(Ok(Row {
                raw_values: row,
                column_names: self.column_names.clone(),
                column_types: self.column_types.clone(),
            }));
        }

        if self.chunked {
            return Some(Err(to_error(Status::internal("stream closed with pending chunked value"))));
        }

        None
    }

    fn consume_metadata(&mut self, metadata: &ResultSetMetadata) {
        self.metadata = Some(metadata.clone());
        if let Some(row_type) = &metadata.row_type {
            let mut names = HashMap::new();
            let mut types = Vec::new();
            for (i, field) in row_type.fields.iter().enumerate() {
                names.insert(field.name.clone(), i);
                if let Some(t) = &field.r#type {
                    types.push(t.clone());
                } else {
                    types.push(Type::default());
                }
            }
            self.column_names = Arc::new(names);
            self.column_types = Arc::new(types);
        }

        if let Some(cb) = self.transaction_callback.take() {
            if let Some(tx) = &metadata.transaction {
                cb(Ok(tx.id.to_vec()));
            } else {
                cb(Err(Status::internal("No transaction returned in metadata")));
            }
        }

        if let Some(out) = &self.read_timestamp_out {
            if let Some(tx) = &metadata.transaction {
                if let Some(ts) = &tx.read_timestamp {
                    if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
                        let _ = out.set(dt);
                    }
                }

            }
        }
    }

    fn merge_values(target: &mut Value, source: Value) -> Result<(), Status> {
        let type_err =
            || Status::failed_precondition("incompatible type in chunked PartialResultSet");

        match (&mut target.kind, source.kind) {
            (
                Some(prost_types::value::Kind::StringValue(s)),
                Some(prost_types::value::Kind::StringValue(append_s)),
            ) => {
                s.push_str(&append_s);
                Ok(())
            }
            (
                Some(prost_types::value::Kind::ListValue(l1)),
                Some(prost_types::value::Kind::ListValue(mut l2)),
            ) => {
                if l2.values.is_empty() {
                    return Ok(());
                }
                if l1.values.is_empty() {
                    l1.values = l2.values;
                    return Ok(());
                }

                if let Some(last) = l1.values.last_mut() {
                    let first = l2.values.first().unwrap();
                    match (&last.kind, &first.kind) {
                        (
                            Some(prost_types::value::Kind::StringValue(_)),
                            Some(prost_types::value::Kind::StringValue(_)),
                        )
                        | (
                            Some(prost_types::value::Kind::ListValue(_)),
                            Some(prost_types::value::Kind::ListValue(_)),
                        ) => {
                            let first = l2.values.remove(0);
                            Self::merge_values(last, first)?;
                        }
                        _ => {
                            // Elements are incompatible for internal chunk merging,
                            // they must be distinct array items.
                        }
                    }
                }

                l1.values.extend(l2.values);
                Ok(())
            }
            _ => Err(type_err()),
        }
    }
}

fn to_error(status: Status) -> crate::Error {
    crate::Error::service(
        google_cloud_gax::error::rpc::Status::default()
            .set_code(status.code() as i32)
            .set_message(status.message().to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use gaxi::grpc::tonic::Response;
    use spanner_grpc_mock::MockSpanner;
    use spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner as SpannerTrait;
    use spanner_grpc_mock::google::spanner::v1::struct_type::Field;
    use spanner_grpc_mock::google::spanner::v1::{PartialResultSet, ResultSetMetadata, StructType};
    use spanner_grpc_mock::start;

    fn string_val(s: &str) -> Value {
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

        // Boilerplate session mock needed for db client
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

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = client.database_client("db").await.unwrap();
        let tx = db_client.single_use();
        tx.build().execute_query("SELECT 1").await.unwrap()
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
            cache_update: None,
            last: true,
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
            cache_update: None,
            last: true,
        }])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values.len(), 2);
        
        // Basic integration check
        assert_eq!(row.raw_values[0], string_val("a"));
        assert_eq!(row.raw_values[1], string_val("b"));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_multiple_rows() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(2),
            values: vec![
                string_val("a"),
                string_val("b"),
                string_val("c"),
                string_val("d"),
            ],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            cache_update: None,
            last: true,
        }])
        .await;

        let row1 = rs.next().await.unwrap().unwrap();
        assert_eq!(row1.raw_values.len(), 2);
        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(row2.raw_values.len(), 2);
        assert!(rs.next().await.is_none());
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_one_column() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(1),
            values: vec![string_val("a"), string_val("b")],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            cache_update: None,
            last: true,
        }])
        .await;

        let row1 = rs.next().await.unwrap().unwrap();
        assert_eq!(row1.raw_values.len(), 1);
        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(row2.raw_values.len(), 1);
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_many_rows_per_prs() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(1),
            values: vec![string_val("a"), string_val("b"), string_val("c")],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            cache_update: None,
            last: true,
        }])
        .await;

        assert!(rs.next().await.is_some());
        assert!(rs.next().await.is_some());
        assert!(rs.next().await.is_some());
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_result_set_sub_row_per_prs() {
        // Here, each PRS returns only one piece of the column (out of 2 cols)
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(2),
                values: vec![string_val("a")],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val("b")],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values.len(), 2);
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
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val("world")],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values.len(), 1);
        if let Some(prost_types::value::Kind::StringValue(ref s)) = row.raw_values[0].kind {
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
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![string_val("B")])],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values.len(), 1);
        if let Some(prost_types::value::Kind::ListValue(ref l)) = row.raw_values[0].kind {
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
    async fn test_result_set_chunked_values_list_unmergeable() {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![list_val(vec![Value {
                    kind: Some(prost_types::value::Kind::BoolValue(true)),
                }])],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![list_val(vec![Value {
                    kind: Some(prost_types::value::Kind::BoolValue(false)),
                }])],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        ])
        .await;

        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.raw_values.len(), 1);
        if let Some(prost_types::value::Kind::ListValue(ref l)) = row.raw_values[0].kind {
            assert_eq!(l.values.len(), 2);
            if let Some(prost_types::value::Kind::BoolValue(ref b1)) = l.values[0].kind {
                assert_eq!(*b1, true);
            } else {
                panic!("Expected BoolValue");
            }
            if let Some(prost_types::value::Kind::BoolValue(ref b2)) = l.values[1].kind {
                assert_eq!(*b2, false);
            } else {
                panic!("Expected BoolValue");
            }
        } else {
            panic!("Expected ListValue");
        }
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_stream_closed() {
        let mut rs = run_mock_query(vec![PartialResultSet {
            metadata: metadata(1),
            values: vec![string_val("abcdefg")],
            chunked_value: true,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            cache_update: None,
            last: true, // Emulate stream ending prematurely
        }])
        .await;

        let result = rs.next().await;
        assert!(result.is_some());
        let err = result.unwrap().unwrap_err();
        assert_eq!(
            err.status().unwrap().code,
            google_cloud_gax::error::rpc::Code::Internal
        );
    }

    #[tokio::test]
    async fn test_multi_response_chunking_strings() {
        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![string_val("before"), string_val("abcdefg")],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val("hijklmnop")],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val("qrstuvwxyz"), string_val("after")],
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
        assert_eq!(row1.raw_values[0], string_val("before"));
        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(row2.raw_values[0], string_val("abcdefghijklmnopqrstuvwxyz"));
        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values[0], string_val("after"));
        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_bytes() {
        let _expected_bytes = b"abcdefghijklmnopqrstuvwxyz";
        let base64_str = "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo="; // Base64 encoding for the alphabet
        let chunk1 = &base64_str[0..10];
        let chunk2 = &base64_str[10..20];
        let chunk3 = &base64_str[20..];

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![string_val("YmVmb3Jl"), string_val(chunk1)],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val(chunk2)],
                chunked_value: true,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            },
            PartialResultSet {
                metadata: None,
                values: vec![string_val(chunk3), string_val("YWZ0ZXI=")],
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
        assert_eq!(row1.raw_values[0], string_val("YmVmb3Jl")); // "before"
        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(row2.raw_values[0], string_val(base64_str));
        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values[0], string_val("YWZ0ZXI=")); // "after"
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
        assert_eq!(row1.raw_values[0], list_val(vec![bool_val(true)]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values[0],
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
        assert_eq!(row3.raw_values[0], list_val(vec![bool_val(true)]));

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
        assert_eq!(row1.raw_values[0], list_val(vec![string_val("10")]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values[0],
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
        assert_eq!(row3.raw_values[0], list_val(vec![string_val("20")]));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_float64_array() {
        fn float_val(f: f64) -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NumberValue(f)),
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
                    list_val(vec![float_val(10.0)]),
                    list_val(vec![null_val(), float_val(2.0), float_val(3.0)]),
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
                values: vec![list_val(vec![float_val(4.0), null_val()])],
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
                    list_val(vec![float_val(6.0), float_val(7.0), null_val()]),
                    list_val(vec![float_val(20.0)]),
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
        assert_eq!(row1.raw_values[0], list_val(vec![float_val(10.0)]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values[0],
            list_val(vec![
                null_val(),
                float_val(2.0),
                float_val(3.0),
                float_val(4.0),
                null_val(),
                float_val(6.0),
                float_val(7.0),
                null_val()
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values[0], list_val(vec![float_val(20.0)]));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_string_array() {
        fn null_val() -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }
        }

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![
                    list_val(vec![string_val("before")]),
                    list_val(vec![string_val("a"), string_val("b"), null_val()]),
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
                values: vec![list_val(vec![string_val("d"), null_val()])],
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
                    list_val(vec![string_val("f"), null_val(), string_val("h")]),
                    list_val(vec![string_val("after")]),
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
        assert_eq!(row1.raw_values[0], list_val(vec![string_val("before")]));

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values[0],
            list_val(vec![
                string_val("a"),
                string_val("b"),
                null_val(),
                string_val("d"),
                null_val(),
                string_val("f"),
                null_val(),
                string_val("h")
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(row3.raw_values[0], list_val(vec![string_val("after")]));

        assert!(rs.next().await.is_none());
    }

    #[tokio::test]
    async fn test_multi_response_chunking_struct_array() {
        fn null_val() -> Value {
            Value {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            }
        }
        fn struct_val(a: Option<&str>, b: Option<i64>) -> Value {
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("a".to_string(), a.map(string_val).unwrap_or_else(null_val));
            fields.insert(
                "b".to_string(),
                b.map(|i| string_val(&i.to_string()))
                    .unwrap_or_else(null_val),
            );
            Value {
                kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                    fields,
                })),
            }
        }

        let mut rs = run_mock_query(vec![
            PartialResultSet {
                metadata: metadata(1),
                values: vec![
                    list_val(vec![struct_val(Some("before"), Some(10))]),
                    list_val(vec![
                        struct_val(Some("a"), Some(1)),
                        struct_val(Some("b"), Some(2)),
                        struct_val(Some("c"), Some(3)),
                    ]),
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
                values: vec![list_val(vec![null_val(), struct_val(None, Some(5))])],
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
                    list_val(vec![
                        null_val(),
                        struct_val(Some("g"), Some(7)),
                        struct_val(Some("h"), Some(8)),
                    ]),
                    list_val(vec![struct_val(Some("after"), Some(20))]),
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
        assert_eq!(
            row1.raw_values[0],
            list_val(vec![struct_val(Some("before"), Some(10))])
        );

        let row2 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row2.raw_values[0],
            list_val(vec![
                struct_val(Some("a"), Some(1)),
                struct_val(Some("b"), Some(2)),
                struct_val(Some("c"), Some(3)),
                null_val(),
                struct_val(None, Some(5)),
                null_val(),
                struct_val(Some("g"), Some(7)),
                struct_val(Some("h"), Some(8))
            ])
        );

        let row3 = rs.next().await.unwrap().unwrap();
        assert_eq!(
            row3.raw_values[0],
            list_val(vec![struct_val(Some("after"), Some(20))])
        );

        assert!(rs.next().await.is_none());
    }
}
