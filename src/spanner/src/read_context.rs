use crate::client::Spanner;
use crate::model::{ExecuteSqlRequest, Session};
use std::sync::Arc;

macro_rules! impl_read_options_builder {
    ($name:ident) => {
        impl $name {
            pub fn with_return_read_timestamp(mut self, return_read_timestamp: bool) -> Self {
                self.options.return_read_timestamp = return_read_timestamp;
                self
            }

            pub fn with_read_timestamp(mut self, read_timestamp: chrono::DateTime<chrono::Utc>) -> Self {
                let ts = wkt::Timestamp::new(
                    read_timestamp.timestamp(),
                    read_timestamp.timestamp_subsec_nanos() as i32,
                ).expect("Timestamp out of supported range");

                self.options.timestamp_bound = Some(
                    crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::ReadTimestamp(Box::new(ts)),
                );
                self
            }

            pub fn with_exact_staleness(mut self, exact_staleness: std::time::Duration) -> Self {
                let duration = wkt::Duration::try_from(exact_staleness)
                    .expect("Duration out of supported range");

                self.options.timestamp_bound = Some(
                    crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::ExactStaleness(Box::new(duration)),
                );
                self
            }

            pub fn with_options(mut self, options: crate::generated::gapic_dataplane::model::transaction_options::ReadOnly) -> Self {
                self.options = options;
                self
            }
        }
    };
}

pub struct MultiUseReadOnlyTransactionBuilder {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
    pub(crate) options: crate::generated::gapic_dataplane::model::transaction_options::ReadOnly,
    pub(crate) explicit_begin_transaction: bool,
}

impl_read_options_builder!(MultiUseReadOnlyTransactionBuilder);

impl MultiUseReadOnlyTransactionBuilder {
    pub fn with_explicit_begin_transaction(mut self, explicit: bool) -> Self {
        self.explicit_begin_transaction = explicit;
        self
    }

    pub async fn build(self) -> Result<MultiUseReadOnlyTransaction, crate::Error> {
        use crate::model::transaction_options::Mode;

        let tx_selector = if self.explicit_begin_transaction {
            let mut request = crate::model::BeginTransactionRequest::new();
            request.session = self.session.name.clone();

            let mut tx_options = crate::model::TransactionOptions::new();
            tx_options.mode = Some(Mode::ReadOnly(Box::new(self.options.clone())));
            request.options = Some(tx_options);

            let response = self.client.begin_transaction(request).await?;
            TxSelector::Static(crate::model::transaction_selector::Selector::Id(
                response.id,
            ))
        } else {
            let mut tx_options = crate::model::TransactionOptions::new();
            tx_options.mode = Some(Mode::ReadOnly(Box::new(self.options.clone())));
            TxSelector::InlineBegin(Arc::new(std::sync::Mutex::new(
                InlineBeginState::NotBegun(tx_options),
            )))
        };

        Ok(MultiUseReadOnlyTransaction {
            context: ReadContext {
                client: self.client,
                session: self.session,
                transaction_selector: tx_selector,
            },
        })
    }
}

pub struct MultiUseReadOnlyTransaction {
    context: ReadContext,
}

impl MultiUseReadOnlyTransaction {
    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> Result<crate::result_set::ResultSet, crate::Error> {
        self.context.execute_query(statement).await
    }
}

pub struct SingleUseReadOnlyTransactionBuilder {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
    pub(crate) options: crate::generated::gapic_dataplane::model::transaction_options::ReadOnly,
}

impl_read_options_builder!(SingleUseReadOnlyTransactionBuilder);

impl SingleUseReadOnlyTransactionBuilder {
    pub fn with_min_read_timestamp(
        mut self,
        min_read_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        let ts = wkt::Timestamp::new(
            min_read_timestamp.timestamp(),
            min_read_timestamp.timestamp_subsec_nanos() as i32,
        )
        .expect("Timestamp out of supported range");

        self.options.timestamp_bound = Some(
            crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::MinReadTimestamp(Box::new(ts)),
        );
        self
    }

    pub fn with_max_staleness(mut self, max_staleness: std::time::Duration) -> Self {
        let duration =
            wkt::Duration::try_from(max_staleness).expect("Duration out of supported range");

        self.options.timestamp_bound = Some(
            crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::MaxStaleness(Box::new(duration)),
        );
        self
    }

    pub fn build(self) -> SingleUseReadOnlyTransaction {
        use crate::model::transaction_options::Mode;

        let mut tx_options = crate::model::TransactionOptions::new();
        tx_options.mode = Some(Mode::ReadOnly(Box::new(self.options)));

        SingleUseReadOnlyTransaction {
            context: ReadContext {
                client: self.client,
                session: self.session,
                transaction_selector: TxSelector::Static(
                    crate::model::transaction_selector::Selector::SingleUse(Box::new(tx_options)),
                ),
            },
        }
    }
}

pub struct SingleUseReadOnlyTransaction {
    context: ReadContext,
}

impl SingleUseReadOnlyTransaction {
    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> Result<crate::result_set::ResultSet, crate::Error> {
        self.context.execute_query(statement).await
    }
}

pub(crate) enum TxSelector {
    Static(crate::model::transaction_selector::Selector),
    InlineBegin(Arc<std::sync::Mutex<InlineBeginState>>),
}

pub(crate) enum InlineBeginState {
    NotBegun(crate::model::TransactionOptions),
    Starting(Vec<tokio::sync::oneshot::Sender<Result<Vec<u8>, gaxi::grpc::tonic::Status>>>),
    Begun(Vec<u8>),
    Failed(gaxi::grpc::tonic::Status),
}

impl Default for InlineBeginState {
    fn default() -> Self {
        InlineBeginState::NotBegun(crate::model::TransactionOptions::default())
    }
}

pub(crate) struct ReadContext {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
    pub(crate) transaction_selector: TxSelector,
}

impl ReadContext {
    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> Result<crate::result_set::ResultSet, crate::Error> {
        let statement = statement.into();
        let mut request = ExecuteSqlRequest::new();
        request.session = self.session.name.clone();
        request.sql = statement.sql;
        if !statement.params.is_empty() {
            request.params = Some(statement.params);
            request.param_types = statement.param_types;
        }

        let (tx_selector, callback) = self.resolve_transaction_selector().await?;
        request.transaction = Some(tx_selector);
        let stream = self.client.execute_streaming_sql(request).send().await?;
        if let Some(cb) = callback {
            Ok(crate::result_set::ResultSet::new_with_callback(stream, cb))
        } else {
            Ok(crate::result_set::ResultSet::new(stream))
        }
    }

    async fn resolve_transaction_selector(
        &self,
    ) -> Result<
        (
            crate::model::TransactionSelector,
            Option<crate::result_set::TransactionCallback>,
        ),
        crate::Error,
    > {
        let state_mutex = match &self.transaction_selector {
            TxSelector::Static(selector) => {
                let mut tx_selector = crate::model::TransactionSelector::new();
                tx_selector.selector = Some(selector.clone());
                return Ok((tx_selector, None));
            }
            TxSelector::InlineBegin(state_mutex) => state_mutex.clone(),
        };

        let (rx, options, begun_id) = {
            let mut state = state_mutex
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            match &mut *state {
                InlineBeginState::Begun(id) => (None, None, Some(id.clone())),
                InlineBeginState::Starting(waiters) => {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    waiters.push(tx);
                    (Some(rx), None, None)
                }
                InlineBeginState::NotBegun(opts) => {
                    let options = opts.clone();
                    *state = InlineBeginState::Starting(Vec::new());
                    (None, Some(options), None)
                }
                InlineBeginState::Failed(status) => {
                    return Err(crate::Error::service(
                        google_cloud_gax::error::rpc::Status::default()
                            .set_code(status.code() as i32)
                            .set_message(status.message().to_string()),
                    ));
                }
            }
        };

        if let Some(id) = begun_id {
            let mut tx_selector = crate::model::TransactionSelector::new();
            tx_selector.selector =
                Some(crate::model::transaction_selector::Selector::Id(id.into()));
            return Ok((tx_selector, None));
        }

        if let Some(rx) = rx {
            let id = rx.await.unwrap().map_err(|s| {
                crate::Error::service(
                    google_cloud_gax::error::rpc::Status::default()
                        .set_code(s.code() as i32)
                        .set_message(s.message().to_string()),
                )
            })?;
            let mut tx_selector = crate::model::TransactionSelector::new();
            tx_selector.selector =
                Some(crate::model::transaction_selector::Selector::Id(id.into()));
            return Ok((tx_selector, None));
        }

        if let Some(options) = options {
            let mut tx_selector = crate::model::TransactionSelector::new();
            tx_selector.selector = Some(crate::model::transaction_selector::Selector::Begin(
                Box::new(options),
            ));

            let callback: crate::result_set::TransactionCallback =
                Box::new(move |res: Result<Vec<u8>, gaxi::grpc::tonic::Status>| {
                    let mut state = state_mutex
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let waiters = match std::mem::take(&mut *state) {
                        InlineBeginState::Starting(waiters) => waiters,
                        _ => unreachable!("State modified unexpectedly"),
                    };

                    match &res {
                        Ok(id) => {
                            *state = InlineBeginState::Begun(id.clone());
                        }
                        Err(e) => {
                            *state = InlineBeginState::Failed(e.clone());
                        }
                    }

                    for tx in waiters {
                        let _ = tx.send(res.clone());
                    }
                });

            return Ok((tx_selector, Some(callback)));
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use spanner_grpc_mock::{MockSpanner, start};


    fn create_mock_stream() -> <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream{
        let stream = tokio_stream::iter(vec![Ok(
            spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                metadata: None,
                values: vec![],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        )]);
        Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream
    }

    fn create_mock_with_session() -> MockSpanner {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });
        mock
    }

    macro_rules! setup_mock_db_client {
        ($mock:expr) => {{
            let (address, _server) = start("0.0.0.0:0", $mock)
                .await
                .expect("Failed to start mock server");
            let client = Spanner::builder()
                .with_endpoint(address)
                .with_credentials(Anonymous::new().build())
                .build()
                .await
                .expect("Failed to build client");

            let db_client = client
                .database_client("projects/test-project/instances/test-instance/databases/test-db")
                .await
                .expect("Failed to create DatabaseClient");
            (db_client, _server)
        }};
    }

    #[tokio::test]
    async fn test_single_use_execute_query() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/test-project/instances/test-instance/databases/test-db/sessions/123");
            assert_eq!(req.sql, "SELECT 1");

            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            assert!(ro.return_read_timestamp);
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let tx = db_client.single_use();
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_params() {
        use crate::statement::Statement;

        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
            );
            assert_eq!(req.sql, "SELECT * FROM users WHERE id = @id");

            assert!(req.params.is_some());
            let params = req.params.unwrap();
            let id_param = params.fields.get("id").expect("id param should be present");
            assert_eq!(
                id_param.kind,
                Some(prost_types::value::Kind::StringValue("42".to_string()))
            );

            assert!(req.param_types.contains_key("id"));
            let id_type = req.param_types.get("id").unwrap();
            assert_eq!(
                id_type.code,
                spanner_grpc_mock::google::spanner::v1::TypeCode::Int64 as i32
            );
            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let statement = Statement::new("SELECT * FROM users WHERE id = @id").add_typed_param(
            "id",
            &42i64,
            crate::types::int64(),
        );

        let tx = db_client.single_use().build();
        let _result = tx.execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_implicit_params() {
        use crate::statement::{Statement, ToSpannerValue};

        struct ImplicitParam;
        impl ToSpannerValue for ImplicitParam {
            fn to_value(&self) -> serde_json::Value {
                serde_json::Value::String("implicit_val".to_string())
            }
        }

        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.params.is_some());
            let params = req.params.unwrap();
            let val_param = params
                .fields
                .get("val")
                .expect("val param should be present");
            assert_eq!(
                val_param.kind,
                Some(prost_types::value::Kind::StringValue(
                    "implicit_val".to_string()
                ))
            );

            // The main assertion for implicit types: Ensure "val" is NOT in param_types
            assert!(!req.param_types.contains_key("val"));

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let statement = Statement::new("SELECT * FROM objects WHERE val = @val")
            .add_param("val", &ImplicitParam);

        let tx = db_client.single_use();
        let _result = tx.build().execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_read_timestamp() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            let bound = ro.timestamp_bound.as_ref().unwrap();
                            match bound {
                                spanner_grpc_mock::google::spanner::v1::transaction_options::read_only::TimestampBound::ReadTimestamp(ts) => {
                                    assert_eq!(ts.seconds, 1234);
                                    assert_eq!(ts.nanos, 5678);
                                }
                                _ => panic!("Expected ReadTimestamp bound"),
                            }
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let timestamp = chrono::DateTime::from_timestamp(1234, 5678).unwrap();
        let tx = db_client.single_use().with_read_timestamp(timestamp);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_min_read_timestamp() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            let bound = ro.timestamp_bound.as_ref().unwrap();
                            match bound {
                                spanner_grpc_mock::google::spanner::v1::transaction_options::read_only::TimestampBound::MinReadTimestamp(ts) => {
                                    assert_eq!(ts.seconds, 1234);
                                    assert_eq!(ts.nanos, 5678);
                                }
                                _ => panic!("Expected MinReadTimestamp bound"),
                            }
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let timestamp = chrono::DateTime::from_timestamp(1234, 5678).unwrap();
        let tx = db_client.single_use().with_min_read_timestamp(timestamp);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_both_timestamps() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            let bound = ro.timestamp_bound.as_ref().unwrap();
                            // The last one called should be min_read_timestamp.
                            match bound {
                                spanner_grpc_mock::google::spanner::v1::transaction_options::read_only::TimestampBound::MinReadTimestamp(ts) => {
                                    assert_eq!(ts.seconds, 5678);
                                    assert_eq!(ts.nanos, 1234);
                                }
                                _ => panic!("Expected MinReadTimestamp bound since it was called last"),
                            }
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let read_ts = chrono::DateTime::from_timestamp(1234, 5678).unwrap();
        let min_read_ts = chrono::DateTime::from_timestamp(5678, 1234).unwrap();

        let tx = db_client
            .single_use()
            .with_read_timestamp(read_ts)
            .with_min_read_timestamp(min_read_ts);

        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_exact_staleness() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            let bound = ro.timestamp_bound.as_ref().unwrap();
                            match bound {
                                spanner_grpc_mock::google::spanner::v1::transaction_options::read_only::TimestampBound::ExactStaleness(d) => {
                                    assert_eq!(d.seconds, 15);
                                    assert_eq!(d.nanos, 500_000_000);
                                }
                                _ => panic!("Expected ExactStaleness bound"),
                            }
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let staleness = std::time::Duration::new(15, 500_000_000);
        let tx = db_client.single_use().with_exact_staleness(staleness);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_max_staleness() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::SingleUse(tx_opts) => {
                    let mode = tx_opts.mode.as_ref().unwrap();
                    match mode {
                        spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(ro) => {
                            let bound = ro.timestamp_bound.as_ref().unwrap();
                            match bound {
                                spanner_grpc_mock::google::spanner::v1::transaction_options::read_only::TimestampBound::MaxStaleness(d) => {
                                    assert_eq!(d.seconds, 15);
                                    assert_eq!(d.nanos, 500_000_000);
                                }
                                _ => panic!("Expected MaxStaleness bound"),
                            }
                        }
                        _ => panic!("Expected ReadOnly mode"),
                    }
                }
                _ => panic!("Expected SingleUse selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let staleness = std::time::Duration::new(15, 500_000_000);
        let tx = db_client.single_use().with_max_staleness(staleness);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_explicit_multi_use_execute_query() {
        let mut mock = create_mock_with_session();

        // Mock BeginTransaction
        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
            );
            assert!(req.options.is_some());
            let options = req.options.unwrap();
            match options.mode.unwrap() {
                spanner_grpc_mock::google::spanner::v1::transaction_options::Mode::ReadOnly(_) => {}
                _ => panic!("Expected ReadOnly mode"),
            }

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![5, 6, 7, 8],
                    read_timestamp: None,
                    precommit_token: None,
                },
            ))
        });

        // Mock ExecuteStreamingSql
        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();
            match selector {
                spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Id(id) => {
                    assert_eq!(id, &vec![5, 6, 7, 8]);
                }
                _ => panic!("Expected Id selector"),
            }

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let tx = db_client
            .read_only_transaction()
            .with_explicit_begin_transaction(true)
            .build()
            .await
            .expect("Failed to start read-only transaction");

        let mut rs = tx
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await.expect("Failed to get next row");
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_multi_use_execute_query() {
        let mut mock = create_mock_with_session();

        // Mock ExecuteStreamingSql (inline BeginTransaction happens here)
        mock.expect_execute_streaming_sql().times(2).returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            let selector = req.transaction.as_ref().unwrap().selector.as_ref().unwrap();

            let is_first_request = if let spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Begin(_) = selector {
                true
            } else if let spanner_grpc_mock::google::spanner::v1::transaction_selector::Selector::Id(id) = selector {
                assert_eq!(id, &vec![1, 2, 3, 4]);
                false
            } else {
                panic!("Expected Begin or Id selector");
            };

            let stream = tokio_stream::iter(vec![
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        row_type: None,
                        transaction: if is_first_request {
                            Some(spanner_grpc_mock::google::spanner::v1::Transaction {
                                id: vec![1, 2, 3, 4],
                                read_timestamp: None,
                                precommit_token: None,
                            })
                        } else {
                            None
                        },
                        undeclared_parameters: None,
                    }),
                    values: vec![],
                    chunked_value: false,
                    resume_token: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                    last: true,
                })
            ]);
            Ok(gaxi::grpc::tonic::Response::new(
                Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream
            ))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let tx = db_client
            .read_only_transaction()
            .build()
            .await
            .expect("Failed to start read-only transaction");

        // The first execution uses `Begin(...)` and loads the ID into the mutex
        let mut rs1 = tx
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs1.next().await.expect("Failed to get next row");
        assert!(row1.is_none());

        // The second execution uses `Id([1, 2, 3, 4])` from the resolved mutex state
        let mut rs2 = tx
            .execute_query("SELECT 2")
            .await
            .expect("Failed to call execute_query");

        let row2 = rs2.next().await.expect("Failed to get next row");
        assert!(row2.is_none());
    }
}
