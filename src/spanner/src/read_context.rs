use crate::client::Spanner;
use crate::model::Session;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use google_cloud_gax::error::rpc::Code;

macro_rules! impl_read_options_builder {
    ($name:ident) => {
        impl_read_options_builder!($name, inner);
    };
    ($name:ident, $($path:tt)*) => {
        impl $name {
            pub fn return_read_timestamp(mut self, return_read_timestamp: bool) -> Self {
                if let Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(ro)) = &mut self.$($path)*.options.mode {
                    ro.return_read_timestamp = return_read_timestamp;
                }
                self
            }

            pub fn read_timestamp(mut self, read_timestamp: chrono::DateTime<chrono::Utc>) -> Self {
                let ts = wkt::Timestamp::new(
                    read_timestamp.timestamp(),
                    read_timestamp.timestamp_subsec_nanos() as i32,
                ).expect("Timestamp out of supported range");

                if let Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(ro)) = &mut self.$($path)*.options.mode {
                    ro.timestamp_bound = Some(
                        crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::ReadTimestamp(Box::new(ts)),
                    );
                }
                self
            }

            pub fn exact_staleness(mut self, exact_staleness: std::time::Duration) -> Self {
                let duration = wkt::Duration::try_from(exact_staleness)
                    .expect("Duration out of supported range");

                if let Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(ro)) = &mut self.$($path)*.options.mode {
                    ro.timestamp_bound = Some(
                        crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::ExactStaleness(Box::new(duration)),
                    );
                }
                self
            }

            pub fn options(mut self, options: crate::generated::gapic_dataplane::model::TransactionOptions) -> Self {
                self.$($path)*.options = options;
                self
            }
        }
    };
}

pub(crate) use impl_read_options_builder;

pub(crate) struct MultiUseTransaction {
    pub(crate) context: ReadContext,
}

impl MultiUseTransaction {
    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> crate::Result<crate::result_set::ResultSet> {
        let mut statement: crate::statement::Statement = statement.into();
        if let Some(tag) = &self.context.transaction_tag {
            let mut options = statement.request_options.unwrap_or_default();
            options.transaction_tag = tag.clone();
            statement.request_options = Some(options);
        }
        self.context.execute_query(statement).await
    }

    pub(crate) fn is_begun(&self) -> bool {
        self.context.is_begun()
    }
}

#[derive(Clone)]
pub(crate) struct TransactionBuilder {
    pub(crate) client: Arc<Spanner>,
    pub(crate) session: Arc<Session>,
    pub(crate) transaction_tag: Option<String>,
    pub(crate) options: crate::generated::gapic_dataplane::model::TransactionOptions,
}

impl TransactionBuilder {
    pub(crate) fn new(client: Arc<Spanner>, session: Arc<Session>, options: crate::generated::gapic_dataplane::model::TransactionOptions) -> Self {
        Self {
            client,
            session,
            transaction_tag: None,
            options,
        }
    }
}

#[derive(Clone)]
pub struct MultiUseTransactionBuilder {
    pub(crate) transaction_builder: TransactionBuilder,
    pub(crate) explicit_begin_transaction: bool,
}

impl MultiUseTransactionBuilder {
    pub(crate) fn new(client: Arc<Spanner>, session: Arc<Session>, options: crate::generated::gapic_dataplane::model::TransactionOptions) -> Self {
        Self {
            transaction_builder: TransactionBuilder::new(client, session, options),
            explicit_begin_transaction: false,
        }
    }

    pub fn with_explicit_begin_transaction(mut self, explicit: bool) -> Self {
        self.explicit_begin_transaction = explicit;
        self
    }

    pub fn transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.transaction_builder.transaction_tag = Some(tag.into());
        self
    }

    pub(crate) async fn build(self) -> crate::Result<MultiUseTransaction> {
        let (tx_selector, read_timestamp) = if self.explicit_begin_transaction {
            let mut request = crate::model::BeginTransactionRequest::new();
            request.session = self.transaction_builder.session.name.clone();
            request.options = Some(self.transaction_builder.options.clone());

            if let Some(tag) = &self.transaction_builder.transaction_tag {
                let mut options = request.request_options.unwrap_or_default();
                options.transaction_tag = tag.clone();
                request.request_options = Some(options);
            }
            
            let response = self.transaction_builder.client.begin_transaction(request, crate::RequestOptions::default()).await?;
            (
                TxSelector::Static(crate::model::transaction_selector::Selector::Id(response.id)),
                response.read_timestamp
            )
        } else {
            (
                TxSelector::InlineBegin(Arc::new(std::sync::Mutex::new(InlineBeginState::NotBegun(
                    self.transaction_builder.options.clone(),
                )))),
                None
            )
        };

        let context = ReadContext::new(
            self.transaction_builder.client,
            self.transaction_builder.session,
            tx_selector,
            self.transaction_builder.transaction_tag,
        );

        if let Some(ts) = read_timestamp {
             if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds(), ts.nanos() as u32) {
                 let _ = context.read_timestamp.set(dt);
             }
        }



        Ok(MultiUseTransaction {
            context
        })
    }

}

pub struct SingleUseReadOnlyTransactionBuilder {
    pub(crate) inner: TransactionBuilder,
}

impl_read_options_builder!(SingleUseReadOnlyTransactionBuilder);


impl SingleUseReadOnlyTransactionBuilder {
    pub(crate) fn new(client: Arc<Spanner>, session: Arc<Session>) -> Self {
        Self {
            inner: TransactionBuilder::new(
                client,
                session,
                crate::generated::gapic_dataplane::model::TransactionOptions {
                    mode: Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(Box::new(
                        crate::generated::gapic_dataplane::model::transaction_options::ReadOnly {
                            return_read_timestamp: true,
                            ..Default::default()
                        },
                    ))),
                    ..Default::default()
                },
            ),
        }
    }

    pub fn min_read_timestamp(mut self, min_read_timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        let ts = wkt::Timestamp::new(
            min_read_timestamp.timestamp(),
            min_read_timestamp.timestamp_subsec_nanos() as i32,
        )
        .expect("Timestamp out of supported range");

        if let Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(ro)) = &mut self.inner.options.mode {
            ro.timestamp_bound = Some(
                crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::MinReadTimestamp(Box::new(ts)),
            );
        }
        self
    }

    pub fn max_staleness(mut self, max_staleness: std::time::Duration) -> Self {
        let duration =
            wkt::Duration::try_from(max_staleness).expect("Duration out of supported range");

        if let Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadOnly(ro)) = &mut self.inner.options.mode {
            ro.timestamp_bound = Some(
                crate::generated::gapic_dataplane::model::transaction_options::read_only::TimestampBound::MaxStaleness(Box::new(duration)),
            );
        }
        self
    }

    pub fn build(self) -> SingleUseReadOnlyTransaction {
        let tx_options = self.inner.options;

        SingleUseReadOnlyTransaction {
            context: ReadContext::new(
                self.inner.client,
                self.inner.session,
                TxSelector::Static(
                    crate::model::transaction_selector::Selector::SingleUse(Box::new(tx_options)),
                ),
                None,
            ),
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
    ) -> crate::Result<crate::result_set::ResultSet> {
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
    pub(crate) transaction_tag: Option<String>,
    pub(crate) seqno: AtomicI64,
    pub(crate) read_timestamp: Arc<std::sync::OnceLock<chrono::DateTime<chrono::Utc>>>,
    pub(crate) precommit_token: Arc<std::sync::Mutex<Option<crate::model::MultiplexedSessionPrecommitToken>>>,
}

impl ReadContext {
    pub(crate) fn new(
        client: Arc<Spanner>,
        session: Arc<Session>,
        transaction_selector: TxSelector,
        transaction_tag: Option<String>,
    ) -> Self {
        Self {
            client,
            session,
            transaction_selector,
            transaction_tag,
            seqno: AtomicI64::new(1),
            read_timestamp: Arc::new(std::sync::OnceLock::new()),
            precommit_token: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn read_timestamp(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.read_timestamp.get().copied()
    }

    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> crate::Result<crate::result_set::ResultSet> {
        let statement = statement.into();
        let (mut request, options) = statement.build_request(self.session.name.clone());
        request.seqno = self.seqno.fetch_add(1, Ordering::SeqCst);

        // TODO: Fix a potential deadlock when the following happens:
        //       1. execute_query is called on a multi-use transaction, but the caller does not call ResultSet::next()
        //       2. execute_query is again called on the same transaction. This now blocks, as the first call to
        //          execute_query included a BeginTransaction option, but the transaction ID will only be set once
        //          ResultSet::next is called on the first ResultSet.
        let (tx_selector, callback, read_timestamp) = self.resolve_transaction_selector().await?;
        request.transaction = Some(tx_selector);
        let stream = self.client.execute_streaming_sql(request, options).send().await?;
        
        let mut rs = if let Some(cb) = callback {
            crate::result_set::ResultSet::new_with_callback(stream, cb)
        } else {
            crate::result_set::ResultSet::new(stream)
        };

        // Handle precommit token updates from ResultSet
        let precommit_token_state = self.precommit_token.clone();
        rs.set_precommit_token_callback(Box::new(move |token| {
            let mut guard = precommit_token_state.lock().unwrap();
            let update = match &*guard {
                Some(current) => token.seq_num > current.seq_num,
                None => true,
            };
            if update {
                *guard = Some(token);
            }
        }));

        if let Some(rt) = read_timestamp {
            rs = rs.with_read_timestamp(rt);
        }
        Ok(rs)


    }

    pub(crate) async fn resolve_transaction_selector(
        &self,
    ) -> Result<
        (
            crate::model::TransactionSelector,
            Option<crate::result_set::TransactionCallback>,
            Option<Arc<std::sync::OnceLock<chrono::DateTime<chrono::Utc>>>>,
        ),
        crate::Error,
    > {
        let state_mutex = match &self.transaction_selector {
            TxSelector::Static(selector) => {
                let mut tx_selector = crate::model::TransactionSelector::new();
                tx_selector.selector = Some(selector.clone());
                let rt = match selector {
                    crate::model::transaction_selector::Selector::SingleUse(_) => Some(self.read_timestamp.clone()),
                    crate::model::transaction_selector::Selector::Begin(_) => Some(self.read_timestamp.clone()),
                    _ => None,
                };
                return Ok((tx_selector, None, rt));
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
            return Ok((tx_selector, None, None));
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
            return Ok((tx_selector, None, None));
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


            return Ok((tx_selector, Some(callback), Some(self.read_timestamp.clone())));
        }

        unreachable!()
    }
    pub(crate) async fn get_transaction_id(&self) -> Result<Vec<u8>, crate::Error> {
        match &self.transaction_selector {
            TxSelector::Static(selector) => match selector {
                crate::model::transaction_selector::Selector::Id(id) => Ok(id.to_vec()),
                _ => Err(crate::Error::service(
                    google_cloud_gax::error::rpc::Status::default()
                        .set_code(Code::Internal)
                        .set_message("invalid transaction selector state: expected Id"),
                )),
            },
            TxSelector::InlineBegin(state_mutex) => {
                let state = state_mutex.lock().unwrap_or_else(|p| p.into_inner()).clone();
                match state {
                    InlineBeginState::Begun(id) => Ok(id),
                    _ => Err(crate::Error::service(
                        google_cloud_gax::error::rpc::Status::default()
                            .set_code(Code::Internal)
                            .set_message("transaction not yet started or failed"),
                    )),
                }
            }
        }
    }
    pub(crate) fn is_begun(&self) -> bool {
        match &self.transaction_selector {
            TxSelector::Static(selector) => matches!(selector, crate::model::transaction_selector::Selector::Id(_)),
            TxSelector::InlineBegin(state_mutex) => {
                let state = state_mutex.lock().unwrap_or_else(|p| p.into_inner());
                matches!(*state, InlineBeginState::Begun(_))
            }
        }
    }
}

impl Clone for InlineBeginState {
    fn clone(&self) -> Self {
        match self {
            InlineBeginState::NotBegun(opts) => InlineBeginState::NotBegun(opts.clone()),
            InlineBeginState::Starting(_) => InlineBeginState::Starting(Vec::new()), // We can't easily clone waiting senders
            InlineBeginState::Begun(id) => InlineBeginState::Begun(id.clone()),
            InlineBeginState::Failed(status) => InlineBeginState::Failed(status.clone()),
        }
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

        let row1 = rs.next().await;
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

        let statement = Statement::new("SELECT * FROM users WHERE id = @id")
            .add_typed_param("id", &42i64, crate::types::int64())
            .build();

        let tx = db_client.single_use().build();
        let _result = tx.execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_query_options() {
        use crate::statement::Statement;

        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");

            assert!(req.query_options.is_some());
            let options = req.query_options.unwrap();
            assert_eq!(options.optimizer_version, "3");
            assert_eq!(options.optimizer_statistics_package, "pkg");

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let statement = Statement::new("SELECT 1")
            .optimizer_version("3")
            .optimizer_statistics_package("pkg")
            .build();

        let tx = db_client.single_use().build();
        let _result = tx.execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_request_options() {
        use crate::statement::Statement;

        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");

            assert!(req.request_options.is_some());
            let options = req.request_options.unwrap();
            assert_eq!(options.request_tag, "my-tag");
            assert_eq!(
                options.priority,
                spanner_grpc_mock::google::spanner::v1::request_options::Priority::High as i32
            );
            assert!(options.client_context.is_some());
            let context = options.client_context.unwrap();
            assert!(context.secure_context.contains_key("key"));

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let mut client_context =
            crate::generated::gapic_dataplane::model::request_options::ClientContext::default();
        client_context
            .secure_context
            .insert("key".to_string(), wkt::Value::default());

        let statement = Statement::new("SELECT 1")
            .request_tag("my-tag")
            .priority(crate::generated::gapic_dataplane::model::request_options::Priority::High)
            .client_context(client_context)
            .build();

        let tx = db_client.single_use().build();
        let _result = tx.execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_query_mode() {
        use crate::statement::Statement;

        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");

            assert_eq!(
                req.query_mode,
                spanner_grpc_mock::google::spanner::v1::execute_sql_request::QueryMode::Profile
                    as i32
            );

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let statement = Statement::new("SELECT 1")
            .query_mode(
                crate::generated::gapic_dataplane::model::execute_sql_request::QueryMode::Profile,
            )
            .build();

        let tx = db_client.single_use().build();
        let _result = tx.execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_data_boost_and_directed_read() {
        use crate::statement::Statement;

        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");

            assert!(req.data_boost_enabled);
            assert!(req.directed_read_options.is_some());
            let options = req.directed_read_options.unwrap();
            assert!(options.replicas.is_some());

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let directed_read_options =
            crate::generated::gapic_dataplane::model::DirectedReadOptions::default();
        let mut selection = crate::generated::gapic_dataplane::model::directed_read_options::ReplicaSelection::default();
        selection.location = "us-east1".to_string();
        let directed_read_options = directed_read_options.set_include_replicas(
            crate::generated::gapic_dataplane::model::directed_read_options::IncludeReplicas {
                replica_selections: vec![selection],
                auto_failover_disabled: false,
                _unknown_fields: serde_json::Map::new(),
            },
        );

        let statement = Statement::new("SELECT 1")
            .data_boost_enabled(true)
            .directed_read_options(directed_read_options)
            .build();

        let tx = db_client.single_use().build();
        let _result = tx.execute_query(statement).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_use_execute_query_with_implicit_params() {
        use crate::statement::Statement;
        use crate::value::ToValue;

        struct ImplicitParam;
        impl ToValue for ImplicitParam {
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
            .add_param("val", &ImplicitParam)
            .build();

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
        let tx = db_client.single_use().read_timestamp(timestamp);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await;
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
        let tx = db_client.single_use().min_read_timestamp(timestamp);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await;
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
            .read_timestamp(read_ts)
            .min_read_timestamp(min_read_ts);

        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await;
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
        let tx = db_client.single_use().exact_staleness(staleness);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await;
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
        let tx = db_client.single_use().max_staleness(staleness);
        let mut rs = tx
            .build()
            .execute_query("SELECT 1")
            .await
            .expect("Failed to call execute_query");

        let row1 = rs.next().await;
        assert!(row1.is_none());
    }

    #[tokio::test]
    async fn test_execute_query_with_seqno() {
        let mut mock = create_mock_with_session();

        mock.expect_execute_streaming_sql().times(2).returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");

            let seqno = req.seqno;
            assert!(seqno == 1 || seqno == 2, "Unexpected seqno: {}", seqno);

            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);

        let tx = db_client.single_use().build();
        
        // First query
        let _ = tx.execute_query("SELECT 1").await.unwrap();
        
        // Second query
        let _ = tx.execute_query("SELECT 1").await.unwrap();
    }

    #[tokio::test]
    async fn test_execute_query_with_timeout() {
        let mut mock = create_mock_with_session();
        mock.expect_execute_streaming_sql().withf(|req| {
            req.get_ref().sql == "SELECT 1" 
             // Note: We can't easily verify the retry policy timeout here because it is handled client-side 
             // in the retry loop before the request is made. The individual request timeout might be set, 
             // but checking it relies on tonic's behavior. 
             // For now we verify the SQL matches.
        }).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        let (db_client, _server) = setup_mock_db_client!(mock);
        let stmt = crate::statement::Statement::new("SELECT 1").timeout(std::time::Duration::from_secs(5));
        let tx = db_client.single_use().build();
        let _ = tx.execute_query(stmt).await.unwrap();
    }


}
