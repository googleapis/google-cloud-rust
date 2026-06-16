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

use crate::Error;
use crate::connection::connection::ExecutionResult;
use crate::connection::{Dialect, SavepointSupport};
use crate::database_client::DatabaseClient;
use crate::read_only_transaction::{BeginTransactionOption, MultiUseReadOnlyTransaction};
use crate::read_write_transaction::{ReadWriteTransaction, ReadWriteTransactionBuilder};
use crate::result_set::ResultSet;
use crate::statement::Statement;
use crate::timestamp_bound::TimestampBound;
use google_cloud_gax::error::rpc::{Code, Status};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;

#[async_trait::async_trait]
pub(crate) trait TransactionRetryCoordinator: std::fmt::Debug + Send + Sync {
    async fn retry_transaction(
        &self,
        failed_statement_index: usize,
        last_resume_token: &bytes::Bytes,
        current_consumed_rows: usize,
        current_checksum: [u8; 16],
    ) -> Result<ResultSet, crate::Error>;

    fn sync_statement_state(
        &self,
        statement_index: usize,
        consumed_rows: usize,
        checksum: [u8; 16],
    );
}

/// Represents a stateful transaction unit of work on a Connection.
#[async_trait::async_trait]
pub(crate) trait ConnectionTransaction: Send + Sync {
    async fn execute_query(
        &mut self,
        staleness: Option<TimestampBound>,
        statement: Statement,
    ) -> Result<ExecutionResult, Error>;
    async fn execute_update(&mut self, statement: Statement) -> Result<ExecutionResult, Error>;
    async fn execute_batch_update(
        &mut self,
        batch: crate::batch::BatchDml,
    ) -> Result<ExecutionResult, Error>;
    async fn commit(self: Box<Self>) -> Result<(), Error>;
    async fn rollback(self: Box<Self>) -> Result<(), Error>;
    fn is_autocommit(&self) -> bool {
        false
    }
    fn savepoint(&mut self, _name: &str, _dialect: Dialect) -> Result<(), Error> {
        Err(Error::deser(
            "Savepoints are not supported for this transaction type",
        ))
    }
    fn release_savepoint(&mut self, _name: &str) -> Result<(), Error> {
        Err(Error::deser(
            "Savepoints are not supported for this transaction type",
        ))
    }
    async fn rollback_to_savepoint(
        &mut self,
        _name: &str,
        _savepoint_support: SavepointSupport,
    ) -> Result<(), Error> {
        Err(Error::deser(
            "Savepoints are not supported for this transaction type",
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RetriableStatement {
    Update {
        statement: Statement,
        expected_update_count: i64,
    },
    BatchUpdate {
        batch: crate::batch::BatchDml,
        expected_update_counts: Vec<i64>,
    },
    Query {
        statement: Statement,
        consumed_rows: usize,
        expected_checksum: [u8; 16],
    },
    Failed {
        statement: Statement,
        expected_error_code: Option<Code>,
    },
    BatchFailed {
        batch: crate::batch::BatchDml,
        expected_error_code: Option<Code>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Savepoint {
    pub(crate) name: String,
    pub(crate) statement_position: usize,
    pub(crate) is_auto_savepoint: bool,
}

#[derive(Debug)]
pub(crate) struct ActiveTransactionState {
    pub(crate) transaction: ReadWriteTransaction,
    pub(crate) history: Vec<RetriableStatement>,
    pub(crate) retry_aborts_internally: bool,
    pub(crate) savepoints: Vec<Savepoint>,
    pub(crate) rolled_back_to_savepoint_error: Option<Error>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReadWriteTransactionUnit {
    inner: Arc<Mutex<ActiveTransactionState>>,
    client: DatabaseClient,
    transaction_tag: Option<String>,
    savepoint_support: SavepointSupport,
}

impl ReadWriteTransactionUnit {
    pub(crate) fn new(
        transaction: ReadWriteTransaction,
        client: DatabaseClient,
        transaction_tag: Option<String>,
        retry_aborts_internally: bool,
        savepoint_support: SavepointSupport,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ActiveTransactionState {
                transaction,
                history: Vec::new(),
                retry_aborts_internally,
                savepoints: Vec::new(),
                rolled_back_to_savepoint_error: None,
            })),
            client,
            transaction_tag,
            savepoint_support,
        }
    }

    async fn retry_transaction_loop(
        &self,
        failed_statement_index: Option<usize>,
        _last_resume_token: &bytes::Bytes,
    ) -> Result<Option<ResultSet>, Error> {
        let max_attempts = 5;
        let mut attempt = 0;
        let mut backoff = RetryBackoff::new();

        loop {
            attempt += 1;
            let new_transaction = match self
                .prepare_new_transaction(attempt, max_attempts, &mut backoff)
                .await
            {
                Ok(transaction) => transaction,
                Err(error) if is_aborted_error(&error) && attempt < max_attempts => {
                    continue;
                }
                Err(error) => return Err(error),
            };

            let (active_result_set, success, last_aborted_error) = self
                .replay_history(&new_transaction, failed_statement_index)
                .await?;

            if !success {
                if attempt < max_attempts {
                    let error = last_aborted_error.unwrap();
                    let delay = crate::transaction_retry_policy::extract_retry_delay(&error)
                        .unwrap_or_else(|| backoff.next_delay());
                    sleep(delay).await;
                    continue;
                }
                return Err(Error::deser("Transaction aborted: too many retry attempts"));
            }

            let mut guard = self.inner.lock().unwrap();
            guard.transaction = new_transaction;

            return Ok(active_result_set);
        }
    }

    async fn prepare_new_transaction(
        &self,
        attempt: usize,
        max_attempts: usize,
        backoff: &mut RetryBackoff,
    ) -> Result<ReadWriteTransaction, Error> {
        let mut builder = ReadWriteTransactionBuilder::new(self.client.clone());
        if let Some(ref tag) = self.transaction_tag {
            builder = builder.set_transaction_tag(tag.clone());
        }
        builder = builder.with_begin_transaction_option(BeginTransactionOption::ExplicitBegin);

        match builder.build(None).await {
            Ok(transaction) => Ok(transaction),
            Err(error) => {
                if is_aborted_error(&error) && attempt < max_attempts {
                    let delay = crate::transaction_retry_policy::extract_retry_delay(&error)
                        .unwrap_or_else(|| backoff.next_delay());
                    sleep(delay).await;
                }
                Err(error)
            }
        }
    }

    async fn replay_history(
        &self,
        new_transaction: &ReadWriteTransaction,
        failed_statement_index: Option<usize>,
    ) -> Result<(Option<ResultSet>, bool, Option<Error>), Error> {
        let history = {
            let guard = self.inner.lock().unwrap();
            guard.history.clone()
        };

        let mut success = true;
        let mut active_result_set = None;
        let mut last_aborted_error = None;

        for (index, statement) in history.iter().enumerate() {
            let is_failed_statement = failed_statement_index == Some(index);
            match self
                .replay_statement(new_transaction, statement, is_failed_statement, index)
                .await
            {
                Ok(Some(result_set)) => active_result_set = Some(result_set),
                Ok(None) => {}
                Err(error)
                    if is_aborted_error(&error) && !is_concurrent_modification_error(&error) =>
                {
                    success = false;
                    last_aborted_error = Some(error);
                    break;
                }
                Err(error) => return Err(error),
            }
        }

        Ok((active_result_set, success, last_aborted_error))
    }

    async fn replay_statement(
        &self,
        new_transaction: &ReadWriteTransaction,
        statement: &RetriableStatement,
        is_failed_statement: bool,
        statement_index: usize,
    ) -> Result<Option<ResultSet>, Error> {
        match statement {
            RetriableStatement::Update {
                statement,
                expected_update_count,
            } => {
                self.replay_update(new_transaction, statement, *expected_update_count)
                    .await?;
                Ok(None)
            }
            RetriableStatement::BatchUpdate {
                batch,
                expected_update_counts,
            } => {
                self.replay_batch_update(new_transaction, batch, expected_update_counts)
                    .await?;
                Ok(None)
            }
            RetriableStatement::Query {
                statement,
                consumed_rows,
                expected_checksum,
            } => {
                self.replay_query(
                    new_transaction,
                    statement,
                    *consumed_rows,
                    *expected_checksum,
                    is_failed_statement,
                    statement_index,
                )
                .await
            }
            RetriableStatement::Failed {
                statement,
                expected_error_code,
            } => {
                self.replay_failed(
                    new_transaction,
                    statement,
                    *expected_error_code,
                    is_failed_statement,
                )
                .await?;
                Ok(None)
            }
            RetriableStatement::BatchFailed {
                batch,
                expected_error_code,
            } => {
                self.replay_batch_failed(new_transaction, batch, *expected_error_code)
                    .await?;
                Ok(None)
            }
        }
    }

    async fn replay_update(
        &self,
        transaction: &ReadWriteTransaction,
        statement: &Statement,
        expected_update_count: i64,
    ) -> Result<(), Error> {
        let count = transaction.execute_update(statement.clone()).await?;
        if count != expected_update_count {
            return Err(self.concurrent_modification_error(
                "Concurrent transaction modification detected: DML update count mismatch on retry",
            ));
        }
        Ok(())
    }

    async fn replay_batch_update(
        &self,
        transaction: &ReadWriteTransaction,
        batch: &crate::batch::BatchDml,
        expected_update_counts: &[i64],
    ) -> Result<(), Error> {
        let counts = transaction.execute_batch_update(batch.clone()).await?;
        if counts != expected_update_counts {
            return Err(self.concurrent_modification_error(
                "Concurrent transaction modification detected: DML batch update counts mismatch on retry",
            ));
        }
        Ok(())
    }

    async fn replay_query(
        &self,
        transaction: &ReadWriteTransaction,
        statement: &Statement,
        consumed_rows: usize,
        expected_checksum: [u8; 16],
        is_failed_statement: bool,
        statement_index: usize,
    ) -> Result<Option<ResultSet>, Error> {
        let mut result_set = transaction.execute_query(statement.clone()).await?;
        let mut checksum = crate::connection::checksum::ChecksumCalculator::new();
        let mut rows_read = 0;
        let mut aborted_error = None;

        while rows_read < consumed_rows {
            match result_set.next().await {
                Some(Ok(row)) => {
                    checksum.update_row(&row);
                    rows_read += 1;
                }
                Some(Err(error)) if is_aborted_error(&error) => {
                    aborted_error = Some(error);
                    break;
                }
                Some(Err(error)) => {
                    return Err(error);
                }
                None => {
                    break;
                }
            }
        }

        if let Some(error) = aborted_error {
            return Err(error);
        }

        if rows_read < consumed_rows {
            return Err(self.concurrent_modification_error(
                "Concurrent transaction modification detected: Query returned fewer rows on retry",
            ));
        }

        let calculated_checksum = checksum.finalize();
        if calculated_checksum != expected_checksum {
            return Err(self.concurrent_modification_error(
                "Concurrent transaction modification detected: Query row checksum mismatch on retry",
            ));
        }

        if is_failed_statement {
            let self_shared = Arc::new(self.clone());
            result_set = result_set.with_connection_retry(self_shared, statement_index, true);
            Ok(Some(result_set))
        } else {
            Ok(None)
        }
    }

    async fn replay_failed(
        &self,
        transaction: &ReadWriteTransaction,
        statement: &Statement,
        expected_error_code: Option<Code>,
        is_failed_statement: bool,
    ) -> Result<(), Error> {
        let result = if is_failed_statement && is_dml_statement(statement.sql()) {
            match transaction.execute_update(statement.clone()).await {
                Ok(_) => Err(self.concurrent_modification_error(
                    "Statement succeeded on retry but failed originally",
                )),
                Err(error) => Ok(error),
            }
        } else {
            match transaction.execute_query(statement.clone()).await {
                Ok(mut result_set) => match result_set.next().await {
                    Some(Err(error)) => Ok(error),
                    _ => Err(self.concurrent_modification_error(
                        "Statement succeeded on retry but failed originally",
                    )),
                },
                Err(error) => Ok(error),
            }
        };

        let error = result?;
        if is_aborted_error(&error) {
            return Err(error);
        }
        if get_error_code(&error) != expected_error_code {
            return Err(self.concurrent_modification_error(
                "Concurrent transaction modification detected: Statement failed with different error code on retry",
            ));
        }
        Ok(())
    }

    async fn replay_batch_failed(
        &self,
        transaction: &ReadWriteTransaction,
        batch: &crate::batch::BatchDml,
        expected_error_code: Option<Code>,
    ) -> Result<(), Error> {
        let error = match transaction.execute_batch_update(batch.clone()).await {
            Ok(_) => {
                return Err(self.concurrent_modification_error(
                    "Batch statement succeeded on retry but failed originally",
                ));
            }
            Err(error) => error,
        };

        if is_aborted_error(&error) {
            return Err(error);
        }
        if get_error_code(&error) != expected_error_code {
            return Err(self.concurrent_modification_error(
                "Concurrent transaction modification detected: Batch statement failed with different error code on retry",
            ));
        }
        Ok(())
    }

    fn concurrent_modification_error(&self, message: &str) -> Error {
        let status = Status::default()
            .set_code(Code::Aborted)
            .set_message(message.to_string());
        Error::service(status)
    }

    async fn with_retry<F, Fut, T>(&self, mut execute_op: F) -> Result<T, Error>
    where
        F: FnMut(ReadWriteTransaction) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let inner = self.inner.clone();
        let max_attempts = 5;
        let mut attempt = 0;
        let mut backoff = RetryBackoff::new();

        loop {
            let rolled_back_error = {
                let mut guard = inner.lock().unwrap();
                guard.rolled_back_to_savepoint_error.take()
            };

            if let Some(error) = rolled_back_error {
                if self.savepoint_support == SavepointSupport::FailAfterRollback {
                    return Err(Error::deser(
                        "Using a read/write transaction after rolling back to a savepoint is not supported with SavepointSupport=FailAfterRollback",
                    ));
                }
                let delay = crate::transaction_retry_policy::extract_retry_delay(&error)
                    .unwrap_or_else(|| backoff.next_delay());
                sleep(delay).await;
                self.retry_transaction_loop(None, &bytes::Bytes::new())
                    .await?;
                continue;
            }

            attempt += 1;
            let transaction = {
                let guard = inner.lock().unwrap();
                guard.transaction.clone()
            };

            match execute_op(transaction.clone()).await {
                Ok(result) => {
                    let mut guard = inner.lock().unwrap();
                    guard.transaction = transaction;
                    return Ok(result);
                }
                Err(error) => {
                    if is_aborted_error(&error) {
                        let retry_aborts = {
                            let guard = inner.lock().unwrap();
                            guard.retry_aborts_internally
                        };
                        if retry_aborts && attempt < max_attempts {
                            let delay =
                                crate::transaction_retry_policy::extract_retry_delay(&error)
                                    .unwrap_or_else(|| backoff.next_delay());
                            sleep(delay).await;
                            self.retry_transaction_loop(None, &bytes::Bytes::new())
                                .await?;
                            continue;
                        }
                    }
                    return Err(error);
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl TransactionRetryCoordinator for ReadWriteTransactionUnit {
    async fn retry_transaction(
        &self,
        failed_statement_index: usize,
        last_resume_token: &bytes::Bytes,
        current_consumed_rows: usize,
        current_checksum: [u8; 16],
    ) -> Result<ResultSet, crate::Error> {
        self.sync_statement_state(
            failed_statement_index,
            current_consumed_rows,
            current_checksum,
        );
        match self
            .retry_transaction_loop(Some(failed_statement_index), last_resume_token)
            .await?
        {
            Some(result_set) => Ok(result_set),
            None => Err(crate::Error::deser(
                "Retry failed to produce a new ResultSet",
            )),
        }
    }

    fn sync_statement_state(
        &self,
        statement_index: usize,
        consumed_rows: usize,
        checksum: [u8; 16],
    ) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(RetriableStatement::Query {
            consumed_rows: current_rows,
            expected_checksum: current_checksum,
            ..
        }) = guard.history.get_mut(statement_index)
        {
            *current_rows = consumed_rows;
            *current_checksum = checksum;
        }
    }
}

#[async_trait::async_trait]
impl ConnectionTransaction for ReadWriteTransactionUnit {
    async fn execute_query(
        &mut self,
        _staleness: Option<TimestampBound>,
        statement: Statement,
    ) -> Result<ExecutionResult, Error> {
        let statement_clone = statement.clone();
        let result = self
            .with_retry(move |transaction| {
                let stmt = statement_clone.clone();
                async move { transaction.execute_query(stmt).await }
            })
            .await;

        match result {
            Ok(mut result_set) => {
                let mut guard = self.inner.lock().unwrap();
                let statement_index = guard.history.len();
                guard.history.push(RetriableStatement::Query {
                    statement: statement.clone(),
                    consumed_rows: 0,
                    expected_checksum: [0; 16],
                });

                let self_shared = Arc::new(self.clone());
                result_set = result_set.with_connection_retry(self_shared, statement_index, true);

                Ok(ExecutionResult::QueryResult(Box::new(result_set)))
            }
            Err(error) => {
                let mut guard = self.inner.lock().unwrap();
                let error_code = get_error_code(&error);
                guard.history.push(RetriableStatement::Failed {
                    statement: statement.clone(),
                    expected_error_code: error_code,
                });
                Err(error)
            }
        }
    }

    async fn execute_update(&mut self, statement: Statement) -> Result<ExecutionResult, Error> {
        let statement_clone = statement.clone();
        let result = self
            .with_retry(move |transaction| {
                let stmt = statement_clone.clone();
                async move { transaction.execute_update(stmt).await }
            })
            .await;

        match result {
            Ok(count) => {
                let mut guard = self.inner.lock().unwrap();
                guard.history.push(RetriableStatement::Update {
                    statement: statement.clone(),
                    expected_update_count: count,
                });
                Ok(ExecutionResult::UpdateResult(count))
            }
            Err(error) => {
                let mut guard = self.inner.lock().unwrap();
                let error_code = get_error_code(&error);
                guard.history.push(RetriableStatement::Failed {
                    statement: statement.clone(),
                    expected_error_code: error_code,
                });
                Err(error)
            }
        }
    }

    async fn execute_batch_update(
        &mut self,
        batch: crate::batch::BatchDml,
    ) -> Result<ExecutionResult, Error> {
        let batch_clone = batch.clone();
        let result = self
            .with_retry(move |transaction| {
                let b = batch_clone.clone();
                async move { transaction.execute_batch_update(b).await }
            })
            .await;

        match result {
            Ok(counts) => {
                let mut guard = self.inner.lock().unwrap();
                guard.history.push(RetriableStatement::BatchUpdate {
                    batch: batch.clone(),
                    expected_update_counts: counts.clone(),
                });
                Ok(ExecutionResult::BatchUpdateResult(counts))
            }
            Err(error) => {
                let mut guard = self.inner.lock().unwrap();
                let error_code = get_error_code(&error);
                guard.history.push(RetriableStatement::BatchFailed {
                    batch: batch.clone(),
                    expected_error_code: error_code,
                });
                Err(error)
            }
        }
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        self.with_retry(|transaction| async move { transaction.commit().await.map(|_| ()) })
            .await
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        let transaction = {
            let guard = self.inner.lock().unwrap();
            guard.transaction.clone()
        };
        transaction.rollback().await
    }

    fn savepoint(&mut self, name: &str, dialect: Dialect) -> Result<(), Error> {
        if self.savepoint_support == SavepointSupport::Disabled {
            return Err(Error::deser(
                "Savepoint creation is not allowed when savepoint support is disabled",
            ));
        }

        if name.is_empty() {
            return Err(Error::deser("Savepoint name cannot be empty"));
        }
        if name.len() > 128 {
            return Err(Error::deser("Savepoint name cannot exceed 128 characters"));
        }
        let first_char = name.chars().next().unwrap();
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return Err(Error::deser(
                "Savepoint name must start with an alphabetic character or underscore",
            ));
        }

        let mut guard = self.inner.lock().unwrap();

        if dialect != Dialect::PostgreSql && guard.savepoints.iter().any(|s| s.name == name) {
            return Err(Error::deser(format!(
                "Savepoint with name {} already exists",
                name
            )));
        }

        let statement_position = guard.history.len();
        guard.savepoints.push(Savepoint {
            name: name.to_string(),
            statement_position,
            is_auto_savepoint: false,
        });

        Ok(())
    }

    fn release_savepoint(&mut self, name: &str) -> Result<(), Error> {
        let mut guard = self.inner.lock().unwrap();

        let index = guard
            .savepoints
            .iter()
            .rposition(|s| s.name == name)
            .ok_or_else(|| Error::deser(format!("Savepoint with name {} does not exist", name)))?;

        guard.savepoints.truncate(index);

        Ok(())
    }

    async fn rollback_to_savepoint(
        &mut self,
        name: &str,
        savepoint_support: SavepointSupport,
    ) -> Result<(), Error> {
        if savepoint_support == SavepointSupport::Disabled {
            return Err(Error::deser("Savepoints are disabled"));
        }

        let (index, savepoint) = {
            let guard = self.inner.lock().unwrap();
            let index = guard
                .savepoints
                .iter()
                .rposition(|s| s.name == name)
                .ok_or_else(|| {
                    Error::deser(format!("Savepoint with name {} does not exist", name))
                })?;
            (index, guard.savepoints[index].clone())
        };

        // Roll back the current physical transaction
        let transaction = {
            let guard = self.inner.lock().unwrap();
            guard.transaction.clone()
        };
        transaction.rollback().await?;

        let mut guard = self.inner.lock().unwrap();

        let status = Status::default()
            .set_code(Code::Aborted)
            .set_message(format!(
                "Transaction has been rolled back to savepoint {}",
                name
            ));
        guard.rolled_back_to_savepoint_error = Some(Error::service(status));

        guard.history.truncate(savepoint.statement_position);
        guard.savepoints.truncate(index + 1);

        Ok(())
    }
}

pub(crate) struct ReadOnlyTransactionUnit {
    transaction: MultiUseReadOnlyTransaction,
}

impl ReadOnlyTransactionUnit {
    pub(crate) fn new(transaction: MultiUseReadOnlyTransaction) -> Self {
        Self { transaction }
    }
}

#[async_trait::async_trait]
impl ConnectionTransaction for ReadOnlyTransactionUnit {
    async fn execute_query(
        &mut self,
        _staleness: Option<TimestampBound>,
        statement: Statement,
    ) -> Result<ExecutionResult, Error> {
        let result_set = self.transaction.execute_query(statement).await?;
        Ok(ExecutionResult::QueryResult(Box::new(result_set)))
    }

    async fn execute_update(&mut self, _statement: Statement) -> Result<ExecutionResult, Error> {
        Err(Error::deser(
            "Cannot execute DML update statement inside a read-only transaction",
        ))
    }

    async fn execute_batch_update(
        &mut self,
        _batch: crate::batch::BatchDml,
    ) -> Result<ExecutionResult, Error> {
        Err(Error::deser(
            "Cannot execute DML batch update inside a read-only transaction",
        ))
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }
}

pub(crate) struct AutocommitTransactionUnit {
    client: DatabaseClient,
    dialect: Dialect,
}

impl AutocommitTransactionUnit {
    pub(crate) fn new(client: DatabaseClient, dialect: Dialect) -> Self {
        Self { client, dialect }
    }
}

fn is_dml_query(sql: &str, dialect: Dialect) -> bool {
    let mut parser = crate::connection::parser::SimpleParser::new(sql, dialect);
    if let Some(keyword) = parser.read_keyword() {
        let keyword_uppercase = keyword.to_uppercase();
        matches!(keyword_uppercase.as_str(), "INSERT" | "UPDATE" | "DELETE")
    } else {
        false
    }
}

#[async_trait::async_trait]
impl ConnectionTransaction for AutocommitTransactionUnit {
    async fn execute_query(
        &mut self,
        staleness: Option<TimestampBound>,
        statement: Statement,
    ) -> Result<ExecutionResult, Error> {
        if is_dml_query(statement.sql(), self.dialect) {
            let runner = self.client.read_write_transaction().build().await?;
            let transaction_result = runner
                .run(async move |transaction| transaction.execute_query(statement.clone()).await)
                .await?;
            Ok(ExecutionResult::QueryResult(Box::new(
                transaction_result.result,
            )))
        } else {
            let mut builder = self.client.single_use();
            if let Some(ref bound) = staleness {
                builder = builder.set_timestamp_bound(bound.clone());
            }
            let transaction = builder.build();
            let result_set = transaction.execute_query(statement).await?;
            Ok(ExecutionResult::QueryResult(Box::new(result_set)))
        }
    }

    async fn execute_update(&mut self, statement: Statement) -> Result<ExecutionResult, Error> {
        let statement = statement.set_last_statement(true);
        let runner = self.client.read_write_transaction().build().await?;
        let result = runner
            .run(async move |transaction| transaction.execute_update(statement.clone()).await)
            .await?;

        Ok(ExecutionResult::UpdateResult(result.result))
    }

    async fn execute_batch_update(
        &mut self,
        mut batch: crate::batch::BatchDml,
    ) -> Result<ExecutionResult, Error> {
        batch.last_statements = true;
        let runner = self.client.read_write_transaction().build().await?;
        let counts = runner
            .run(async move |transaction| transaction.execute_batch_update(batch.clone()).await)
            .await?;

        Ok(ExecutionResult::BatchUpdateResult(counts.result))
    }

    async fn commit(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }

    fn is_autocommit(&self) -> bool {
        true
    }
}

struct RetryBackoff {
    delay: Duration,
}

impl RetryBackoff {
    fn new() -> Self {
        Self {
            delay: Duration::from_millis(50),
        }
    }

    fn next_delay(&mut self) -> Duration {
        let current = self.delay;
        self.delay = std::cmp::min(self.delay * 2, Duration::from_secs(5));
        current
    }
}

fn is_aborted_error(err: &Error) -> bool {
    if let Some(status) = err.status() {
        return matches!(status.code, Code::Aborted);
    }
    false
}

fn is_concurrent_modification_error(err: &Error) -> bool {
    if let Some(status) = err.status() {
        return matches!(status.code, Code::Aborted)
            && status
                .message
                .contains("Concurrent transaction modification detected");
    }
    false
}

fn get_error_code(error: &Error) -> Option<Code> {
    error.status().map(|s| s.code)
}

fn is_dml_statement(sql: &str) -> bool {
    let sql_uppercase = sql.trim().to_uppercase();
    sql_uppercase.starts_with("INSERT")
        || sql_uppercase.starts_with("UPDATE")
        || sql_uppercase.starts_with("DELETE")
}
