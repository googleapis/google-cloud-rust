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

/// SQL dialects supported by Spanner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Dialect {
    /// Google Standard SQL dialect.
    GoogleSql,
    /// PostgreSQL-compatible dialect.
    PostgreSql,
}

/// Option value used for determining the behavior of savepoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SavepointSupport {
    /// Savepoints are enabled and can be used on the connection.
    /// Rolling back to a savepoint will trigger a retry of the transaction
    /// up to the point where the savepoint was set.
    #[default]
    Enabled,
    /// Savepoints are enabled and can be used on the connection.
    /// Rolling back to a savepoint will not trigger a retry. Further attempts
    /// to use a read/write transaction after a rollback to savepoint will fail.
    FailAfterRollback,
    /// Savepoints are disabled. Any attempt to create a savepoint will fail.
    Disabled,
}

crate::impl_connection_enum!(
    SavepointSupport,
    Enabled => "enabled",
    FailAfterRollback => "fail_after_rollback",
    Disabled => "disabled",
);

/// Errors specific to the stateful Connection API.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionError {
    /// SQL statement failed to parse.
    #[error("SQL syntax error: {0}")]
    SyntaxError(String),
    /// Invalid or unsupported option value.
    #[error("invalid option: {0}")]
    InvalidOption(String),
    /// Action is invalid in the current transaction state.
    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(String),
}
