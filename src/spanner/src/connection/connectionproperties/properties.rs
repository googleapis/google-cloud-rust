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

use crate::connection::connectionstate::{
    BooleanProperty, ConnectionProperty, EnumProperty, IntegerProperty, StartupStringProperty,
    StringProperty,
};
use crate::connection::{Dialect, SavepointSupport};
use std::collections::HashMap;
use std::sync::LazyLock;

use super::read_only_staleness::ReadOnlyStalenessProperty;

/// Connection property static instance for specifying read-only staleness.
pub static READ_ONLY_STALENESS: ReadOnlyStalenessProperty = ReadOnlyStalenessProperty;

/// Connection property static instance for controlling savepoint behavior.
pub static SAVEPOINT_SUPPORT: EnumProperty<SavepointSupport> = EnumProperty::new(
    "savepoint_support",
    "Determines the behavior of savepoints on this connection. \
     Supported values: 'enabled' (savepoints are fully supported and transactions are retried on rollback to savepoint), \
     'fail_after_rollback' (savepoints are supported but rollbacks to savepoint prevent subsequent statements from executing), \
     'disabled' (savepoint creation fails).",
    "enabled",
);

/// Connection property for controlling connection autocommit behavior.
pub static AUTOCOMMIT: BooleanProperty = BooleanProperty::new(
    "autocommit",
    "Determines whether the connection executes statements in auto-commit mode (true/false).",
    "true",
);

/// Connection property for controlling connection read-only mode.
pub static READONLY: BooleanProperty = BooleanProperty::new(
    "readonly",
    "Determines whether the connection is in read-only mode (true/false).",
    "false",
);

/// Connection property for controlling whether the connection retries aborted transactions.
pub static RETRY_ABORTS_INTERNALLY: BooleanProperty = BooleanProperty::new(
    "retry_aborts_internally",
    "Should the connection automatically retry Aborted errors (true/false).",
    "true",
);

/// Connection property for specifying a statement timeout duration.
pub static STATEMENT_TIMEOUT: IntegerProperty = IntegerProperty::new(
    "statement_timeout",
    "The timeout to apply to all statements on this connection. Setting the timeout to zero means no timeout.",
    "0",
);

/// Connection property for attaching a tag to Spanner transactions.
pub static TRANSACTION_TAG: StringProperty = StringProperty::new(
    "transaction_tag",
    "The transaction tag to add to the next transaction on this connection.",
    None,
);

/// Connection property for attaching a tag to Spanner requests.
pub static REQUEST_TAG: StringProperty = StringProperty::new(
    "request_tag",
    "The request tag to add to the next request on this connection.",
    None,
);

/// Connection property for specifying the default query optimizer version.
pub static OPTIMIZER_VERSION: StartupStringProperty = StartupStringProperty::new(
    "optimizer_version",
    "Sets the default query optimizer version to use for this connection.",
    Some("latest"),
);

/// Connection property for specifying the application name.
pub static APPLICATION_NAME: StringProperty = StringProperty::new_with_dialect(
    "application_name",
    "Sets the application name for the connection.",
    None,
    Dialect::PostgreSql,
);

fn all_properties() -> Vec<&'static dyn ConnectionProperty> {
    vec![
        &AUTOCOMMIT,
        &READONLY,
        &RETRY_ABORTS_INTERNALLY,
        &STATEMENT_TIMEOUT,
        &TRANSACTION_TAG,
        &REQUEST_TAG,
        &OPTIMIZER_VERSION,
        &READ_ONLY_STALENESS,
        &SAVEPOINT_SUPPORT,
        &APPLICATION_NAME,
    ]
}

/// Static registry of defined connection properties for GoogleSQL dialect.
pub static GOOGLE_SQL_REGISTRY: LazyLock<HashMap<String, &'static dyn ConnectionProperty>> =
    LazyLock::new(|| {
        all_properties()
            .into_iter()
            .filter(|p| p.is_supported(Dialect::GoogleSql))
            .map(|p| (p.name().to_string(), p))
            .collect()
    });

/// Static registry of defined connection properties for PostgreSQL dialect.
pub static POSTGRESQL_REGISTRY: LazyLock<HashMap<String, &'static dyn ConnectionProperty>> =
    LazyLock::new(|| {
        all_properties()
            .into_iter()
            .filter(|p| p.is_supported(Dialect::PostgreSql))
            .map(|p| (p.name().to_string(), p))
            .collect()
    });

/// Retrieve the static properties registry for a given database dialect.
pub fn get_registry(dialect: Dialect) -> &'static HashMap<String, &'static dyn ConnectionProperty> {
    match dialect {
        Dialect::GoogleSql => &GOOGLE_SQL_REGISTRY,
        Dialect::PostgreSql => &POSTGRESQL_REGISTRY,
    }
}
