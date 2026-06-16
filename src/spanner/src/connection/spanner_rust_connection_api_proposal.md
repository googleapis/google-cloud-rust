# Proposal: Stateful Connection API for Cloud Spanner Rust Client

## 1. Introduction & Motivation

The standard Cloud Spanner Rust client provides a high-performance, stateless API centered around `DatabaseClient` and transaction closures (e.g. `TransactionRunner`). This model is highly idiomatic for native Spanner applications.

However, certain use cases require a stateful, connection-oriented API model similar to traditional JDBC or PG wire-protocol drivers:
1. **PGAdapter in Rust**: Re-implementing the Java-based PGAdapter in Rust to improve speed and drastically reduce memory footprint.
2. **Direct PostgreSQL-like Driver**: Providing a driver for Spanner PostgreSQL-dialect databases that exposes the same connection/statement model as standard PostgreSQL drivers (like `tokio-postgres`), avoiding the need for PGAdapter as a separate proxy process.

This proposal outlines a design for a new, stateful `Connection` API module in the `google-cloud-spanner` crate.

### Core Design Goals
- **Separation & Optionality**: The Connection API should be separate from the core client API and hidden behind an optional Cargo feature flag (`connection`).
- **Stateful Connection Lifecycle**: A single `Connection` manages transaction state (autocommit, read-only, isolation level) and a transaction lifecycle.
- **Standalone Token-Based Parser**: A regex-free, token-based SQL parser that classifies statements (DQL, DML, DDL) and parses client-side controls (e.g., `SET`, `SHOW`, `BEGIN`, `COMMIT`).
- **Registry-Based Connection State**: Property definitions are registered in a standalone catalog of typed properties, making the state manager extremely easy to extend without any match blocks or hard-coded property dispatch logic.
- **Prioritized Hierarchical Scoping**: Supports four prioritized scopes (Statement > Local > Transaction > Session) and dynamic extension variables.
- **Dynamic DSN Property Mapping**: The connection string dynamically supports setting any connection property without code changes.

---

## 2. Shared Client Pooling & Dynamic DSN Configuration

### Dynamic DSN Configuration
A Connection DSN (or connection string) contains the target database URI and optional query parameters representing connection properties:
`projects/p/instances/i/databases/d?autocommit=false&readonly=true&my_ext.my_prop=my-val`

When a connection is initialized, we parse the DSN. Any query parameter is dynamically mapped to `ConnectionState::set`:
```rust
fn parse_and_apply_dsn_params(conn_str: &str, state: &mut ConnectionState) -> crate::Result<String> {
    let url = url::Url::parse(conn_str)
        .map_err(|e| crate::Error::Client(format!("Invalid connection string: {}", e)))?;
        
    let database_uri = url.path().trim_start_matches('/').to_string();
    
    for (key, val) in url.query_pairs() {
        state.set(&key, &val)?;
    }
    
    Ok(database_uri)
}
```

### Client Pooling
To share gRPC connection channels, thread pools, and background session maintainers, we define a global `ClientPool` protected by a `Mutex`:

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex, LazyLock};

use crate::client::{ClientConfig, Spanner};

static CLIENT_POOL: LazyLock<Mutex<ClientPool>> = LazyLock::new(|| {
    Mutex::new(ClientPool::new())
});

struct ClientPool {
    clients: HashMap<String, Spanner>,
}

impl ClientPool {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    async fn get_or_create(&mut self, conn_str: &str, config: ClientConfig) -> crate::Result<Spanner> {
        if let Some(client) = self.clients.get(conn_str) {
            return Ok(client.clone());
        }
        
        let client = Spanner::builder()
            .build()
            .await?;
            
        self.clients.insert(conn_str.to_string(), client.clone());
        Ok(client)
    }
}
```

---

## 3. Connection Property Registry & Extensible Connection State (`state.rs`)

### Connection Property Catalog
To allow clean scaling to dozens or hundreds of connection properties without complex match blocks, we define a generic registration catalog:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Context {
    Startup,
    User,
}

pub trait ConnectionProperty: std::fmt::Debug + Send + Sync {
    fn key(&self) -> &str;
    fn context(&self) -> Context;
    fn convert(&self, value: &str) -> crate::Result<String>;
    fn default_value(&self) -> String;
}

#[derive(Debug)]
pub struct TypedProperty<T> {
    pub key: &'static str,
    pub default_value: T,
    pub context: Context,
    pub converter: fn(&str) -> crate::Result<T>,
    pub formatter: fn(&T) -> String,
}

impl<T: std::fmt::Debug + Clone + 'static> ConnectionProperty for TypedProperty<T> {
    fn key(&self) -> &str {
        self.key
    }
    fn context(&self) -> Context {
        self.context
    }
    fn convert(&self, value: &str) -> crate::Result<String> {
        let val = (self.converter)(value)?;
        Ok((self.formatter)(&val))
    }
    fn default_value(&self) -> String {
        (self.formatter)(&self.default_value)
    }
}

pub static PROPERTY_REGISTRY: LazyLock<HashMap<String, Box<dyn ConnectionProperty>>> = LazyLock::new(|| {
    let mut registry: HashMap<String, Box<dyn ConnectionProperty>> = HashMap::new();
    
    // Register autocommit
    registry.insert("autocommit".to_string(), Box::new(TypedProperty {
        key: "autocommit",
        default_value: true,
        context: Context::User,
        converter: parse_bool,
        formatter: |v| v.to_string(),
    }));

    // Register readonly
    registry.insert("readonly".to_string(), Box::new(TypedProperty {
        key: "readonly",
        default_value: false,
        context: Context::User,
        converter: parse_bool,
        formatter: |v| v.to_string(),
    }));

    // ... Other properties can be added here without modifying connection state logic ...
    registry
});

fn parse_bool(value: &str) -> crate::Result<bool> {
    let val_lower = value.to_lowercase();
    if val_lower == "true" || val_lower == "on" || val_lower == "1" {
        Ok(true)
    } else if val_lower == "false" || val_lower == "off" || val_lower == "0" {
        Ok(false)
    } else {
        Err(crate::Error::Client(format!("Invalid boolean: {}", value)))
    }
}
```

---

### Extensible Connection State Scopes

The connection state (`state.rs`) tracks and resolves properties through four prioritized scopes:
1. **Statement Scope**: Valid only for the execution of a single statement. Cleared immediately after execution.
2. **Local Scope**: Active during the current transaction (set via `SET LOCAL`). Always discarded when the transaction finishes (on both `COMMIT` and `ROLLBACK`). If executed outside a transaction, it is a no-op.
3. **Transaction Scope**: Active during the current transaction (set via `SET`). Committed to Session Scope on `COMMIT`, discarded on `ROLLBACK`.
4. **Session Scope**: Persistent default properties.

#### Priority Resolution Order
`Statement Scope` $\rightarrow$ `Local Scope` $\rightarrow$ `Transaction Scope` $\rightarrow$ `Session Scope`

```rust
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropertyValue {
    pub current: String,
    pub original: String,
}

pub struct ConnectionState {
    dialect: Dialect,
    is_transactional: bool, // True for PostgreSQL, False for GoogleSQL
    in_transaction: bool,
    
    properties: HashMap<String, PropertyValue>,
    transaction_properties: Option<HashMap<String, PropertyValue>>,
    local_properties: Option<HashMap<String, PropertyValue>>,
    statement_properties: Option<HashMap<String, PropertyValue>>,
}

impl ConnectionState {
    pub fn new(dialect: Dialect) -> Self {
        let is_transactional = dialect == Dialect::PostgreSql;
        let mut properties = HashMap::new();
        
        // Populate defaults from the PROPERTY_REGISTRY
        for (key, prop) in PROPERTY_REGISTRY.iter() {
            properties.insert(key.clone(), PropertyValue {
                current: prop.default_value(),
                original: prop.default_value(),
            });
        }
        
        Self {
            dialect,
            is_transactional,
            in_transaction: false,
            properties,
            transaction_properties: None,
            local_properties: None,
            statement_properties: None,
        }
    }

    /// Set a standard session property (or transaction-buffered property).
    pub fn set(&mut self, key: &str, value: &str) -> crate::Result<()> {
        let key_lower = key.to_lowercase();
        let converted_value = self.validate_and_convert(&key_lower, value)?;

        if self.in_transaction && self.is_transactional {
            if self.transaction_properties.is_none() {
                self.transaction_properties = Some(self.properties.clone());
            }
            if let Some(ref mut tx_props) = self.transaction_properties {
                tx_props.insert(key_lower.clone(), PropertyValue {
                    current: converted_value.clone(),
                    original: self.properties.get(&key_lower).map(|v| v.current.clone()).unwrap_or(converted_value),
                });
            }
        } else {
            self.properties.insert(key_lower.clone(), PropertyValue {
                current: converted_value.clone(),
                original: self.properties.get(&key_lower).map(|v| v.original.clone()).unwrap_or(converted_value),
            });
        }
        Ok(())
    }

    /// Set a transaction-local property (SET LOCAL). Only valid in a transaction; no-op outside.
    pub fn set_local(&mut self, key: &str, value: &str) -> crate::Result<()> {
        if !self.in_transaction {
            // No-op outside transaction
            return Ok(());
        }
        let key_lower = key.to_lowercase();
        let converted_value = self.validate_and_convert(&key_lower, value)?;

        if self.local_properties.is_none() {
            self.local_properties = Some(HashMap::new());
        }
        if let Some(ref mut local_props) = self.local_properties {
            local_props.insert(key_lower, PropertyValue {
                current: converted_value,
                original: self.get(key).map(|v| v.to_string()).unwrap_or_else(|| value.to_string()),
            });
        }
        Ok(())
    }

    /// Set a statement-scoped property (hint for a single statement execution).
    pub fn set_statement_scoped(&mut self, key: &str, value: &str) -> crate::Result<()> {
        let key_lower = key.to_lowercase();
        let converted_value = self.validate_and_convert(&key_lower, value)?;

        if self.statement_properties.is_none() {
            self.statement_properties = Some(HashMap::new());
        }
        if let Some(ref mut stmt_props) = self.statement_properties {
            stmt_props.insert(key_lower, PropertyValue {
                current: converted_value,
                original: self.get(key).map(|v| v.to_string()).unwrap_or_else(|| value.to_string()),
            });
        }
        Ok(())
    }

    /// Get active property value based on prioritized scoping search.
    pub fn get(&self, key: &str) -> Option<&str> {
        let key_lower = key.to_lowercase();
        
        // 1. Check statement-scoped overrides
        if let Some(ref stmt_props) = self.statement_properties {
            if let Some(val) = stmt_props.get(&key_lower) {
                return Some(&val.current);
            }
        }
        
        // 2. Check transaction-local values
        if let Some(ref local_props) = self.local_properties {
            if let Some(val) = local_props.get(&key_lower) {
                return Some(&val.current);
            }
        }
        
        // 3. Check transaction-buffered values
        if self.in_transaction && self.is_transactional {
            if let Some(ref tx_props) = self.transaction_properties {
                if let Some(val) = tx_props.get(&key_lower) {
                    return Some(&val.current);
                }
            }
        }
        
        // 4. Fallback to default session values
        self.properties.get(&key_lower).map(|v| v.current.as_str())
    }

    pub fn begin(&mut self) {
        self.in_transaction = true;
        if self.is_transactional {
            self.transaction_properties = Some(self.properties.clone());
        }
        self.local_properties = Some(HashMap::new());
    }

    pub fn commit(&mut self) {
        self.in_transaction = false;
        if self.is_transactional && self.transaction_properties.is_some() {
            if let Some(tx_props) = self.transaction_properties.take() {
                self.properties = tx_props;
            }
        }
        self.local_properties = None;
        self.statement_properties = None;
    }

    pub fn rollback(&mut self) {
        self.in_transaction = false;
        self.transaction_properties = None;
        self.local_properties = None;
        self.statement_properties = None;
    }

    pub fn clear_statement_scoped(&mut self) {
        self.statement_properties = None;
    }

    fn validate_and_convert(&self, key: &str, value: &str) -> crate::Result<String> {
        if key.contains('.') {
            // Extension property: dynamically allowed, value stored as raw string
            return Ok(value.to_string());
        }
        
        if let Some(prop) = PROPERTY_REGISTRY.get(key) {
            prop.convert(value)
        } else {
            Err(crate::Error::Client(format!("Unknown configuration property: {}", key)))
        }
    }
}
```

---

## 4. Standalone Token-Based Parser (`parser.rs`)

We implement a simple, token-based, regex-free SQL parser (`parser.rs`) that operates as a standalone module. It classifies statements and extracts key tokens from client-side control commands.

```rust
#[derive(Debug, PartialEq, Eq)]
pub enum StatementType {
    Query,      // DQL (SELECT / WITH)
    Update,     // DML (INSERT / UPDATE / DELETE)
    Ddl,        // DDL (CREATE / DROP / ALTER)
    ClientSide(ClientSideCommand),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientSideCommand {
    Set { key: String, value: String, is_local: bool },
    Show { key: String },
    Begin,
    Commit,
    Rollback,
    StartBatchDml,
    StartBatchDdl,
    RunBatch,
    AbortBatch,
}

pub struct SimpleParser<'a> {
    sql: &'a [u8],
    pos: usize,
}

impl<'a> SimpleParser<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql: sql.as_bytes(),
            pos: 0,
        }
    }

    pub fn eat_identifier(&mut self) -> Option<String> {
        self.skip_whitespace_and_comments();
        if self.pos >= self.sql.len() {
            return None;
        }
        
        let start = self.pos;
        while self.pos < self.sql.len() && is_identifier_char(self.sql[self.pos]) {
            self.pos += 1;
        }
        
        if self.pos > start {
            Some(String::from_utf8_lossy(&self.sql[start..self.pos]).to_string())
        } else {
            None
        }
    }

    pub fn eat_keyword(&mut self, keyword: &str) -> bool {
        self.skip_whitespace_and_comments();
        let kw_len = keyword.len();
        if self.pos + kw_len > self.sql.len() {
            return false;
        }
        
        let next_slice = &self.sql[self.pos..self.pos + kw_len];
        if next_slice.eq_ignore_ascii_case(keyword.as_bytes()) {
            if self.pos + kw_len < self.sql.len() && is_identifier_char(self.sql[self.pos + kw_len]) {
                return false;
            }
            self.pos += kw_len;
            return true;
        }
        false
    }

    pub fn eat_token(&mut self, token: u8) -> bool {
        self.skip_whitespace_and_comments();
        if self.pos < self.sql.len() && self.sql[self.pos] == token {
            self.pos += 1;
            return true;
        }
        false
    }

    fn skip_whitespace_and_comments(&mut self) {
        while self.pos < self.sql.len() {
            if self.sql[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            } else if self.pos + 1 < self.sql.len() && self.sql[self.pos] == b'-' && self.sql[self.pos + 1] == b'-' {
                self.pos += 2;
                while self.pos < self.sql.len() && self.sql[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else if self.pos + 1 < self.sql.len() && self.sql[self.pos] == b'/' && self.sql[self.pos + 1] == b'*' {
                self.pos += 2;
                while self.pos + 1 < self.sql.len() && !(self.sql[self.pos] == b'*' && self.sql[self.pos + 1] == b'/') {
                    self.pos += 1;
                }
                self.pos += 2;
            } else {
                break;
            }
        }
    }
}

fn is_identifier_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'.'
}
```
