# Implementation Plan: Stateful Spanner Connection API in Rust

This plan details the step-by-step execution to implement the stateful Spanner Connection API in the `google-cloud-spanner` crate. To ensure high code quality, idiomatic Rust, and robust test coverage, the implementation is broken down into 7 incremental phases.

---

## Phase 1: Workspace Setup & Skeleton
1.  **Cargo Feature Flag**:
    - Update `src/spanner/Cargo.toml` to define the `connection` feature (disabled by default):
      ```toml
      [features]
      default = ["default-rustls-provider"]
      connection = []
      ```
2.  **File Structure & Skeleton**:
    - Create a directory `src/spanner/src/connection/`.
    - Create `src/spanner/src/connection/mod.rs` to expose the module.
    - Create empty files `src/spanner/src/connection/parser.rs`, `src/spanner/src/connection/state.rs`, `src/spanner/src/connection/pool.rs`, and `src/spanner/src/connection/connection.rs`.
    - Conditionally export the module in `src/spanner/src/lib.rs`:
      ```rust
      #[cfg(feature = "connection")]
      pub mod connection;
      ```

---

## Phase 2: Standalone Token-Based Parser (`parser.rs`)
1.  **`SimpleParser` Character Tokenizer**:
    - Implement a simple struct `SimpleParser` that holds the input query byte slice and index.
    - Add utility methods: `skip_whitespace_and_comments`, `eat_keyword`, `eat_token`, `eat_identifier`, and string literal parsing.
    - Ensure comments (`--` and `/* ... */`) are correctly skipped without regex.
2.  **Statement Classifier**:
    - Implement an enum `StatementType` with variants: `Query` (DQL), `Update` (DML), `Ddl`, and `ClientSide(ClientSideCommand)`.
    - Classify standard commands by identifying first keywords (e.g. `SELECT`, `WITH`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`, `GRANT`, `REVOKE`, `RENAME`).
3.  **Client-Side Command Parser**:
    - Parse client-side control syntax:
      - `SET [SESSION | LOCAL] [prefix.]property = value`
      - `SET TRANSACTION ...`
      - `SHOW property`
      - `BEGIN [TRANSACTION]` / `START TRANSACTION`
      - `COMMIT [TRANSACTION]`
      - `ROLLBACK [TRANSACTION]`
      - `START BATCH DML` / `START BATCH DDL`
      - `RUN BATCH` / `ABORT BATCH`
4.  **Unit Tests**:
    - Add thorough tests in `parser.rs` for comment-stripping, keyword matching, SQL syntax classification, and edge-cases (e.g., lowercase vs uppercase, string parameters containing keywords).

---

## Phase 3: Property Registry & Hierarchical State (`state.rs`) [COMPLETED]
1.  **`ConnectionProperty` Traits & Structs**:
    - Define `Context` (Startup / User) and `Dialect` (GoogleSql / PostgreSql).
    - Define `ConnectionProperty` trait.
    - Implement `TypedProperty<T>` generic struct wrapping parsing closures/converters.
2.  **`PROPERTY_REGISTRY` Catalog**:
    - Instantiate a static/LazyLock registry mapping property names to `Box<dyn ConnectionProperty>`.
    - Register core properties (e.g., `autocommit`, `readonly`, `statement_timeout`, `transaction_tag`, `request_tag`, `read_only_staleness`).
3.  **`ConnectionState` & Scopes**:
    - Implement the prioritized search hierarchy of scopes (`Statement` > `Local` > `Transaction` > `Session`).
    - Handle `SET LOCAL` as a no-op when outside a transaction.
    - Dynamically allow any property containing a dot (`.`) as a raw string property (PG extensions).
4.  **State Transitions**:
    - Implement `begin()`, `commit()`, and `rollback()` state changes, ensuring `local_properties` and uncommitted `transaction_properties` are correctly applied or cleared.
5.  **Unit Tests**:
    - Test validation of property values, dynamic extensions, `SET LOCAL` no-op vs transactional behaviour, and rollback operations.

---

## Phase 4: Shared Client Pool (`pool.rs`) [COMPLETED]
1.  **Global Client Map**:
    - Implement a `ClientPool` mapping parsed connection DSNs to cached `Spanner` client instances.
    - Protect the global pool with a `LazyLock<Mutex<ClientPool>>`.
2.  **Client Reuse Logic**:
    - Implement `get_or_create` logic that returns a cloned `Spanner` instance, ensuring only one client is constructed for identical connection parameters.
3.  **DSN Query Parameter Application**:
    - Implement parsing of DSN parameters dynamically. Iterate over all query-string pairs and apply them to `ConnectionState` via `set(key, value)`.
4.  **Unit Tests**:
    - Verify client caching works, and test the dynamic mapping of custom/extension properties from a DSN query-string.

---

## Phase 5: Connection Orchestrator & Execution (`connection.rs`) [COMPLETED]
1.  **Stateful Connection Struct**:
    - Implement `Connection` wrapping `DatabaseClient`, `ConnectionState`, and `TransactionState`.
2.  **Auto-Dialect Detection**:
    - On connection initialization, perform a query or metadata lookup to identify if the database is GoogleSQL or PostgreSQL, and configure `Dialect` accordingly.
3.  **Autocommit Execution**:
    - Implement query/update execution in autocommit mode using single-use transaction builders.
4.  **Transactional Execution**:
    - Implement lazy transaction creation on the first DQL/DML command.
    - Map `COMMIT` and `ROLLBACK` commands to target Spanner transaction commits/rollbacks and transition connection states.
5.  **DML & DDL Batching**:
    - Buffer DML/DDL statements when in batch state, and execute them collectively upon receiving `RUN BATCH`. Ensure DDL executions block and wait for completion.

---

## Phase 6: Local Result Set Construction (`result_set.rs`)
1.  **`ResultSet::new_local` Constructor**:
    - Add a `pub(crate)` constructor `new_local` to `src/spanner/src/result_set.rs` under the `connection` feature flag.
    - Allow creation of static result sets (with `stream: None` and populated `ready_rows`) to return connection parameters for `SHOW` client-side queries.
2.  **Integration with Connection State**:
    - Map `SHOW property` client commands to return this client-side local `ResultSet`.
3.  **Unit Tests**:
    - Verify that local result sets return correct values and can be drained correctly by clients.

---

## Phase 7: Integration & Validation Testing
1.  **Workspace Mock Tests**:
    - Write mock tests in `src/spanner/tests/connection_test.rs` simulating database client interactions, lazy transactions, and dialect checks against `grpc-mock`.
2.  **Clippy & Formatting**:
    - Run `cargo fmt` and `cargo clippy-strict` checks to ensure zero clippy warnings.
