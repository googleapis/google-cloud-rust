# Feature Gap Analysis: Cloud Spanner Connection API in Rust

This document analyzes the gaps between the new Rust Connection API and the mature implementations in the Java Connection API (`com.google.cloud.spanner.connection`) and the Golang `database/sql` driver (`go-sql-spanner`).

---

## 1. Feature Comparison Matrix

| Feature | Java Connection API | Go database/sql Driver | Rust Connection API |
| :--- | :---: | :---: | :---: |
| **Connection States & Dialects** | Yes (GoogleSQL, PG) | Yes (GoogleSQL, PG) | Yes (GoogleSQL, PG) |
| **Transaction Modes (Auto-commit/Manual)** | Yes | Yes | Yes |
| **Explicit Commit/Rollback** | Yes | Yes | Yes |
| **Dml/Ddl Batches (`START BATCH`)** | Yes | Yes | Yes |
| **Internal Retry of Aborted Transactions** | Yes (With Checksums) | Yes (With Checksums) | Yes (With Checksums) |
| **Savepoints (`SAVEPOINT`, `RELEASE`)** | Yes | No | Yes |
| **Partitioned Queries & Data Boost** | Yes (`RUN PARTITION`) | Yes | No |
| **Statement-Level Timeouts** | Yes (`STATEMENT_TIMEOUT`) | Yes | No |
| **Transaction-Level Timeouts** | Yes (`TRANSACTION_TIMEOUT`) | Yes | No |
| **Autocommit DML Execution Modes** | Yes (`TRANSACTIONAL` / `PARTITIONED_NON_TRANSACTIONAL`) | Yes | No |
| **DDL Execution Modes** | Yes | Yes (`SYNC` / `ASYNC` / `ASYNC_WAIT`) | No (SYNC only) |
| **Database Autocreation on Emulator** | Yes (`autoConfigEmulator`) | Yes (`auto_config_emulator`) | No |
| **Connection/Session Leak Tracking** | Yes | Yes | No |
| **Transaction Heartbeats** | Yes (`keep_transaction_alive`) | No | No |
| **Mutation Support** | Yes | No | No |
| **Postgres Savepoints / Transaction Modes**| Yes | No | Yes |

---

## 2. Detailed Breakdown of Key Gaps

### A. Savepoint Support (`SAVEPOINT`, `RELEASE`, `ROLLBACK TO SAVEPOINT`)
*   **Java implementation**: Implemented via `SavepointSupport.java`. Allows setting nested savepoints in read-write transactions. On rollback to a savepoint, only the operations executed after the savepoint are undone by reverting buffered state or dropping the sub-transaction.
*   **Go implementation**: Not supported.
*   **Rust implementation**: Fully supported. Implemented by truncating statement history on rollback, initiating a physical rollback on the active transaction, and raising a simulated aborted error to restart and replay transaction history up to the savepoint.

### B. Partitioned Queries & Data Boost
Allows distributing heavy query execution across multiple partitions and utilizing independent compute resources (Data Boost).
*   **Java implementation**: Adds client-side commands like:
    - `SET AUTO_PARTITION_MODE = true` (enforces executing all queries via partition tokens).
    - `SET DATA_BOOST_ENABLED = true`.
    - `PARTITION <query>`: returns partition tokens.
    - `RUN PARTITION <token>`: runs a specific partition.
*   **Go implementation**: Supported via `partitioned_query.go` and `merged_row_iterator.go` which automatically partitions and streams rows concurrently.
*   **Rust implementation**: Not supported in the Connection API.

### C. Checksum Validation on Retries (`ChecksumResultSet`)
To safely retry aborted read-write transactions internally, the client must verify that any queries executed prior to the abort return the exact same data during retry.
*   **Java/Go/Rust implementations**:
    - Generates a running checksum of all rows returned by queries (`ChecksumResultSet.java`/`checksum_row_iterator.go` and `checksum.rs` with `ResultSet::with_connection_retry`).
    - During internal retry, it re-runs those queries and asserts the checksum and row count match. If it mismatches, it raises an `Aborted` error with concurrent modification message, forcing the user to handle the retry manually.

### D. Client-Side Timeouts & Delay Modes
*   **Statement Timeout**: `SET STATEMENT_TIMEOUT = '<duration>'` aborts statements exceeding the timeout.
*   **Transaction Timeout**: `SET TRANSACTION_TIMEOUT = '<duration>'` bounds the total active transaction duration.
*   **Delay Transaction Start**: `SET DELAY_TRANSACTION_START_UNTIL_FIRST_WRITE = true` delays starting the Spanner read-write transaction (running reads in auto-commit/read-only mode first) to optimize locks.
*   **Transaction Heartbeats**: `SET KEEP_TRANSACTION_ALIVE = true` automatically runs `SELECT 1` queries every 10 seconds to keep locks and sessions active.

### E. DDL Execution Control
*   **Java/Go implementation**: Enables setting DDL modes:
    - `SYNC` (default): wait until the Long Running Operation (LRO) is fully completed.
    - `ASYNC`: request DDL changes and return immediately.
    - `ASYNC_WAIT`: wait for a specific timeout duration (e.g. 10s). If it doesn't finish, return anyway and let the LRO continue in the background.

### F. Mutation Support
Cloud Spanner supports writing data using either DML statements or Mutation objects. Writing via Mutations can be faster for batch operations.
*   **Java implementation**: Supported in the Connection API. Allows buffering Mutation objects in read-write transactions, which are written to Spanner during commit.
*   **Go/Rust Connection implementations**: Not supported. All modifications must use DML statements.

---

## 3. Recommended Implementation Priorities

We recommend prioritizing these gaps in the following phases:

1.  **Phase 1: Query & Transaction Timeouts**
    - Implement `STATEMENT_TIMEOUT` and `TRANSACTION_TIMEOUT` variables.
    - Wire timeouts to tokio futures and gRPC context deadlines.

2.  **Phase 2: DDL Async Execution Control**
    - Support `ddl_execution_mode` (`SYNC`, `ASYNC`, `ASYNC_WAIT`) and `ddl_async_wait_timeout`.

3.  **Phase 3: Partitioned Queries & Data Boost**
    - Implement properties `auto_partition_mode`, `data_boost_enabled`, `max_partitions`, and `max_partitioned_parallelism`.
    - Support executing partitioned queries concurrently in the background.

4.  **Phase 4: Checksum Validation for Aborted Transaction Retries**
    - Implement query hashing/checksum tracking to make internal aborted transaction retries robust and safe.
