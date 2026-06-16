# Spanner Connection API

The `connection` module provides a stateful, connection-oriented API for interacting with Google Cloud Spanner. It is designed for applications or framework integrations (such as ORMs or database proxies like PGAdapter) that require a traditional, stateful database connection model rather than a stateless client-builder approach.

## Key Features

### 1. Stateful Connection-Oriented API
*   **In-Memory State Management:** All connection-specific state (such as transaction mode, read-only staleness, transaction tags, and active transaction references) is kept in-memory.
*   **Lightweight Connections:** A `Connection` instance does not maintain an exclusive physical network connection to the Spanner server. Instead, it leverages the shared Spanner client pool, making instances highly lightweight and efficient to create, pool, or destroy.

### 2. Traditional Transaction Control Flow
*   **Explicit Begin & Commit:** Unlike the standard Spanner client library which executes transactions via closures, the Connection API supports the traditional `BEGIN`, `COMMIT`, and `ROLLBACK` control flow.
*   **Checksum-Based Retries:** Because Spanner can abort read-write transactions at any point (due to locking conflicts or network interruptions), the Connection API implements a checksum-based replay mechanism. When a transaction is aborted, the API attempts to replay the executed statements to reconstruct the transaction.
*   **Safety Fallbacks:** If the replay mechanism detects that the underlying data has changed since the initial execution, the transaction is not retried, and the `Aborted` error is propagated to the application.

### 3. Client-Side SQL Command Support
The Connection API parses and executes various client-side commands directly in memory. These allow configuring the connection behavior dynamically using standard SQL queries, including:
*   **Transaction Controls:** `BEGIN [TRANSACTION]`, `COMMIT`, `ROLLBACK`, `START BATCH DML/DDL`, and `RUN/ABORT BATCH`.
*   **Savepoint Operations (PostgreSQL Dialect):** `SAVEPOINT <name>`, `RELEASE SAVEPOINT <name>`, and `ROLLBACK TO SAVEPOINT <name>`.
*   **Prepared Statements (PostgreSQL Dialect):** `PREPARE <name> AS <sql>` and `EXECUTE <name>(<params>)` with inline parameter evaluation.
*   **Connection Setting Modifiers:** `SET <property> = <value>` and `SHOW <property>` to manage attributes like read-only staleness, autocommit mode, and transaction tags.

## Usage Example

```rust
use google_cloud_spanner::connection::Connection;

#[tokio::main]
async fn main() -> Result<(), google_cloud_spanner::Error> {
    let dsn = "projects/my-project/instances/my-instance/databases/my-db";
    let mut conn = Connection::connect(dsn).await?;

    // Start a transaction using SQL
    conn.execute("BEGIN TRANSACTION").await?;

    // Execute query or update
    let result = conn.execute("INSERT INTO Users (Id, Name) VALUES (1, 'Alice')").await?;

    // Commit transaction using SQL
    conn.execute("COMMIT").await?;

    Ok(())
}
```
