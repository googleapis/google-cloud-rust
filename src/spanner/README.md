# Google Cloud Client Libraries for Rust - Spanner

This crate implements the [Spanner] client library.

**WARNING:** this is a preview release of the crate. We believe the APIs to be
stable. We also are seeking feedback about the APIs and may need to make
breaking changes if we discover that some parts are hard to use.

We welcome feedback about the APIs, documentation, missing features, bugs, etc.

## About Spanner

[Spanner] is a fully managed, mission-critical, relational database service that
offers transactional consistency at global scale, schemas, SQL (ANSI 2011 with
extensions), and automatic, synchronous replication for high availability.

To read, write, and query data, use the `Spanner` client to connect to the
service and create a `DatabaseClient` for your database.

## Getting Started

### Installation

To add this crate to your project, use `cargo add`:

```bash
cargo add google-cloud-spanner
cargo add tokio --features full
```

To find the latest version and see how to add it manually to your `Cargo.toml`,
refer to the [crate's page on docs.rs](https://docs.rs/google-cloud-spanner).

### Authentication and Authorization

The Spanner client automatically uses Application Default Credentials (ADC). You
can also provide explicit credentials by calling
`.with_credentials(credentials)` on the client builder:

```rust
use google_cloud_spanner::client::Spanner;
// ...
# async fn sample(
#     credentials: google_cloud_auth::credentials::CredentialsFile,
# ) -> Result<(), google_cloud_spanner::Error> {
let spanner = Spanner::builder()
    .with_credentials(credentials)
    .build()
    .await?;
# Ok(())
# }
```

Ensure your authenticated principal has the required IAM roles to access Spanner
resources. See the
[Authentication](https://github.com/googleapis/google-cloud-rust#authentication)
section in the workspace root for setup details.

### Local Development with the Spanner Emulator

To develop locally against the
[Spanner Emulator](https://cloud.google.com/spanner/docs/emulator), set the
`SPANNER_EMULATOR_HOST` environment variable before creating the client:

```bash
export SPANNER_EMULATOR_HOST=localhost:9010
```

The client builder automatically detects this variable, connects to the emulator
endpoint, and configures anonymous credentials.

## Session Management and Client Lifecycle

The Spanner Rust client manages a long-lived multiplexed session under the hood.
Because session creation is expensive, `DatabaseClient` is designed to be a
long-lived object.

- Create a single `DatabaseClient` per database and reuse it across your
  application for all queries and transactions.
- `DatabaseClient` is thread-safe and cheap to clone, as it shares the
  underlying session pool and gRPC channels via reference counting (`Arc`).
- Avoid creating a new `DatabaseClient` for individual requests.

## Common Tasks

### 1. Creating a client

The following example shows how to initialize the `Spanner` client and build a
long-lived `DatabaseClient` for a specific database:

```rust
use google_cloud_spanner::Error;
use google_cloud_spanner::client::Spanner;

async fn create_client() -> Result<(), Error> {
    // Build the Spanner client.
    let spanner = Spanner::builder().build().await?;

    // Create a long-lived DatabaseClient for a specific database.
    // This client should be reused across your application.
    let _database_client = spanner
        .database_client(
            "projects/my-project/instances/my-instance/databases/my-database",
        )
        .build()
        .await?;

    Ok(())
}
```

### 2. Executing a query using a single-use read-only transaction

A single-use read-only transaction (`single_use()`) is optimized for executing a
single read or query. It reduces latency by avoiding the overhead of multi-use
transaction initialization.

```rust
use google_cloud_spanner::Error;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

async fn execute_single_use_query(
    database_client: &DatabaseClient,
) -> Result<(), Error> {
    // Create a single-use read-only transaction.
    let transaction = database_client.single_use().build();

    // Build a parameterized SQL query.
    let statement = Statement::builder(
        "SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId = @id",
    )
    .add_param("id", 42)
    .build();

    // Execute the query to receive a stream of rows.
    let mut result_set = transaction.execute_query(statement).await?;

    // Iterate through the rows. Using `.transpose()?` cleanly handles both
    // stream completion (Option) and potential row errors (Result).
    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id: i64 = row.get("SingerId");
        let first_name: String = row.get("FirstName");
        let last_name: String = row.get("LastName");
        println!("Singer {singer_id}: {first_name} {last_name}");
    }

    Ok(())
}
```

### 3. Executing a read/write transaction

Read/write transactions execute queries and mutations atomically. Spanner may
abort a transaction if contention occurs or for other transient reasons. Use
`TransactionRunner` to execute read/write operations; it automatically retries
the closure when aborted.

```rust
use google_cloud_spanner::Error;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

async fn execute_read_write(
    database_client: &DatabaseClient,
) -> Result<(), Error> {
    // Create a TransactionRunner for a read/write transaction.
    let runner = database_client.read_write_transaction().build().await?;

    // Execute the transaction. The async closure is automatically retried
    // if Spanner aborts the transaction.
    runner
        .run(async |transaction| {
            let statement = Statement::builder(
                "UPDATE Singers SET FirstName = 'John' WHERE SingerId = 1",
            )
            .build();
            transaction.execute_update(statement).await?;
            Ok(())
        })
        .await?;

    Ok(())
}
```

### 4. Executing a read-only transaction

A multi-use read-only transaction executes multiple reads or queries at a
consistent snapshot in time without taking locks or blocking write operations.

```rust
use google_cloud_spanner::Error;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;

async fn execute_multi_use_read_only(
    database_client: &DatabaseClient,
) -> Result<(), Error> {
    // Create a multi-use read-only transaction.
    let transaction = database_client.read_only_transaction().build().await?;

    // Execute the first query.
    let first_statement = Statement::builder(
        "SELECT SingerId, FirstName FROM Singers WHERE SingerId = 1",
    )
    .build();
    let mut first_result_set =
        transaction.execute_query(first_statement).await?;
    while let Some(row) = first_result_set.next().await.transpose()? {
        let singer_id: i64 = row.get("SingerId");
        let first_name: String = row.get("FirstName");
        println!("Singer {singer_id}: {first_name}");
    }

    // Execute the second query against the same consistent snapshot.
    let second_statement = Statement::builder(
        "SELECT AlbumId, AlbumTitle FROM Albums WHERE SingerId = 1",
    )
    .build();
    let mut second_result_set =
        transaction.execute_query(second_statement).await?;
    while let Some(row) = second_result_set.next().await.transpose()? {
        let album_id: i64 = row.get("AlbumId");
        let album_title: String = row.get("AlbumTitle");
        println!("Album {album_id}: {album_title}");
    }

    Ok(())
}
```

### 5. Executing a stale query

When reading slightly older data is acceptable, choosing a stale timestamp bound
can reduce read latency.

```rust
use std::time::Duration;
use google_cloud_spanner::Error;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::TimestampBound;

async fn execute_stale_query(
    database_client: &DatabaseClient,
) -> Result<(), Error> {
    // Read data as it was exactly 15 seconds ago.
    let timestamp_bound =
        TimestampBound::exact_staleness(Duration::from_secs(15));
    let transaction = database_client
        .single_use()
        .set_timestamp_bound(timestamp_bound)
        .build();

    let statement =
        Statement::builder("SELECT SingerId, FirstName, LastName FROM Singers")
            .build();
    let mut result_set = transaction.execute_query(statement).await?;

    while let Some(row) = result_set.next().await.transpose()? {
        let singer_id: i64 = row.get("SingerId");
        println!("Stale read SingerId: {singer_id}");
    }

    Ok(())
}
```

## Admin Operations

While `DatabaseClient` handles data-plane queries and transactions, you can
perform DDL and instance management operations using admin clients:

- **`DatabaseAdmin`**: Create and drop databases, execute DDL schema updates.
- **`InstanceAdmin`**: Manage Spanner instances and configurations.

Admin builders automatically inherit endpoints, credentials, and emulator
settings from the parent `Spanner` instance:

```rust
use google_cloud_spanner::Error;
use google_cloud_spanner::client::Spanner;

async fn create_admin_client(spanner: &Spanner) -> Result<(), Error> {
    let _database_admin = spanner.database_admin_builder().build().await?;
    let _instance_admin = spanner.instance_admin_builder().build().await?;
    Ok(())
}
```

### Executing Schema Updates

When creating tables or applying schema changes, always combine related DDL
statements into a single `update_database_ddl` request. Splitting schema changes
into multiple individual operations is inefficient on Spanner and causes each
change to run as a separate long-running operation.

```rust
use google_cloud_lro::Poller;
use google_cloud_spanner::Error;
use google_cloud_spanner::client::Spanner;

async fn create_tables_batch(spanner: &Spanner) -> Result<(), Error> {
    let database_admin = spanner.database_admin_builder().build().await?;

    let statements = vec![
        "CREATE TABLE Singers (
            SingerId INT64 NOT NULL,
            FirstName STRING(1024),
            LastName STRING(1024)
        ) PRIMARY KEY (SingerId)"
            .to_string(),
        "CREATE TABLE Albums (
            SingerId INT64 NOT NULL,
            AlbumId INT64 NOT NULL,
            AlbumTitle STRING(MAX)
        ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE"
            .to_string(),
    ];

    database_admin
        .update_database_ddl()
        .set_database(
            "projects/my-project/instances/my-instance/databases/my-database",
        )
        .set_statements(statements)
        .poller()
        .until_done()
        .await?;

    Ok(())
}
```

## Features

- `default-rustls-provider`: enabled by default. Uses `aws-lc-rs` for TLS and
  authentication. Applications with specific cryptographic requirements (such as
  exclusively using the `ring` crate) should disable this default and call
  `rustls::crypto::CryptoProvider::install_default()`.
- `unstable-stream`: enables the `.into_stream()` method on streaming types like
  `ResultSet` and `ExecuteStreamingSql`, allowing them to be consumed as a
  standard `futures::Stream`.

## More Information

- [Spanner Documentation](https://cloud.google.com/spanner/docs)
- [Crate Documentation on docs.rs](https://docs.rs/google-cloud-spanner)
- [Spanner Timestamp Bounds](https://cloud.google.com/spanner/docs/timestamp-bounds)

[spanner]: https://cloud.google.com/spanner
