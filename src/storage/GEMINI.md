# Google Cloud Client Libraries for Rust - Storage

## Project Overview

This directory contains the `google-cloud-storage` Rust crate, which provides
idiomatic client libraries to interact with Google Cloud Storage. It is part of
the larger `google-cloud-rust` workspace.

The library exposes two primary clients:

- **`Storage`** (`src/storage/client.rs`): Handles data plane operations
  (reading, writing, and managing objects).
- **`StorageControl`** (`src/control/client.rs`): Handles control plane and
  administrative operations (managing buckets, folders, and managed folders).

Most of the underlying communication logic, gRPC/Protobuf representations, and
base clients are generated (located in `src/generated` and `src/google`), while
ergonomic wrappers, trait implementations, and custom logic (like
`read_resume_policy` and `streaming_source`) are manually maintained.

## Key Directories and Files

- **`src/`**: The core library code.
  - `src/lib.rs`: The root of the crate, exporting the main clients, builders,
    stubs, and types.
  - `src/storage/`: Implementations for the data plane client (bidi streaming,
    open object, read/write object).
  - `src/control/`: Implementations for the control plane client.
  - `src/generated/` & `src/google/`: Auto-generated protobuf code (do not edit
    directly).
  - `src/stub/`: Defines traits and structures for mocking client interactions
    during tests.
- **`tests/`**: Contains unit and integration tests (e.g., `mocking.rs`,
  `binding.rs`).
  - **`tests/scenarios/`**: A standalone binary package (`storage-scenarios`)
    used for stress testing bidirectional streaming reads against live GCP
    environments.
- **`examples/`**: Code examples showcasing how to authenticate and use the
  library.
- **`benchmarks/`**: Contains benchmarking tools to measure client performance.
- **`grpc-mock/`**: Provides a mock gRPC server for testing the client locally
  without requiring live GCP access.

## Building and Running

Since this crate is part of a larger Cargo workspace, standard Cargo commands
are used. Run these commands from within the `storage` directory or the
workspace root.

**Building:**

```bash
cargo build --package google-cloud-storage
```

**Testing:**

```bash
# Run tests for this crate
cargo test --package google-cloud-storage

# Run tests with the unstable-stream feature enabled
cargo test --package google-cloud-storage --features unstable-stream
```

**Running Scenarios (Stress Tests):** The stress testing utility is built as a
separate package:

```bash
cargo run --release --package storage-scenarios -- --bucket-name <BUCKET_NAME> [OPTIONS]
```

## Development Conventions

- **Asynchronous Execution:** The library is heavily reliant on `tokio` and
  `futures`. All RPCs are asynchronous.
- **Code Generation:** Core protobuf definitions are generated from Google API
  descriptors using the internal `sidekick` tool. Manual implementation work
  should occur in extension files like `model_ext.rs`, `builder_ext.rs`, or the
  high-level `client.rs` implementations rather than the generated modules.
- **Mocking Strategy:** The library provides robust mocking capabilities for
  developers using the crate. The `src/stub/` module defines traits that can be
  implemented or mocked using `mockall` to simulate GCP behavior in unit tests.
- **Networking & Crypto:** The `default-rustls-provider` feature is enabled by
  default, using `aws-lc-rs` for TLS.
- **Configuration Defaults:** The `Storage` client provides a `builder()`
  pattern (`ClientBuilder`) to configure options like `with_endpoint`,
  `with_credentials`, `with_retry_policy`, `with_grpc_subchannel_count`, and
  `with_tracing`.
- **Linting & Formatting:** Ensure code complies with workspace standards:
  ```bash
  cargo clippy --package google-cloud-storage
  cargo fmt --package google-cloud-storage
  ```
