# Agent Instructions

## Project Overview

This project provides idiomatic Rust client libraries for Google Cloud Platform
(GCP) services. The goal is to offer a natural and familiar API for Rust
developers to interact with GCP. The libraries are under active development, and
the APIs are subject to change.

The project is structured as a Rust workspace, containing numerous crates. Most
of the client libraries are auto-generated from Protobuf service specifications.
The core components include:

- **Generated Libraries:** Located in `src/generated`, these are the
  auto-generated client libraries for various GCP services.
- **Authentication:** The `src/auth` crate handles authentication with Google
  Cloud.
- **GAX (Google API Extensions):** The `src/gax` crate provides common
  components for all clients, including error handling, retry policies, and
  backoff strategies. `src/gax-internal` contains implementation details shared
  across clients.
- **Long-Running Operations (LRO):** The `src/lro` crate provides support for
  handling long-running operations.
- **Well-Known Types (WKT):** The `src/wkt` crate contains shared types used
  across the libraries.
- **Cloud Storage:** The `src/storage` crate contains a client library for
  Google Cloud Storage.
- **Pub/Sub:** The `src/pubsub` crate contains a client library for Google Cloud
  Pub/Sub.
- **Spanner:** The `src/spanner` crate contains a client library for Spanner.
  See [`src/spanner/llms.txt`](src/spanner/llms.txt) for guidance, best
  practices, and code samples for coding agents.

## Building and Running

The project uses Cargo, the Rust build tool and package manager.

### Building the Project

To build the entire workspace, run the following command from the root
directory:

```bash
cargo build
```

Workspace dependencies in the root `Cargo.toml` set `default-features = false`.
When adding a new binary, sample, integration-test, or conformance crate that
depends on a client crate via `.workspace = true`, explicitly set
`default-features = true` on that client dependency (for example,
`google-cloud-storage = { workspace = true, default-features = true }`).
Otherwise `default-rustls-provider` is disabled and `reqwest` panics at runtime
with `No rustls crypto provider is configured`.

### Running Tests

To run the tests for the entire workspace, use the following command:

```bash
cargo test
```

To run the tests for a particular crate, use the following command:

```bash
cargo test -p ${crate_name}
```

For example:

```bash
cargo test -p google-cloud-storage
```

### Running Integration Tests

Integration tests are located in the `tests` directory (e.g.,
`tests/integration`, `tests/integration-auth`). They are often disabled by
default because they run against live GCP services.

To run integration tests, you typically need to:

1. **Authenticate:** Run `gcloud auth application-default login` to set up
   credentials.
1. **Set Environment Variables:** Many tests require specific environment
   variables to be set (e.g., `GOOGLE_CLOUD_PROJECT`).

To run integration tests for a specific suite, you need to enable the
corresponding feature.

For example, to run the general integration tests with required environment
variables:

```bash
GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
env \
    GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT} \
    GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT=rust-sdk-test@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com \
    GOOGLE_CLOUD_RUST_TEST_STORAGE_KMS_RING=us-central1 \
  cargo test -p integration-tests --features run-integration-tests
```

See `doc/contributor/howto-guide-set-up-development-environment.md` for more
details on setting up resources for integration tests.

### Linter

Our CI runs clippy with `--deny warnings` in different configurations to ensure
full coverage. It is recommended to run both of the following commands locally:

To check the default build (which catches issues in `#[cfg(not(test))]` code
paths):

```bash
cargo clippy --all-targets -- -D warnings
```

To check the build with tests enabled (includes `#[cfg(test)]` and test files):

```bash
cargo clippy --profile test -- -D warnings
```

To run the linter for a particular crate, use the following command:

```bash
cargo clippy -p ${crate_name} --all-targets -- -D warnings
cargo clippy -p ${crate_name} --profile test -- -D warnings
```

For example:

```bash
cargo clippy -p google-cloud-storage --profile test -- -D warnings
```

We run a comprehensive strict check as part of our CI that enforces stricter
lints on all handwritten crates. This check is defined as a Cargo alias called
`clippy-strict` in [.cargo/config.toml](.cargo/config.toml).

To run these checks locally on all targeted handwritten crates, use the cargo
alias `clippy-strict`:

```bash
cargo clippy-strict
```

To run this strict check for a specific crate, run standard `cargo clippy` with
the same flags from the alias:

```bash
cargo clippy --all-features --no-deps -p google-cloud-storage -- -D missing_docs -D clippy::exhaustive_enums
```

Please refer to [.cargo/config.toml](.cargo/config.toml) for the authoritative
list of crates and exact set of lint flags currently subject to these checks.

Do not pass `-D missing_docs` via `RUSTFLAGS` (for example,
`RUSTFLAGS="-D missing_docs" cargo check`). `RUSTFLAGS` applies to workspace
`build.rs` scripts (such as `src/gax-internal/build.rs`) and breaks the build;
pass `-D missing_docs -D clippy::exhaustive_enums` after
`cargo clippy --all-features --no-deps -p ${crate_name} --`.

> [!IMPORTANT]
> When editing or adding code within any of the handwritten crates, you MUST run
> the strict clippy check (`cargo clippy-strict` for all crates or the specific
> command above for the modified crate) to ensure no new warnings are
> introduced.

### Code Formatter

To format the code for the entire workspace, use the following command:

```bash
cargo fmt
```

To format a crate use:

```bash
cargo fmt -p ${crate_name}
```

For example:

```bash
cargo fmt -p google-cloud-storage
```

## Development Conventions

### Setting up the Development Environment

- Install `rustc`, `rustfmt`, `clippy`, and `cargo` using `rustup`.
- Run `cargo build` and `cargo test` to verify the setup.

### Forks and Pull Requests

- Create a fork of the `google-cloud-rust` repository.
- Create a branch for your changes.
- Make your changes and commit them with a descriptive message.
- The commit message should follow the "Conventional Commits" specification.
- Push your changes to your fork.
- Create a pull request from your fork to the `main` branch of the
  `google-cloud-rust` repository.
- The PR body should include a detailed description of the changes.
- The PR will be reviewed by the maintainers.

### Generated Code Maintenance

- Most of the code is generated by the `librarian` tool.
- Do not edit the generated code directly.
- To update the generated code, you need to modify the generator templates or
  the service definitions. See
  `doc/contributor/howto-guide-generated-code-maintenance.md` for details.

### Proc-macro Crates

`cargo-semver-checks` cannot verify proc-macro crates such as
`src/bigquery-derive`. See the "Compatibility" section in
`src/bigquery-derive/src/lib.rs` for the full policy.

- Published proc-macro crates have compile-only contract tests in
  `src/*/tests/<name>_contract/`, where `<name>` is the package name without
  `google-cloud-` and with `-` replaced by `_` (e.g.
  `src/bigquery/tests/bigquery_derive_contract/`). For new accepted input, add a
  new case file and a `mod` line in its `main.rs`.
- Never modify, rename, or delete an existing case file. CI fails unless the
  pull request has the `rust: proc-macro-contract-change` label.
- If you need that label because accepted input changed, it is very likely a
  breaking change: take a breaking version bump of both the proc-macro crate and
  `google-cloud-bigquery`, by hand (librarian defaults to non-breaking bumps).
  Comment-only or formatting edits need the label but no bump.
- In new proc-macro crates, **every** path emitted by generated code—including
  trait paths in `impl` headers, return/error types, and constructors—must
  resolve through `#[doc(hidden)] pub mod __private` in the runtime crate, never
  its user-facing modules. Include a contract test in a module with no
  user-facing imports in scope to verify hygiene.
- Reject ambiguous input as an error instead of resolving it silently: check
  direct field collisions, direct-vs-`flatten` prefix collisions, and pairwise
  `flatten`-vs-`flatten` prefix overlaps (`p1 == p2` or
  `p1.starts_with("{p2}.")`) at compile time, and reject duplicate property keys
  across nested `flatten` structs at runtime.

### Hand-written Client Libraries

These rules apply to **new public APIs** in hand-written crates (e.g.
`src/storage`, `src/pubsub`, `src/bigquery`, `src/datastore`). Some existing
APIs predate them; do not copy those exceptions, and do not change existing
public APIs only to conform. [ARCHITECTURE.md](ARCHITECTURE.md) is binding, in
particular "No client-side validation" and "Separate namespace for fields vs.
options". The full checklist, with rationale and examples, is in
[howto-guide-handwritten-client-api.md](doc/contributor/howto-guide-handwritten-client-api.md).
Read it before designing or reviewing a public API.

Rules that are hard to fix after a release:

- Stub trait methods and RPC wrappers return a named `#[non_exhaustive]`
  response struct, never `()`, `Vec<T>`, or a primitive. Do not discard server
  responses. Audit every field of the underlying proto response (such as
  continuation queries or batch status flags) so server metadata is never
  dropped. Every stub method has a default implementation.
- Traits that applications implement (policies, stubs) receive a
  `#[non_exhaustive]` state struct, not positional arguments.
- Public enum struct variants are `#[non_exhaustive]`. Error enums use
  `#[non_exhaustive]` struct variants, not tuple variants with a `String`.
- Functions that buffer work, validate input, or may fail in the future return
  `Result` from the start.
- Setters are infallible; validate in `send()`. Do not enforce service limits on
  the client, and never validate data decoded from the service.
- Open protocol enums derive `Clone`, `Debug`, and `PartialEq` only (never `Eq`
  or `Hash`) and wrap `wkt::internal::UnknownEnumValue` so both integer and
  REST/ProtoJSON string unknowns round-trip without coercing strings to `0`.
  Protocol `oneof` message unions (such as values, filters, aggregations, or
  transforms) likewise include an opaque `Unknown` variant so unrecognized
  server extensions survive read-modify-write and continuation requests.
- Model mutually exclusive wire `oneof` branches as an `enum` or clear sibling
  fields in every `set_*()` method (last-setter-wins); never leave multiple
  branches `Some` in memory while dropping one during wire encoding.
- Every field participating in `PartialEq` on a constructible stub request,
  response, or model type has a public setter and getter; never include hidden
  `pub(crate)` plumbing fields in derived `PartialEq`.
- Use `set_<field>()` for request payload fields and `with_<option>()` for
  options that change the client library behavior.
- Environment variables that configure client behavior, endpoints, or
  credentials (such as `GOOGLE_CLOUD_UNIVERSE_DOMAIN`,
  `GOOGLE_CLOUD_RUST_LOGGING`, and `*_EMULATOR_HOST`) take higher precedence
  than `ClientBuilder` options (`with_endpoint()`, `with_universe_domain()`,
  `with_credentials()`, `with_tracing()`). Emulator variables also configure
  anonymous credentials so real credentials are never sent over plaintext HTTP.
- Outer retry loops (such as transaction runners) must not re-retry errors
  already retried by `gax` (`error.is_transient_and_before_rpc()` or calls with
  explicit `idempotent == true`), and HTTP retry classifiers inspect
  `error.http_status_code()` alongside `error.status()` for bare HTTP
  502/503/504 responses.
- Result iterators use `async fn next(&mut self) -> Option<Result<T>>`, plus
  `next_as()` and `into_stream()` behind `unstable-stream`. Never return `None`
  after a terminal error.
- `Client::builder()` takes no required arguments, and `Client::from_stub(stub)`
  takes only the stub. Mocked clients must be able to use the builder
  configuration.
- Prefer the generated `tracing::X<transport::X>` stub as the transport.
  Client-level spans use the attributes in `src/storage/src/storage/tracing.rs`,
  and their names are documented.
- Use `wkt::Struct` and `wkt::Value` instead of `serde_json` types in public
  signatures.

Required checks, in the same pull request as the new API:

- Compile-time assertions that the futures returned by public async functions
  are `Send`, including an outer `async fn` that awaits closure-taking methods
  (`async move |...|`). In Rust, non-`move` `async |...|` closures that borrow
  outer locals make enclosing futures `!Send`. Documentation examples and
  samples must use the `async move` form proven `Send`.
- For every public setter on a request, query, or builder, a test using a mock
  stub that asserts the stub receives the value. For mutually exclusive `oneof`
  setters, test chaining in both orders (`set_a().set_b()` and
  `set_b().set_a()`). If a setter overrides a client-level default, test that
  the override wins for every request kind that accepts it, and test that
  environment variables override `ClientBuilder` options (isolating ambient
  variables with `scoped_env::ScopedEnv`).
- For multi-RPC operations (pagination, continuations, resumable reads), tests
  for a transient failure that recovers, for retries running out, and for the
  behavior after the terminal error.
- For APIs that hold server resources (transactions, sessions), tests for the
  behavior when the future or handle is dropped.
- `TestLayer` span tests covering both named resources and empty/default
  resource segments (for example, default database `""` without a trailing
  slash) and verifying application callback errors do not mark client spans as
  `"ERROR"`.
- If the crate has a `DESIGN.md`, update it when the implementation diverges.

### Asynchronous Programming

All remote procedure calls (RPCs) are asynchronous and are designed to work with
the Tokio runtime.

### Error Handling

The libraries provide robust error handling, with a focus on providing clear and
actionable error messages.

### Retry and Polling Policies

The libraries include configurable policies for retrying failed requests and for
polling the status of long-running operations. These policies can be customized
by the application.

### Contribution Guidelines

Contributions are welcome. The [CONTRIBUTING.md](CONTRIBUTING.md) file provides
detailed instructions for contributors. Key documents for contributors include:

- `doc/contributor/howto-guide-set-up-development-environment.md`
- `ARCHITECTURE.md`
