# Designing Hand-written Client APIs

This guide applies to **new public APIs** in hand-written crates, such as
`src/storage`, `src/pubsub`, `src/bigquery`, `src/firestore`, and new crates
like `src/datastore`. Some existing APIs predate this guide and do not follow
every rule. Do not copy those exceptions into new code, and do not change
existing public APIs only to conform; that requires a breaking change and a
separate discussion.

[ARCHITECTURE.md](../../ARCHITECTURE.md) is binding for hand-written crates as
well as generated ones. In particular, read "No client-side validation" and
"Separate namespace for fields vs. options" before designing a new API.

Each rule below explains *why* it exists. Most rules prevent changes that would
otherwise require a breaking release when the service evolves, or when we
improve features shared across all client libraries (mocking, retries,
observability).

## Evolving with the service

### Return named, non-exhaustive response types

Functions and stub methods that wrap an RPC return a named `#[non_exhaustive]`
struct, even if today the response has a single field or no fields at all.

```rust
// ❌ Adding a field to the response later is a breaking change.
async fn allocate_ids(&self, req: AllocateIdsRequest, o: RequestOptions)
    -> Result<Response<Vec<Key>>>;
async fn rollback(&self, req: RollbackRequest, o: RequestOptions)
    -> Result<Response<()>>;

// ✅
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct AllocateIdsResponse { pub(crate) keys: Vec<Key> }
```

The same applies to high-level helpers. Do not discard parts of the server
response because the first caller does not need them. For example, a `delete()`
that supports preconditions must return the mutation result, so the caller can
tell if the delete was skipped due to a conflict. Audit every field of the
underlying `.proto` response message when designing a veneer wrapper or stub
response struct (for example, batch status flags or server-compiled continuation
queries). Never drop a proto response field because the initial client workflow
does not read it.

### Mark enum struct variants as non-exhaustive

`#[non_exhaustive]` on an enum does not apply to the fields of its struct
variants. Mark each struct variant, or wrap a `#[non_exhaustive]` struct.

```rust
#[non_exhaustive]
pub enum TransactionOptions {
    // ✅ Users cannot build or destructure this variant without `..`.
    #[non_exhaustive]
    ReadWrite { previous_transaction: Option<Bytes> },
    // ✅ Also fine.
    ReadOnly(ReadOnlyOptions),
}
```

Error enums follow the same rule: prefer `#[non_exhaustive]` struct variants
over tuple variants carrying a `String`, so variants can gain fields.

### Return `Result` from day one

Any public function that buffers work, validates input, or could plausibly need
to fail in the future returns `Result`. Changing `()` or `Option<T>` to
`Result<..>` later is a breaking change.

Handles that can outlive the scope they are valid in (for example, a cloned
transaction handle used after the transaction closure returned) must report an
error when used out of scope. They must never silently drop work.

### Traits implemented by applications take a state struct

Applications implement policy traits (retry, backoff, resume) and stub traits.
Their methods must be able to receive new inputs without breaking existing
implementations:

```rust
// ❌ Adding a deadline or server retry delay breaks every implementation.
fn on_abort(&self, error: Error, attempts: u32, elapsed: Duration) -> RetryResult;

// ✅ Follows `google_cloud_gax::retry_policy::RetryPolicy`.
fn on_error(&self, state: &RetryState, error: Error) -> RetryResult;
```

Prefer reusing the gax traits (`RetryPolicy`, `BackoffPolicy`) and their state
types. Define a crate-specific trait only when the gax trait cannot express the
decision, and then give it a `#[non_exhaustive]` state type of its own.

Every method in a stub trait has a default implementation (see
`gaxi::unimplemented::unimplemented_stub()`), so adding RPCs is not a breaking
change for application mocks.

### Decode leniently

Data returned by the service may use features newer than the client library.

- Unknown enum values, operators, or `oneof` message branches must not fail a
  read or be silently dropped:
  - **Open protocol enums** follow the generated enum pattern:
    `#[derive(Clone, Debug, PartialEq)]` (never `Eq` or `Hash`) with an
    `UnknownValue` variant wrapping
    `pub(crate) wkt::internal::UnknownEnumValue`. Deriving `Eq + Hash` is a
    semver hazard when a minor release promotes wire integer `N` from
    `UnknownValue` to a named variant, and coercing `.value().unwrap_or(0)`
    silently corrupts REST/ProtoJSON string enum values
    (`UnknownEnumValue::String`) into `0`.
  - **Protocol `oneof` message unions** (such as dynamic values, query filters,
    aggregations, or property transforms) likewise include an opaque `Unknown`
    variant preserving the raw wire representation. Dropping an unrecognized
    filter on a server-compiled continuation query silently strips a `WHERE`
    clause on subsequent pages.
- Propagate envelope fallback context (such as inherited project or database
  IDs) into **nested** decoded values (such as key-reference properties and
  embedded entities), not just top-level items.
- Do not convert server data into closed public types unless the caller needs
  them. For example, a continuation token returned by the service should stay
  opaque to the library.
- Never apply service limits (lengths, counts, ranges) to data decoded from the
  service.

Checking that a response is consistent with the request (for example, one result
per mutation sent) is fine: it detects bugs, not new service features.

### Do not validate service limits on the client

See "No client-side validation" in [ARCHITECTURE.md](../../ARCHITECTURE.md).
Limits such as "at most 30 values in an `IN` filter" or "at most 1000 keys" are
service policies, and they change. Let the service reject the request. If a
limit is mentioned in documentation, quote the proto comments and describe it as
a current service limit.

Validation that is needed to build a well-formed request is fine. Examples
include rejecting a value that cannot be represented in the wire format, an
integer overflow while computing a request field, or a key that has the wrong
shape for the operation. Report these errors with `Error::binding()`, as
`src/storage` and `src/spanner` do.

### Setters are infallible

Builder and request setters return `Self`, not `Result<Self>`. Changing
`Result<Self>` to `Self` is a breaking change, and fallible setters break method
chains. Perform any validation that is truly required in `send()` (or the
equivalent), which already returns `Result`.

Constructors and factory functions may return `Result` only when the input
conversion can genuinely fail (for example, an application `ToValue`
implementation). Prefer infallible overloads (`impl Into<Value>`) for the
common, infallible cases.

### Model wire `oneof`s faithfully

Do not represent mutually exclusive wire `oneof` branches (for example,
`read_consistency | read_time | transaction`, or `base_version | update_time`)
as independent `Option` fields that stay simultaneously `Some` on a builder or
stub request struct. Either store a single internal `enum`, or clear all sibling
branches in every `set_*()` method (documenting last-setter-wins). Otherwise,
chained setters look populated in unit tests while wire encoding silently drops
one branch. Test chaining both `set_a().set_b()` and `set_b().set_a()`.

### Keep third-party types out of public signatures

- Use `wkt::Struct` and `wkt::Value` instead of spelling `serde_json` types.
- Accessors for binary data return `&[u8]`. Accepting `impl Into<bytes::Bytes>`
  is fine.
- Do not expose `http`, `reqwest`, `tokio`, or `tonic` types. `futures` types
  are only allowed behind the `unstable-stream` feature.

### Define equality for types with inherited fields

If a public type has fields that are "inherited from the client" when unset (for
example, project or namespace), document what `Eq` and `Hash` mean and test that
a value built by the application compares equal to the same value returned by
the service. Changing the semantics of `Eq`/`Hash` later is a silent behavior
change.

## Mocking

- Clients provide `Client::from_stub(stub)` taking only the stub, consistent
  with the generated clients, `Storage`, and `Spanner`. If a mocked client needs
  configuration (database, retry policy, tracing), provide a builder path, for
  example `ClientBuilder::build_with_stub(stub)`, so mocked clients honor the
  same configuration as real ones.
- Request and response types used by the stub are constructible and inspectable
  by applications: `new()`, setters, and getters, with `pub(crate)` fields.
  Every field that participates in `PartialEq` must have a public setter and
  getter; never include uninspectable `pub(crate)` plumbing fields (such as a
  client-injected default namespace) in derived `PartialEq`, or mock equality
  assertions (`assert_eq!(got, expected)`) fail under non-default client
  configuration.
- Document, on each stub method, which client functions call it, and anything
  the client does before calling the stub (deduplication, defaulting, partition
  resolution). Document that per-RPC retries happen in the transport, not around
  the stub.

## Observability

- Prefer to use the generated `tracing::X<transport::X>` stub as the transport,
  wrapping it with conversions if the hand-written request types differ. This
  keeps generator improvements (new attributes, metrics) flowing into the crate.
- If the crate uses a hand-written transport, its `DESIGN.md` (or the module
  documentation) explains why the generated stub could not be used, and the
  per-RPC span and metrics cover the complete operation, including response
  decoding.
- Client-level spans (operations spanning multiple RPCs) use the same attribute
  set as `src/storage/src/storage/tracing.rs`: `otel.kind`,
  `gcp.client.service`, `gcp.client.version`, `gcp.client.repo`,
  `gcp.client.artifact`, and `gcp.schema.url`. Do not invent attribute
  namespaces.
- Span names (`otel.name`) refer to public methods that exist.
- Document span names and attributes in the client builder `with_tracing()`
  docs. They become a de facto contract for dashboards.
- Test spans with `google_cloud_test_utils::test_layer::TestLayer`, asserting
  `otel.name` and `gcp.resource.destination.id` for both named and empty/default
  resource segments (for example, verifying an empty default database ID does
  not emit a trailing `.../databases/` slash, and verifying application callback
  errors do not mark client spans as `"ERROR"`).

## Idiomatic APIs

### `set_*()` for fields, `with_*()` for options

Request builders and request types use `set_<field>()` for anything that changes
the request payload sent to the service, and `with_<option>()` for anything that
changes how the client library behaves (retry and backoff policies, timeouts,
idempotency, tracing, resume policies). See "Separate namespace for fields vs.
options" in [ARCHITECTURE.md](../../ARCHITECTURE.md).

```rust
client
    .read_object("projects/_/buckets/my-bucket", "my-object")
    .set_generation(123456)                       // payload field
    .with_retry_policy(AlwaysRetry.with_attempt_limit(3)) // client behavior
    .send()
    .await?;
```

Apply the rule consistently to hand-written builders, request types used by
stubs, and transaction or session runners. Do not mix `set_x()` on one type and
`with_x()` on another for the same concept. In particular:

- A runner setting that ends up in the request payload (for example, a
  transaction `read_time` or request tags) is a field: use `set_*()`.
- Types that mirror a request or response message, including response types that
  applications build in mocks, use `set_*()` for their fields, like the
  generated model types.
- Client builders (`ClientBuilder`) and client-level defaults use `with_*()`.
  This includes defaults that end up in requests, such as a default database or
  namespace, because they configure the client rather than a single request.

### Environment variables override client options

See "Environment variables override client options" in
[ARCHITECTURE.md](../../ARCHITECTURE.md). When a client supports environment
variables such as `GOOGLE_CLOUD_UNIVERSE_DOMAIN`, `GOOGLE_CLOUD_RUST_LOGGING`,
or a service `*_EMULATOR_HOST`, non-empty environment variables take higher
precedence than programmatic `ClientBuilder` calls (`with_endpoint()`,
`with_universe_domain()`, `with_credentials()`, `with_tracing()`).

In particular, a non-empty `*_EMULATOR_HOST` overrides both `with_endpoint()`
(and `with_universe_domain()`) and `with_credentials()`, configuring anonymous
credentials so production tokens are never sent over plaintext HTTP to an
emulator. Test both precedence directions with `scoped_env::ScopedEnv`, and
clear ambient environment variables in unit or conformance harnesses that test
explicit endpoints.

### Iterate with `next() -> Option<Result<T>>`

Types that return results incrementally follow the shape used by the existing
crates (`spanner::ResultSet`, `bigquery::QueryIterator`, gax paginators):

```rust
pub async fn next(&mut self) -> Option<Result<T>>;
pub async fn next_as<U: FromX>(&mut self) -> Option<Result<U>>;   // if typed
#[cfg(feature = "unstable-stream")]
pub fn into_stream(self) -> impl futures::Stream<Item = Result<T>> + Unpin;
```

After a terminal error, never return `None` (or `Ok(None)`), which is
indistinguishable from the end of the results. Either keep returning the error,
or document the behavior prominently. A `futures::Stream` that ends after
yielding `Some(Err(..))` is fine, as the caller has seen the error. Likewise, do
not end the results silently when the service reports more results but the
library cannot continue: return an error. Document whether, and how, the caller
can resume (for example, from a cursor).

Preserve the original `google_cloud_gax::error::Error` fidelity when latching a
terminal iterator error—never stringify an RPC failure into
`Error::deser(e.to_string())`—and keep failure state distinct from normal
exhaustion so post-stream accessors (such as execution metrics) return an error
rather than `Ok(None)`. Fall back to the batch end cursor at batch boundaries or
when per-item cursors are omitted, and verify that iterators bound to a scoped
handle (such as a transaction) check that the scope is still active before
fetching continuation batches. Every variant on a public result enum (such as a
`Deferred` batch entry) must be reachable via policy configuration.

### Other naming rules

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/),
  in particular C-CONV (`as_`, `to_`, `into_`), C-GETTER (no `get_` prefix), and
  C-CTOR.
- Do not name associated functions after methods of common `std` traits (`eq`,
  `cmp`, `from`, `into`, `clone`) on types that implement those traits.
- Methods named like nouns (`id()`, `name()`) are getters. Do not use those
  names for functions that construct values or return `Result`.
- Prefer `impl Into<T>` or `impl Borrow<T>` parameters over adding
  `From<&T> for T` impls.
- Use the `google_cloud_gax::error::Error` constructors for their documented
  meaning: invalid request input detected before sending is a `binding` error;
  do not report state or configuration errors as serialization, deserialization,
  or transport errors.

### Coordinate multi-layer retries

When a veneer adds an outer retry loop (such as a transaction runner) above the
per-RPC transport:

- Do not re-retry errors that the transport already retried. `Aip194Strict`
  retries `error.is_transient_and_before_rpc()` **even when
  `idempotent == false`**, and callers may pass `.with_idempotency(true)` per
  request; outer policies must exclude both to prevent `N * M` attempt
  multiplication.
- Over HTTP/JSON, proxies and load balancers may return bare HTTP 502/503/504
  responses with `error.status() == None` and
  `error.http_status_code() == Some(503)`. Inspect `http_status_code()`
  alongside `status()` in retry classifiers.

## Async APIs

New public `async fn` and futures must be `Send`. Stating this is not enough:
**add a compile-time test**. This is especially important for APIs that take
async closures (`AsyncFn`, `AsyncFnMut`), because the bound cannot require the
closure's future to be `Send`. Testing a direct `.await` expression is not
enough: a non-`move` closure (`async |tx| { ... }`) that borrows outer locals
can compile at a direct `.await` while making an **enclosing `async fn` or
`#[async_trait]` method `!Send`**
(`implementation of Send is not general enough`), breaking `tokio::spawn` and
web handlers.

```rust
fn assert_send<T: Send>(_: &T) {}

async fn outer_caller(client: &Client, key: Key) -> Result<()> {
    client
        .transaction()
        .run(async move |tx| {
            tx.get(&key).send().await?;
            Ok::<_, Error>(())
        })
        .await
        .map(|_| ())
}

#[tokio::test]
async fn outer_async_fn_is_send() {
    let client = Client::from_stub(MockStub::new());
    let fut = outer_caller(&client, Key::new("k"));
    assert_send(&fut);
}
```

Use `async move |...|` consistently in all rustdoc examples, `README` snippets,
and sample programs, and document that borrowing `async |...|` closures make
enclosing spawned futures `!Send`.

## Required tests

In addition to the usual unit tests, new public APIs need:

- **`Send` assertions** for every public future, including enclosing `async fn`
  callers for closure-taking APIs, as described above.
- **A request-level test for every public setter** on a request, query, or
  builder: use a mock stub and assert that the request the stub receives
  reflects the setter. A setter without such a test is assumed to be broken. For
  mutually exclusive `oneof` setters, test chaining both `set_a().set_b()` and
  `set_b().set_a()`. If the setter overrides a client-level default (database,
  namespace, timeout), test that the override wins for every query or request
  kind that accepts it, and test environment-variable precedence over
  `ClientBuilder` options with `scoped_env::ScopedEnv`.
- **Failure tests for multi-RPC operations** (pagination, continuation batches,
  deferred lookups, resumable reads): a transient failure that recovers, retries
  running out, and the behavior of the object after the terminal error.
- **Drop and cancellation tests** for APIs that hold server resources
  (transactions, sessions, leases): dropping the future or the handle must have
  documented, tested behavior.

## Documentation

In addition to the
[documentation standards](howto-guide-documentation-standards.md):

- Every fallible public function has an `# Errors` section. Functions that can
  panic have a `# Panics` section.
- Functions that run a closure more than once (transaction runners, retry
  helpers) warn that side effects outside the service must be idempotent.
- Batch operations that are not atomic say so.
- Multi-RPC operations document consistency: for example, whether all batches
  read from the same snapshot.
- The crate documentation (`lib.rs`) has sections on:
  - testing with mocks (`stub` module, `from_stub`),
  - the emulator, if the service has one,
  - retries and idempotency defaults,
  - observability (`with_tracing()`, span names).
- Derive macros have examples and a single reference for all attributes,
  including the required signatures for function-valued attributes.
- Service limits are quoted from the proto comments, not restated from memory.

## Proc-macro crates

See also the "Proc-macro Crates" section in the top-level
[`AGENTS.md`](../../AGENTS.md). These rules apply to new proc-macro crates.

- **100% `__private` hygiene:** Every path emitted by generated code—including
  trait paths in `impl` headers, return/error types, and constructors—goes
  through `#[doc(hidden)] pub mod __private` in the runtime crate, never its
  user-facing modules. Otherwise every item referenced by the macro is frozen,
  and `cargo-semver-checks` cannot detect violations. Add a compile-only
  contract test in a module with zero user-facing imports in scope.
- **Ambiguity is an error:** Reject duplicate field names, direct-vs-`flatten`
  prefix collisions, and pairwise `flatten`-vs-`flatten` prefix overlaps
  (`p1 == p2` or `p1.starts_with("{p2}.")`) at compile time, and reject
  duplicate property keys across nested `flatten` structs at runtime.

## Design documents

If the crate has a `DESIGN.md`, a change that diverges from it updates the
document in the same pull request. Any behavior promised in `DESIGN.md` (for
example, "transactions are rolled back when dropped") must have a test.

## Review checklist

Use this list when reviewing a pull request that adds public APIs to a
hand-written crate:

- [ ] Stub methods and RPC wrappers return named `#[non_exhaustive]` response
  types; no server data is discarded.
- [ ] Enum struct variants and error variants are `#[non_exhaustive]`.
- [ ] Functions that may fail return `Result`; setters are infallible.
- [ ] Application-implemented traits take state structs and have defaults.
- [ ] No client-side validation of service limits; decoding is lenient.
- [ ] No `serde_json`, `http`, `reqwest`, `tokio` types in public signatures.
- [ ] `set_*()` for payload fields, `with_*()` for client behavior.
- [ ] Iterators use `next() -> Option<Result<T>>` and never end silently after
  an error.
- [ ] `builder()` and `from_stub(stub)` match the other clients.
- [ ] Spans use the standard attribute set and documented names; `TestLayer`
  span tests cover both named and empty/default resource IDs.
- [ ] Open protocol enums omit `Eq` and `Hash` and preserve string unknowns;
  protocol `oneof` unions preserve unknown variants opaquely.
- [ ] Mutually exclusive wire `oneof` setters clear sibling branches;
  constructible `PartialEq` structs have no unsettable `pub(crate)` fields.
- [ ] Environment variables (such as `*_EMULATOR_HOST`) override `ClientBuilder`
  options and configure anonymous credentials for emulators.
- [ ] Outer `async fn` `Send` tests exist for closure APIs, and all docs and
  samples use `async move`.
- [ ] Outer retry policies exclude `is_transient_and_before_rpc()` and explicit
  `idempotent == true` calls, and inspect `http_status_code()` for bare
  502/503/504 responses.
- [ ] `Send` tests, setter tests, failure tests, and drop tests exist.
- [ ] `# Errors`, consistency, atomicity, and re-execution are documented.
- [ ] `DESIGN.md` matches the implementation.
