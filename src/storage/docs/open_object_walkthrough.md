# `open_object` — A Walkthrough

*Last updated: 2026-05-27*

This guide offers a complete walkthrough of `Storage::open_object` in the
`google-cloud-storage` crate. We'll look at what it actually does under the
hood, trace through every layer of the stack, see what remains running after the
call returns, and dive deep into reference sections for the trickier components.

All file paths here are relative to the `google-cloud-rust/` repository root,
and any line numbers correspond to the current state of the source code on disk.

______________________________________________________________________

## Table of Contents

- [Mental Model](#mental-model-a-phone-call-not-a-letter)
- [Files Involved](#files-involved)
- [End-to-End Call Graph](#end-to-end-call-graph)
- [Part 1 — The Linear Trace](#part-1--the-linear-trace)
  - [1. `open_object()` builds a request — lazy, no I/O](#1-open_object-builds-a-request--lazy-no-io)
  - [2. The Builder Setters](#2-the-builder-setters)
  - [3. `.send()` / `.send_and_read()`](#3-send--send_and_read)
  - [4. The Stub Trait — The Mock Seam](#4-the-stub-trait--the-mock-seam)
  - [5. The Transport Routes on Tracing](#5-the-transport-routes-on-tracing)
  - [6. `open_object_plain` — Four Crucial Lines](#6-open_object_plain--four-crucial-lines)
  - [7. `into_parts` — Splitting the Request](#7-into_parts--splitting-the-request)
  - [8. `Connector::new` — Armed but Not Fired](#8-connectornew--armed-but-not-fired)
  - [9. `ObjectDescriptorTransport::new` — Orchestration (Part 1: Prep)](#9-objectdescriptortransportnew--orchestration-part-1-prep)
  - [10. `connect()` — The Retry and Self-Heal Loop](#10-connect--the-retry-and-self-heal-loop)
  - [11. `connect_attempt` — The Dial](#11-connect_attempt--the-dial)
  - [12. `ObjectDescriptorTransport::new` — Orchestration (Part 2)](#12-objectdescriptortransportnew--orchestration-part-2)
  - [13. The Climb Back Up](#13-the-climb-back-up)
- [Part 2 — Reference and Deep Dives](#part-2--reference-and-deep-dives)
  - [A. The Client Type and the Stub Seam](#a-the-client-type-and-the-stub-seam)
  - [B. `ObjectDescriptor` Anatomy](#b-objectdescriptor-anatomy)
  - [C. The Wire Types](#c-the-wire-types)
  - [D. The Channels (Where the Confusion Lives)](#d-the-channels-where-the-confusion-lives)
  - [E. The gRPC Transport Layer](#e-the-grpc-transport-layer)
  - [F. Trailers, Status, and Errors](#f-trailers-status-and-errors)
  - [G. `ActiveRead` vs `ReadObjectResponse`](#g-activeread-vs-readobjectresponse)
  - [H. The Two "Metadata"s](#h-the-two-metadatas)
  - [I. The Worker's `run` Loop, Branch by Branch](#i-the-workers-run-loop-branch-by-branch)
  - [J. The Range-Type Stack](#j-the-range-type-stack)
  - [K. Redirects and the Resilience Machinery](#k-redirects-and-the-resilience-machinery)
  - [L. End-to-End Redirect Trace & The Durable-State Principle](#l-end-to-end-redirect-trace--the-durable-state-principle)
- [Part 3 — After `send()` Returns](#part-3--after-send-returns)
- [Where to Go Next](#where-to-go-next)

## Mental Model: A Phone Call, Not a Letter

It's helpful to remember that `open_object` isn't just a simple "fetch metadata"
request. Instead, it opens a **bidirectional gRPC streaming RPC**
(`google.storage.v2.Storage/BidiReadObject`). When you make a single call to
`open_object`, here is what happens:

1. **It dials once:** It establishes a single, long-lived bidirectional stream
   to Google Cloud Storage (GCS).
1. **The other side picks up:** The server identifies itself, and the very first
   message it sends back contains the object's metadata.
1. **The line stays open:** A background **`Worker` task** is spawned to own and
   manage the stream for its entire lifetime.
1. **You get a handle:** You receive an `ObjectDescriptor`, which acts as your
   handle. You can use it to request multiple byte-ranges over time, and all
   these requests are multiplexed over that single underlying stream.

To map this phone call metaphor to the actual code components:

| Metaphor                                | Real Thing                                                         |
| --------------------------------------- | ------------------------------------------------------------------ |
| The open line                           | The bidirectional gRPC stream                                      |
| The operator keeping it alive           | The spawned `Worker` task                                          |
| Your handset wire to the operator       | The **read-request channel** (which carries `ActiveRead` requests) |
| A dedicated answer line per request     | The per-range **byte channels** (which carry `Result<Bytes>`)      |
| The operator's memory of who you dialed | The `Arc<Mutex<BidiReadObjectSpec>>` (used for redials/reconnects) |
| The handle in your hand                 | The `ObjectDescriptor`                                             |

By the time `open_object` returns, three distinct things are alive: the gRPC
stream itself, the detached worker task running in the background, and your
descriptor handle (which communicates with the worker over a channel).

______________________________________________________________________

## Files Involved

Here's a quick map of the relevant files and the layers they represent:

| Layer                                            | File                                                            |
| ------------------------------------------------ | --------------------------------------------------------------- |
| Public client & `open_object` method             | `src/storage/src/storage/client.rs`                             |
| `DefaultStorage` alias                           | `src/storage/src/lib.rs`                                        |
| Request builder (`OpenObject`)                   | `src/storage/src/storage/open_object.rs`                        |
| App request type & `into_parts`                  | `src/storage/src/model_ext/open_object_request.rs`              |
| Stub trait (mock seam)                           | `src/storage/src/storage/stub.rs`                               |
| Default implementation (routing & orchestration) | `src/storage/src/storage/transport.rs`                          |
| Public `ObjectDescriptor`                        | `src/storage/src/object_descriptor.rs`                          |
| Descriptor stub trait & dynamic bridge           | `src/storage/src/storage/bidi/stub.rs`                          |
| Connect and reconnect logic (the real RPC)       | `src/storage/src/storage/bidi/connector.rs`                     |
| Descriptor transport (orchestration)             | `src/storage/src/storage/bidi/transport.rs`                     |
| Background stream pump                           | `src/storage/src/storage/bidi/worker.rs`                        |
| Per-range read state                             | `src/storage/src/storage/bidi/active_read.rs`                   |
| Redirect handling                                | `src/storage/src/storage/bidi/redirect.rs`                      |
| `Client` trait / tonic glue                      | `src/storage/src/storage/bidi.rs`                               |
| Generic gRPC client                              | `src/gax-internal/src/grpc.rs`                                  |
| Generic retry loop                               | `src/gax/src/retry_loop_internal.rs`                            |
| Generated protobufs                              | `src/storage/src/generated/protos/storage/google.storage.v2.rs` |

______________________________________________________________________

## End-to-End Call Graph

To give you a bird's-eye view, here is the execution trace:

```
client.open_object(b, o)            client.rs:271      build OpenObject (no I/O happens here)
  → .send()                         open_object.rs:66  go async, call the stub
    → stub::open_object             stub.rs:76         trait seam (perfect for mocking)
      → transport open_object       transport.rs:318   tracing enabled? → routes to _plain
        → open_object_plain         transport.rs:219   into_parts + Connector + Transport::new
          → into_parts              open_object_request.rs:150   splits request → (spec, ranges)
          → Connector::new          connector.rs:75              armed, still no I/O
          → ObjectDescriptorTransport::new   bidi/transport.rs:34    ORCHESTRATION BEGINS
              → (read-request channel)       bidi/transport.rs:44
              → connect → connect_attempt    connector.rs:84,140     OPENS STREAM, reads 1st msg
                  → client.start             bidi.Connector::connect
                      → bidi_stream_with_status   src/gax-internal/src/grpc.rs:198
                          → inner.streaming  src/gax-internal/src/grpc.rs:223     ← tonic / hyper / h2 / socket
              → map_ranges (byte channels)   bidi/transport.rs:74
              → Worker::new + spawn(run)     bidi/transport.rs:58,63   detaches background pump
        ← (ObjectDescriptor, readers)
```

______________________________________________________________________

# Part 1 — The Linear Trace

Let's walk through the execution step-by-step.

## 1. `open_object()` builds a request — lazy, no I/O

We start in `client.rs:271`:

```rust
pub fn open_object<B, O>(&self, bucket: B, object: O) -> OpenObject<S>
```

The body of this function is just a single line at `:276`:
`OpenObject::new(self.stub.clone(), bucket, object, self.options.clone())`. It
clones `self.stub` (which is an `Arc<S>`, making it a cheap operation) and
`self.options` (ensuring that any per-request overrides don't mutate the
underlying client).

`OpenObject::new` (`open_object.rs:107`) then packs these into a struct
(`open_object.rs:43`):

```rust
pub struct OpenObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: OpenObjectRequest,
    options: RequestOptions,
}
```

At this point, absolutely no network activity has occurred. (If you're wondering
what `S` is, check out **Reference A**.)

## 2. The Builder Setters

Next, we look at the builder setters in `open_object.rs:142–442`. There are two
main groups of setters, and each one takes `mut self`, mutates a specific field,
and returns `self`:

- **Request fields:** You have `set_generation` (`:142`),
  `set_if_generation_match` (`:162`), `set_if_generation_not_match` (`:186`),
  `set_if_metageneration_match` (`:209`), `set_if_metageneration_not_match`
  (`:232`), and `set_key` (`:256`, used for CSEK).
- **Options:** You can configure behavior with `with_retry_policy` (`:284`),
  `with_backoff_policy` (`:308`), `with_retry_throttler` (`:338`),
  `with_read_resume_policy` (`:365`), `with_attempt_timeout` (`:396`, which
  defaults to 60s), `with_user_agent` (`:415`), and `with_quota_project`
  (`:439`).

We're still entirely in-memory here; no I/O has taken place.

## 3. `.send()` / `.send_and_read()`

The action starts when you invoke `send()` at `open_object.rs:66`:

```rust
pub async fn send(self) -> Result<ObjectDescriptor> {
    let (descriptor, _) = self.stub.open_object(self.request, self.options).await?;  // :67
    Ok(descriptor)                                                                   // :68
}
```

This method consumes the builder by taking `self` by value. It calls the
stub—finally initiating network activity—and notably **discards the
`Vec<ReadObjectResponse>` readers** by using `_`.

There's also a sibling method, `send_and_read` (`:91`). This method pushes a
single range first (`:95`) and retains the single reader returned by the stub
(`:98`). It enforces that only one reader exists via `unreachable!` (`:102`).
This is the preferred path for an "open plus first read in a single round trip"
operation.

## 4. The Stub Trait — The Mock Seam

Over in `stub.rs:76`, we have the trait definition:

```rust
fn open_object(&self, _request, _options)
    -> impl Future<Output = Result<(Descriptor, Vec<ReadObjectResponse>)>> + Send {
    unimplemented_stub::<(Descriptor, Vec<ReadObjectResponse>)>()   // :82 → :104 unimplemented!()
}
```

The default implementation is just an `unimplemented!()` macro. This allows the
library to add new RPCs without breaking downstream trait implementors (as
documented at `:35`). `self.stub` is typed as `Arc<S: stub::Storage>`. The call
resolves to this trait method and is statically dispatched to whatever `S`
actually is—typically the real transport in production, or a mock implementation
during testing. (See **Reference A** for more details.)

## 5. The Transport Routes on Tracing

Inside `transport.rs:318` (which is part of
`impl super::stub::Storage for Storage`, starting at `:267`), we see the routing
logic:

```rust
async fn open_object(&self, request, options) -> Result<(...)> {
    if self.tracing { return self.open_object_tracing(request, options).await; }  // :323
    self.open_object_plain(request, options).await                                // :326
}
```

If tracing is enabled, `open_object_tracing` (`:231`) wraps the call. It sets up
a `client_request` span tagged with
`google.storage.v2.Storage/BidiStreamingRead` (`:245`), calls the `_plain`
variant, and wraps the resulting descriptor and readers with tracing decorators
(`:253–263`). For this walkthrough, we'll follow the `_plain` path.

> ⚠️ *A quick note on naming:* There are two structs named `Storage`. One is the
> **client** `Storage<S>` (`client.rs:92`), and the other is the **transport**
> `Storage` (`transport.rs:49`, which is aliased to `DefaultStorage`). Here,
> `self` refers to the transport.

## 6. `open_object_plain` — Four Crucial Lines

In `transport.rs:219`, the core logic boils down to four lines:

```rust
let (spec, ranges) = request.into_parts();                              // :224  split
let connector = Connector::new(spec, options, self.inner.grpc.clone()); // :225  dialer
let (transport, readers) =
    ObjectDescriptorTransport::new(connector, ranges).await?;           // :226  do the work (I/O)
Ok((ObjectDescriptor::new(transport), readers))                         // :227  wrap & return
```

Line `:226` is the only one that actually touches the network. `self.inner.grpc`
references the shared gRPC client attached to `StorageInner`.

## 7. `into_parts` — Splitting the Request

Looking at `open_object_request.rs:150`:

```rust
pub(crate) fn into_parts(mut self) -> (BidiReadObjectSpec, Vec<ReadRange>) {
    let ranges = std::mem::take(&mut self.ranges);   // :151
    (BidiReadObjectSpec::from(self), ranges)         // :152
}
```

Here, `std::mem::take` elegantly steals the `ranges` Vec out of the request
(leaving behind an empty Vec) so that `self` remains whole. This allows `self`
to be consumed by `From::from` on the next line, which takes its argument by
value (`:157`) to move the `String` fields out. (A partial move here would
prevent using `self` as a whole later on, which is why `mem::take` is used to
swap in a default.)

**Why do we split the request?** The `spec` represents the *connection identity
and conditions* (which are long-lived and resent on every reconnect). The
`ranges` represent the *work* (which is transient; more ranges can be added
later via `read_range`). Consequently, they are routed differently: `spec` goes
to the `Connector` (`:225`), while `ranges` go directly to the transport
(`:226`). This cleanly mirrors the protobuf wire format, where the request
message has distinct `read_object_spec` and `read_ranges` fields (see
**Reference C**).

## 8. `Connector::new` — Armed but Not Fired

In `connector.rs:75`, the connector wraps the `spec` in an `Arc<Mutex<...>>`
(`:77`), stores the options and client, and initializes `reconnect_attempts` to
`0` (`:80`). Again, there's **no I/O** happening here. The struct definition
(`:62`) looks like this:

```rust
pub struct Connector<T = GrpcClient> {
    spec: Arc<Mutex<BidiReadObjectSpec>>,   // mutable, shared identity
    options: RequestOptions,
    client: T,                              // generic for mocking; real = gaxi GrpcClient
    reconnect_attempts: u32,
}
```

As the documentation at `:45` notes, its job is to: *"Establishes (and
reconnects) bidi streaming reads."*

Handling reconnects inherently requires state. The connector must remember the
generation, read handle, routing token, and the number of attempt counts. We use
an `Arc<Mutex>` because the spec is **mutated** after creation (the server fills
in the generation, handle, and token) and must be **shared** across both the
connect retry loop and the worker's reconnect path.

## 9. `ObjectDescriptorTransport::new` — Orchestration (Part 1: Prep)

Moving to `bidi/transport.rs:34`, we prepare the table before actually dialing
out:

```rust
let (tx, rx) = tokio::sync::mpsc::channel(100);                    // :44  REQUEST channel
let requested_ranges = ranges.into_iter().map(|r| r.0).collect::<Vec<_>>(); // :45  unwrap newtype
let proto_ranges = requested_ranges.iter().enumerate()
    .map(|(id, r)| r.as_proto(id as i64)).collect::<Vec<_>>();              // :46–50  mint read-ids
```

- **Line `:44`** creates the **read-request channel** between the descriptor and
  the worker. The descriptor holds onto `tx`, while `rx` will be handed to the
  worker (`:63`). This channel carries new read requests (`ActiveRead`).
- **Lines `:46–50`** assign an index to each range, establishing its
  **read-id**. This identifier acts as the address used to reliably route
  responses back to the correct reader. (For a deep dive into all the channels,
  see **Reference D**.)

## 10. `connect()` — The Retry and Self-Heal Loop

Over in `connector.rs:84`, we find `connect()`. It's actually **a wrapper**
around the `connect_attempt` function, rather than the dial operation itself. It
carefully clones the necessary components for the retry loop so that the
resulting future remains `Send` and `Sync` (`:89–96`, exactly as described in
the comment at `:88`). Note that calling `spec.clone()` just bumps the `Arc`
reference count; it's still the exact same shared spec.

Inside, the `inner` closure (`:98`) calculates
`attempt_timeout = min(per-attempt, remaining-budget)` (fulfilling the promise
in the `with_attempt_timeout` docs, at `:99`) and wraps a single
`connect_attempt` in a `tokio::time::timeout`.

The process is driven by `retry_loop` (`:107`): it continuously attempts the
call as long as the retry policy permits, success hasn't been achieved, and the
throttler allows it, sleeping for the appropriate backoff duration between
attempts. The flag `idempotent` is set to `true` (`retry_loop_internal.rs:54`)
because opening a read is safely side-effect-free and retryable. Notably, **this
exact same `connect()` function is reused by the worker whenever a reconnect is
needed (`connector.rs:125`).**

## 11. `connect_attempt` — The Dial

In `connector.rs:140`, the actual dial happens in two main movements.

**Movement A — Composing and sending the opening message:**

```rust
let request = BidiReadObjectRequest {
    read_object_spec: Some((*spec.lock()...).clone()),   // :147  snapshot the (enriched) spec
    read_ranges: ranges,                                 // :148
};
// :150–176  validate bucket is `projects/_/buckets/*`, else BindingError BEFORE any network
// :177–183  build x-goog-request-params = bucket=… (+ &routing_token=… if set)
let (tx, rx) = tokio::sync::mpsc::channel::<BidiReadObjectRequest>(100);  // :185  wire-out (mpsc)
tx.send(request.clone()).await...;                                       // :186  preload 1st msg
// :188–197  GrpcMethod + path /google.storage.v2.Storage/BidiReadObject
let response = client.start(extensions, path, rx, options, &X_GOOG_API_CLIENT_HEADER,
                            &x_goog_request_params).await?;              // :199  OPEN THE STREAM
```

Here, `client.start` (`bidi.rs:69`) takes `rx`, wraps it in a `ReceiverStream`,
and invokes `bidi_stream_with_status` (refer to **Reference E**). Taking a
snapshot of the spec under lock here ensures that any **reconnect** will
automatically include the previously enriched generation and handle.

**Movement B — The Handshake:**

```rust
let response = match response { Ok(r) => r, Err(status) => return Err(handle_redirect(spec, status)) }; // :211
let (metadata, mut stream, _) = response.into_parts();   // :216  metadata=transport headers
let headers = metadata.into_headers();                   // :217
match stream.next_message().await {                      // :218  read the FIRST message
    Ok(Some(m)) => {
        let mut guard = spec.lock()...;                  // :220  ENRICH the spec:
        if let Some(g) = m.metadata.as_ref().map(|o| o.generation) { guard.generation = g; }  // :222
        if m.read_handle.is_some() { guard.read_handle = m.read_handle.clone(); }             // :225
        Ok((m, headers, Connection::new(tx, stream)))    // :227
    }
    Ok(None) => Err(Error::io("...closed before start")), // :229
    Err(status) => Err(handle_redirect(spec, status)),    // :230
}
```

It is required that the first message carries the object metadata (this is
strictly enforced in step 12). The writes at `:222` and `:225` represent the
**enrichment** phase. If the app initiated the request with `generation: 0`
(meaning "latest"), we capture the concrete generation resolved by the server to
pin our snapshot, alongside the read handle to make future reconnects cheaper.

This block returns `(initial_response, headers, connection)`. This matches
exactly what `connect()` outputs, and what the `connector.connect(...)` call
back in step 9 ultimately receives.

> *Clarification:* Don't confuse the two uses of the word "metadata" here.
> `metadata` turning into `headers` represents the **transport** metadata (the
> gRPC response headers). Conversely, `m.metadata` is the actual **object**
> metadata (the `Object` protobuf). See **Reference H** for details.

## 12. `ObjectDescriptorTransport::new` — Orchestration (Part 2)

Returning to `bidi/transport.rs:51`, armed with our
`(initial, headers, connection)`:

```rust
let (mut initial, headers, connection) = connector.connect(proto_ranges).await?;  // :51
let object = FromProto::cnv(initial.metadata.take().ok_or_else(|| {
    Error::deser("initial response in bidi read must contain object metadata") })?)...;  // :52–55
let object = Arc::new(object);                                                    // :56
let (active, readers) = Self::map_ranges(requested_ranges, &tx, &object);         // :57
let mut worker = super::worker::Worker::new(connector, active);                   // :58
worker.handle_response_success(initial).await.map_err(Error::io)?;                // :59–62
let _handle = tokio::spawn(worker.run(connection, rx));                           // :63
Ok((Self { object, headers, tx }, readers))                                       // :64–71
```

Let's break down this crucial block:

- **Lines `:52–56`:** We use `initial.metadata.take()` (the `Option` equivalent
  of `mem::take`, stealing the field while allowing `initial` to be reused at
  `:59`). If metadata is missing, it throws a deserialization error. We convert
  the protobuf to the domain `Object` and wrap it in an `Arc` because it needs
  to be shared between the descriptor and every reader.
- **Line `:57`:** `map_ranges` (defined at `:74`, with the per-range channel
  created at `:82`) mints exactly one **byte channel per range**. This yields an
  `ActiveRead` (the sender side, kept by the worker) and a `RangeReader` wrapped
  as a `ReadObjectResponse` (the receiver side, handed to you). See **Reference
  D** and **Reference G**.
- **Line `:58`:** We instantiate the worker. The `connector` is **moved in**
  here. Because the worker now completely owns the connector, it can freely call
  `connector.reconnect(...)` from within its detached task later on. The
  `active` variable transforms into the read-id `HashMap` (`worker.rs:38–43`).
- **Lines `:59–62`:** We immediately feed the worker the first message's
  `object_data_ranges`. This happens **synchronously, prior to spawning the
  task**, leveraging `?`. For a standard `send()`, this might be a no-op, but
  for `send_and_read`, it delivers the very first chunk of data instantly. The
  `?` guarantees that if the first response is bad, `send()` will fail right
  away.
- **Line `:63`:** The worker is spawned via `tokio::spawn` and is completely
  **detached** (we deliberately drop the handle). It now has full ownership of
  the connection and will run its loop until the stream concludes naturally, an
  unrecoverable error strikes, or the descriptor's `tx` channel is dropped.
- **Lines `:64–71`:** Finally, we return the fully initialized transport
  (`object`, `headers`, `tx`) along with the `readers`.

## 13. The Climb Back Up

The call stack unwinds cleanly:

```
ObjectDescriptorTransport::new ─▶ (transport, readers)        bidi/transport.rs:64
open_object_plain: ObjectDescriptor::new(transport)           transport.rs:227   wraps the stub into the public type
stub::open_object returns it                                  transport.rs:326
.send(): let (descriptor, _) = …; Ok(descriptor)              open_object.rs:67   discards the readers
   (send_and_read instead retains the descriptor plus the one reader) open_object.rs:98
```

You are now successfully holding an `ObjectDescriptor`.

______________________________________________________________________

# Part 2 — Reference and Deep Dives

## A. The Client Type and the Stub Seam

Let's look at `client.rs:92`:

```rust
pub struct Storage<S = crate::stub::DefaultStorage>
where S: crate::stub::Storage + 'static
{ stub: std::sync::Arc<S>, options: RequestOptions }
```

- **`S`** represents the **stub type**—the actual engine that executes the RPCs.
  It is bounded by the `stub::Storage` trait we discussed back in step 4.
- **Default `S = DefaultStorage`**: This is simply a handy alias for the
  transport. In `lib.rs:122`, you'll find
  `pub use crate::storage::transport::Storage as DefaultStorage;`. This means if
  you don't explicitly specify otherwise, `S` resolves to the real network
  transport (`transport::Storage`).
- **`Arc<S>`**: Thanks to the `Arc`, calling `self.stub.clone()` just bumps a
  reference count; it doesn't do a heavy copy.
- **Why make it generic?** It's all about testing. You can instantiate
  `Storage<MockStub>` to exercise the entire client chain against a mocked
  backend. The `from_stub` method (`client.rs:130`) exists explicitly for this
  purpose. Since `S` is a type parameter rather than a `dyn` trait object,
  method dispatch remains **static**—no virtual table overhead.

To use an analogy: if `open_object` is the act of placing a phone call, the
`stub` is the phone itself. In production, you have a real phone wired directly
to GCS. In tests, you swap it for a toy phone that plays pre-recorded responses.

## B. `ObjectDescriptor` Anatomy

The `object_descriptor.rs` file defines the public handle you receive.
Structurally, it's a **hollow newtype wrapped around a trait object** (`:58`):

```rust
pub struct ObjectDescriptor { inner: Arc<dyn ObjectDescriptorStub> }
```

- `Arc` ensures it is cheap to `Clone`. If you clone the descriptor, both copies
  share the same underlying open stream and worker task.
- `dyn` enables type erasure, letting the same handle wrap either the real
  transport or a mock implementation.
- The three methods exposed to the user—`object()` (`:79`), `read_range()`
  (`:112`), and `headers()` (`:133`)—simply forward their calls directly to
  `self.inner`. All the real behavioral logic lives inside the inner
  `ObjectDescriptorTransport`.
- The `new<T>` constructor (`:141`) takes any type `T: stub::ObjectDescriptor`
  and wraps it in the `Arc<dyn>`. Conversely, `into_parts` (`:150`) extracts and
  returns the raw `inner` object (which is how the tracing path unpacks it).

There are some excellent documentation notes at `:44–56`: the handle is
described as being *"analogous to a 'file descriptor'"*. It warns that reads
across different ranges have **no ordering guarantees**. Crucially, it
highlights a major footgun: if you fail to drain data from one reader, its
buffer will fill up, which will **stall all other reads** sharing the stream.

**The Two-Trait Pattern:** You might notice that the trait used in the `inner`
field type (`:21`, `:59`) differs slightly from the trait bound on `new<T>`
(`:143`). Here is why:

- `crate::stub::ObjectDescriptor` (`stub.rs:92`): This is the **ergonomic**
  trait featuring native `async fn`. However, native async traits aren't
  dyn-compatible (object safe).
- `...bidi::stub::dynamic::ObjectDescriptor` (`bidi/stub.rs:24`): This is the
  **dyn-safe** variant, utilizing `#[async_trait]` to return boxed futures. This
  is what actually gets stored inside the `Arc<dyn>`.
- The bridge: At `bidi/stub.rs:31`, a blanket implementation automatically
  implements the dyn-safe trait for any type `T` that implements the ergonomic
  `stub::ObjectDescriptor`. This means implementors get the clean, modern API,
  while the library handles the messy type erasure internally.

Is it truly like a file descriptor? Yes, specifically, it resembles the
**`pread` flavor**: you open it once, and then you can issue multiple positional
`(offset, length)` reads. There is no internal seek pointer, and dropping the
descriptor is effectively closing it (as seen in `worker.rs:88`, where the
worker gracefully exits when the descriptor's `tx` channel disappears). But it's
also *more* powerful than a simple file descriptor: it caches metadata
in-process (`object()` doesn't require an I/O call), it is **snapshot-pinned**
to a specific object generation (`connector.rs:221`), and it intelligently
**self-heals** by reconnecting under the hood (`worker.rs:153`).

## C. The Wire Types

Let's look at the data structures carrying the requests and responses.

**`OpenObjectRequest`** (`open_object_request.rs:30`): This is the user-friendly
container built by the application. It holds the bucket, object name,
generation, the four `if_*` preconditions, CSEK parameters, and the initial
`ranges`. It deliberately **omits** the `read_handle` and `routing_token` (as
noted in the comment at `:26`), because those values are strictly supplied by
the server.

**`BidiReadObjectSpec`** (`google.storage.v2.rs:410`): This is the generated
protobuf type representing the "which object and under what conditions" half of
the request. The fields can be grouped by where they originate:

- *App-supplied:* `bucket` (`:412`), `object` (`:414`), `generation` (`:416`),
  the four `if_*` conditions (`:418–424`), and `common_object_request_params`
  (`:426`).
- *Server-supplied (filled in by the connector):* `read_handle` (`:431`) and
  `routing_token` (`:433`).
- The `read_mask` field (`:429`) is marked `#[deprecated]`.
- The `generation` field actually switches ownership: it starts as the
  application's request (e.g., `0` for "latest"), but the connector overwrites
  it with the concrete value resolved by the server (`connector.rs:221`),
  effectively pinning the snapshot.

A note on the `#[prost(...)]` decoder: `tag = "N"` represents the wire field
number (names aren't transmitted, and tags may have gaps if fields were
deprecated). `Option<T>` maps to the protobuf `optional` keyword, distinguishing
an intentionally "unset" value from a literal `0`.

**`BidiReadObjectRequest`** (`:446`): This is the actual payload sent by the
client over the stream:

```rust
pub struct BidiReadObjectRequest {
    pub read_object_spec: Option<BidiReadObjectSpec>,  // tag 1
    pub read_ranges: Vec<ReadRange>,                   // tag 8 (repeated)
}
```

The data shapes inherently encode the **streaming grammar**: the spec is an
`Option`, meaning you send it **exactly once** per connection (the first message
has `Some`, subsequent ones send `None`, as seen in `worker.rs:204`; a reconnect
will re-send the enriched spec). The ranges are `repeated`, meaning you can ask
for any number of chunks at any time.

**`BidiReadObjectResponse`** (`:463`): This is what the server streams back to
the client. It contains `object_data_ranges` (`:465`, the raw bytes, tagged by
their read-id), an optional `metadata: Option<Object>` (`:467`, the object
metadata), and a `read_handle` (`:469`).

## D. The Channels (Where the Confusion Lives)

The variables `tx` and `rx` are standard names for sender and receiver halves of
a channel, and they get reused extensively. To trace the logic, you must track
each channel by **what it carries**, not by its variable name. A single
`open_object` call involves **four distinct conduits**: two in-process `mpsc`
channels and the two halves of the gRPC network stream.

| Name                              | Carries                  | Direction           | Created At                            | `tx` Held By                                 | `rx` Held By                                |
| --------------------------------- | ------------------------ | ------------------- | ------------------------------------- | -------------------------------------------- | ------------------------------------------- |
| **Read-Request Channel**          | `ActiveRead`             | descriptor → worker | `bidi/transport.rs:44`                | descriptor (`Self.tx`) + each reader (clone) | worker (`requests`)                         |
| **Byte Channels** (one per range) | `Result<Bytes>`          | worker → one reader | `bidi/transport.rs:82` and `:100`     | worker (inside each `ActiveRead`)            | that specific reader (`RangeReader`)        |
| **Wire-Out**                      | `BidiReadObjectRequest`  | client → server     | `connector.rs:185` (preloaded `:186`) | worker (`Connection.tx`)                     | gRPC (as the request stream)                |
| **Wire-In**                       | `BidiReadObjectResponse` | server → client     | returned from `client.start`          | n/a (managed by tonic)                       | worker (`Connection.rx`, tonic `Streaming`) |

The first two are strictly **in-process** plumbing (Tokio mpsc channels—they
never leave the application). The latter two represent the actual **gRPC stream
halves** sent over the network.

### Detailed Breakdown per Channel

#### 1. Read-Request Channel (Descriptor → Worker)

- **Type:** `mpsc::Sender<ActiveRead>` / `mpsc::Receiver<ActiveRead>`
- **Created:** `bidi/transport.rs:44`, with a capacity of `100`.
- **tx held by:** The `ObjectDescriptorTransport` (as its `tx` field) and cloned
  into each `RangeReader`.
- **rx held by:** The worker task, held locally inside `run` (named `requests`).
- **Purpose:** Whenever `descriptor.read_range(...)` is called, it pushes a new
  `ActiveRead` onto this channel. Branch B of the worker's `run` loop drains it
  via `recv_many`.
- **Lifetime:** Exactly one exists per `open_object` call. It lives as long as
  the descriptor itself or any active reader remains. When all `Sender` clones
  are dropped, Branch B observes `r == 0`, signaling the worker to exit cleanly.

#### 2. Byte Channels (Worker → Reader, One *Per Range*)

- **Type:** `mpsc::Sender<ReadResult<Bytes>>` /
  `mpsc::Receiver<ReadResult<Bytes>>`
- **Created:** `bidi/transport.rs:82` inside `map_ranges` (for ranges
  established during connection) and at `bidi/transport.rs:100` for post-open
  `read_range` calls. Capacity is `100`.
- **tx held by:** The `ActiveRead` corresponding to that specific range (stored
  in the worker's `self.ranges` HashMap).
- **rx held by:** The `RangeReader` for that range (wrapped inside the
  `ReadObjectResponse` returned to the user).
- **Purpose:** When the worker receives object data from the server tagged with
  a specific read-id, it looks up the corresponding `ActiveRead` in its HashMap
  and pushes the bytes into that range's byte channel. The reader extracts them
  by awaiting `.next()`.
- **Lifetime:** One exists per active range. A `send_and_read(N)` call creates
  `N` byte channels. Every subsequent call to `read_range` adds one more.

#### 3. Wire-Out (Worker → Tonic → Server)

- **Type:** `mpsc::Sender<BidiReadObjectRequest>` /
  `mpsc::Receiver<BidiReadObjectRequest>`
- **Created:** `connector.rs:185` (and preloaded at `:186`) during
  `connect_attempt`, with a capacity of `100`.
- **tx held by:** The worker (stored in `Connection.tx`, unpacked into a local
  `tx` variable in `run`).
- **rx held by:** Tonic, after being wrapped in a `ReceiverStream` and passed
  into `inner.streaming(...)` (`src/gax-internal/src/grpc.rs:198`).
- **Purpose:** The worker pushes outbound `BidiReadObjectRequest` messages here.
  Tonic drains the receiver, serializes the messages with Prost, frames them,
  and pushes them down onto the HTTP/2 socket.
- **Lifetime:** **One per connection attempt.** Whenever a reconnect occurs, a
  brand new pair is created, and the old one is discarded. (The opening message
  is preloaded via `tx.send(request.clone()).await` before the stream even
  starts, guaranteeing the server sees the spec on the very first DATA frame.)

#### 4. Wire-In (Server → Tonic → Worker)

- **Type:** `tonic::Streaming<BidiReadObjectResponse>` (Note: this is tonic's
  custom stream type, not an mpsc pair, exposed via the `TonicStreaming` trait
  at `bidi.rs:38–49` for mockability).
- **Created:** Returned directly from `inner.streaming(...)`
  (`src/gax-internal/src/grpc.rs:223`) and exposed via the `Connection` struct.
- **Producer side:** Owned and managed internally by Tonic as it buffers
  incoming HTTP/2 DATA frames.
- **Consumer side:** Held by the worker (in `Connection.rx`, unpacked locally in
  `run`).
- **Purpose:** Branch A of the worker's loop calls `rx.next_message()` to pull
  responses one by one. It can return `Ok(Some(response))`, `Ok(None)` for a
  clean termination, or `Err(status)` for an error or a severed connection.
- **Lifetime:** **One per connection attempt.** A new connection guarantees a
  new wire-in stream.

### Counts at a Glance

If you run a `send_and_read` requesting `N` initial ranges on the very first
connection:

| Channel Type         | Count                                    |
| -------------------- | ---------------------------------------- |
| Read-Request Channel | 1                                        |
| Byte Channels        | `N` (one for each range)                 |
| Wire-Out             | 1                                        |
| Wire-In              | 1 (technically a tonic stream, not mpsc) |

Every new `read_range` call adds precisely one new byte channel, leaving the
other counts unchanged. If the network drops and the worker successfully
reconnects, the wire-in and wire-out conduits are replaced entirely, while the
user-facing read-request and byte channels survive without interruption.

### The Overarching Pattern

- **Read-Request Channel:** The command path from "user code to worker."
- **Byte Channels:** The data path from "worker back to user code."
- **Wire-Out / Wire-In:** The transient network links managed via Tonic, totally
  disposable during a reconnect.

You have two durable, user-facing channels that survive reconnects, and two
network-facing channels that are ephemeral. When a reconnect happens, only the
network side gets rebuilt; the user-facing side remains completely oblivious.

**The Shadowing in `map_ranges`:** At `bidi/transport.rs:57`, the code passes
`&tx` (the sender for the read-request channel) into the function. The signature
renames it to `requests` (`:76`). Then, at `:82`, it creates a *new* channel
`(tx, rx)` for the bytes, reusing the exact same variable names. Consequently,
inside the closure, `requests` is the read-request sender (which gets cloned
into each reader), and the new `tx` is the byte channel sender (which is handed
to the `ActiveRead`).

**Who holds what afterward:**

- **Descriptor:** The read-request channel `tx` (plus `object` and `headers`).
- **Worker:** The read-request channel `rx` (listening for new intents), the
  wire-out `tx` and wire-in stream (talking to the server), and a byte channel
  `tx` tucked inside each `ActiveRead` (pushing bytes to readers). Inside
  `worker.run` (`worker.rs:65`), the connection is destructured into `rx` and
  `tx`, again reusing the names.
- **Each Reader:** Its dedicated byte channel `rx` (to receive bytes) and a
  clone of the read-request channel `tx` (to poke the worker).

## E. The gRPC Transport Layer

The function `client.start` (`bidi.rs:69`, implementing `Client` for
`gaxi::grpc::Client`) wraps the outbound `rx` channel into a `ReceiverStream`
and invokes `bidi_stream_with_status` (`src/gax-internal/src/grpc.rs:198`).
Let's peek inside:

```
make_headers(…)                 src/gax-internal/src/grpc.rs:212 → :555   (user-agent, x-goog-user-project, etc.)
add_auth_headers(headers)       src/gax-internal/src/grpc.rs:213 → :539   (fetches the Bearer token)
MetadataMap::from_headers(…)    src/gax-internal/src/grpc.rs:214          (converts HTTP headers to gRPC metadata)
tonic::Request::from_parts(…)   src/gax-internal/src/grpc.rs:215          (packages metadata and the outbound STREAM)
ProstCodec::default()           src/gax-internal/src/grpc.rs:216          (the protobuf serializer/deserializer)
self.inner.clone()              src/gax-internal/src/grpc.rs:217          (the underlying Tonic InnerClient at :71)
inner.ready().await             src/gax-internal/src/grpc.rs:218          (waits for channel readiness)
inner.streaming(req, path, …)   src/gax-internal/src/grpc.rs:223          ← THE ACTUAL TONIC BIDI CALL
Ok(result)                      src/gax-internal/src/grpc.rs:231
```

Below `:223`, everything descends into the lower layers: Tonic → Hyper → HTTP/2
→ raw socket.

**The Double `Result`:** Line `src/gax-internal/src/grpc.rs:206` returns
`Result<tonic::Result<…>>`.

- The **outer** result represents GAX-level errors (setup failures, header
  issues, auth errors, `ready` checks), propagated via `?`.
- The **inner** `tonic::Result` (which is `Result<_, tonic::Status>`) represents
  the actual outcome of the gRPC call.

While `bidi_stream` (`:168`) eagerly flattens the inner error using
`to_gax_error` (`:190`), `bidi_stream_with_status` deliberately preserves the
raw `Status`. This is crucial because **the Storage layer needs to extract
redirect details directly from the Status** (as documented at `:195–197`). This
is exactly why `connect_attempt` can gracefully handle
`Err(status) → handle_redirect` (`connector.rs:211/230`).

## F. Trailers, Status, and Errors

gRPC trailers (such as `grpc-status`, `grpc-message`, and
`grpc-status-details-bin`) arrive **after** the message body. This
implementation never reads them as a raw metadata map; instead, they surface
dynamically as the **terminal result when reading the stream**.

Calling `next_message` (`bidi.rs:46`) invokes Tonic's `message()`, returning a
`Result<Option<T>, Status>`:

- `Ok(Some(msg))`: A normal data message (we're still inside the body).
- `Ok(None)`: A clean termination; the trailers reported `grpc-status: OK`.
- `Err(status)`: This indicates either an error trailer **or** a prematurely
  severed connection (Tonic synthesizes a `Status` for the latter). Because both
  scenarios look identical, the worker simply treats any `Err` by triggering a
  reconnect (`worker.rs:116`).

You can parse the type signature as answering two questions: `Result` asks "Did
it end okay, or error?", while `Option` asks "Is there more data, or are we
done?".

The status's **details** (derived from the `grpc-status-details-bin` trailer)
are highly relevant. Tonic decodes them into `Status::details()`, and
`handle_redirect` (`redirect.rs:25`) uses Prost to decode them, extracting the
routing token and read handle into the spec (`:30–31`). Thus, trailer-borne
redirect payloads are the primary driver of reconnect routing.

**How the status reaches you:** The function `to_gax_error` (imported from
`gaxi::grpc::from_status` at `redirect.rs:19` and called at `redirect.rs:36`)
converts the `tonic::Status` into a standard `gax::Error`, which `send()`
eventually returns. You inspect this by reading `err.status().code` (for
example, the `open_object` test asserts that `s.code == Code::NotFound` at
`transport.rs:528`).

**To manually inspect trailing metadata,** you would need to completely drain
the body until `Ok(None)` and then invoke Tonic's `Streaming::trailers()`.
However, this codebase never needs to do that (it only cares about the status).
To implement it, you'd add a `trailers()` method to the `TonicStreaming` trait
(`bidi.rs:38`) and call it on the clean-exit path after the worker's loop
terminates (`worker.rs:74`/`:96`), while `rx` is still fully owned.

## G. `ActiveRead` vs `ReadObjectResponse`

These are simply the two opposite ends of a single range's **byte channel**,
forged together in `map_ranges`/`read_range`.

**`ActiveRead`** (`active_read.rs:25`) is the **worker's** write-end, endowed
with routing logic:

```rust
pub(crate) struct ActiveRead {
    state: RemainingRange,                       // tracks how much of the range remains
    sender: Sender<ReadResult<bytes::Bytes>>,    // the WRITE end of the channel
}
```

- `handle_data` (`:41`): When a chunk arrives, it updates `state`, **verifies
  the crc32c checksum** (`:52–60`), and pushes the bytes.
- `as_proto(id)` (`:77`): Generates a proto range describing only the
  *remaining* bytes, ensuring a reconnect only asks for what hasn't been
  received yet (`active_read.rs:77`).
- `interrupted(error)` (`:67`): Pushes a terminal `Err` down the channel to
  intentionally fail the reader.

**`ReadObjectResponse`** (which wraps a `RangeReader`) is **your** read-end
faucet. It simply holds the `Receiver`, yields sequential chunks via
`.next().await`, and contains no complex logic.

Because the channel inherently carries `Result<Bytes, ReadError>`, errors share
the exact same conduit as data. Thus, `reader.next().await` yields an
`Option<Result<Bytes>>` (`Some(Ok)` means a chunk, `Some(Err)` means failure,
and `None` means done). This cleanly explains why, in step 12, the `active` half
is given to the worker while the `readers` half is handed back to you.

## H. The Two "Metadata"s

During the handshake inside `connect_attempt`, two distinct concepts are
confusingly both referred to as "metadata" just lines apart:

|                    | `headers` (`connector.rs:216`)                                        | `m.metadata` (`connector.rs:221`)          |
| ------------------ | --------------------------------------------------------------------- | ------------------------------------------ |
| **What it is**     | gRPC **response headers** (gRPC confusingly calls headers "metadata") | The **object's** actual storage properties |
| **Type**           | `MetadataMap` → `HeaderMap`                                           | `Option<Object>`                           |
| **Extracted From** | The response **envelope** (head)                                      | The very first **body message**            |
| **Becomes**        | `descriptor.headers()`                                                | `descriptor.object()`                      |
| **Example**        | `content-type: application/grpc`, `x-guploader-uploadid`              | `generation: 123456`, `size: 42`           |

Think of it like an HTTP response: the envelope/headers represent transport
metadata wrapping the call itself, while the body carries the message stream.
The very first chunk in that body happens to carry the object's storage metadata
inside a field literally named `metadata`.

## I. The Worker's `run` Loop, Branch by Branch

At `worker.rs:59`, after `Worker::new` initializes everything and the first
message is processed via `handle_response_success`, the worker is launched in
the background via `tokio::spawn(worker.run(connection, rx))`
(`bidi/transport.rs:63`). The `run` method functions as the perpetual background
pump.

### Skeleton of the Loop

```rust
pub async fn run(mut self, connection: Connection<C::Stream>, mut requests: Receiver<ActiveRead>) -> LoopResult<()> {
    let mut ranges = Vec::new();
    let (mut rx, mut tx) = (connection.rx, connection.tx);    // wire-in and wire-out
    let error = loop {
        tokio::select! {
            m = rx.next_message() => { ... }                   // :71  Branch A — Inbound
            r = requests.recv_many(&mut ranges, 16) => { ... } // :88  Branch B — Outbound
        }
    };
    drop(rx); drop(tx);
    let Some(e) = error else { return Ok(()); };
    while let Some(mut r) = requests.recv().await { ... }      // drain any straggling readers
    Err(e)
}
```

The `tokio::select!` macro races both arms every single iteration, running
whichever resolves first. Since it runs in a single task, only one arm executes
per iteration (there is no parallel execution inside the worker). The internal
`loop` acts as an expression: whatever value it `break`s with gets assigned to
`error`, which the post-loop cleanup code evaluates.

There are three exit paths:

- `break None`: A clean shutdown (the loop yields `None`).
- `break Some(e)`: A fatal error occurred.
- No break: Keep looping indefinitely.

After breaking, a clean exit returns `Ok(())`. An error exit aggressively drains
any pending `read_range` calls that were queued too late, failing them all via
`r.interrupted(e.clone())` before returning `Err(e)`. The `println!` sitting at
`:104` is just leftover debug output.

### Branch B — Outbound (Read-Request Channel → Wire-Out)

Located at `worker.rs:88–93`:

```rust
r = requests.recv_many(&mut ranges, 16) => {
    if r == 0 {
        break None;
    };
    self.insert_ranges(tx.clone(), std::mem::take(&mut ranges)).await;
},
```

**Understanding `recv_many`:** This performs a batched receive. There are two
distinct buffers at play here:

- The **channel's internal queue** (capacity `100`, configured at
  `bidi/transport.rs:44`). Senders continuously push into this queue while the
  worker is busy executing other tasks.
- The **Vec scratch buffer named `ranges`** (declared at `worker.rs:64`), passed
  here via mutable reference (`&mut`).

`recv_many(buf, max)` blocks until the queue holds at least one item, then
aggressively drains *up to* `max` items from the queue directly into `buf`,
returning the total count. If seven `read_range` calls queue up while the worker
is busy on Branch A, Branch B drains all seven simultaneously on its next pass.
This keeps latency low for the first item while boosting overall throughput.

The limit `16` restricts the drain count per cycle to keep the loop responsive
to incoming network data (Branch A). The `100` is the channel capacity,
providing backpressure headroom. These are unrelated tunables.

**`r == 0` — Graceful Shutdown:** This only evaluates to true when the channel
is strictly **closed *and* empty**—meaning every single `Sender<ActiveRead>`
clone has been dropped *and* the internal queue is drained. Those clones reside
in the descriptor (`Self.tx`) and inside every `RangeReader` (`requests.clone()`
at `bidi/transport.rs:84`). Thus, this condition only triggers when the
descriptor and all active readers are destroyed. `break None` then exits the
loop cleanly. (A temporary lull in requests simply causes `recv_many` to sleep;
it does *not* return 0).

**The Reality of `mem::take(&mut ranges)`:** `mem::take` functionally acts as
`mem::replace(&mut v, T::default())`:

- It extracts and returns the current filled Vec.
- It leaves a brand new `T::default()` (which for a Vec is `Vec::new()`, a
  zero-capacity, allocation-free shell) in the slot.

It does **not** preserve the allocation between iterations. The memory
allocation rides along with the returned Vec into `insert_ranges` and is
deallocated when that Vec goes out of scope. The next iteration's `recv_many`
starts completely fresh with a zero-capacity Vec.

The genuine reason for using `mem::take` here is strict **borrow-checker
hygiene**. The variable `ranges` must survive across the loop because the
`recv_many` future actively borrows `&mut ranges` each iteration. Moving the Vec
out directly (`insert_ranges(.., ranges)`) would leave the variable in an
invalid, moved-from state. `mem::take` safely swaps a fresh default in,
satisfying the compiler in a single neat expression.

**`tx.clone()` Sidestepping Borrow Constraints:** Invoking `Sender::clone` on an
mpsc channel is just a cheap reference count bump. Passing the clone *by value*
into `insert_ranges` allows that function to execute `.send(..).await` without
retaining a borrow of the outer `tx` variable across an `.await` boundary. The
worker's primary `tx` survives safely in the main `run` scope for the next loop.

**Inside `insert_ranges` (`worker.rs:194–214`):**

```rust
async fn insert_ranges(&mut self, tx: Sender<BidiReadObjectRequest>, readers: Vec<ActiveRead>) {
    let mut ranges = Vec::new();                  // local proto buffer (distinct from outer `ranges`!)
    for r in readers {
        let id = self.next_range_id;              // :197  mint a fresh, monotonic id
        self.next_range_id += 1;
        let request = r.as_proto(id);             // :200  generate proto for *remaining* range
        self.ranges.lock().await.insert(id, r);   // :201  register in the routing HashMap
        ranges.push(request);                     // :202  append to the outgoing wire message
    }
    let request = BidiReadObjectRequest {
        read_ranges: ranges,
        ..BidiReadObjectRequest::default()        // spec: None — it was already sent
    };
    if let Err(e) = tx.send(request).await {      // :211  ship it; failure is strictly non-fatal
        tracing::error!("error sending read range request: {e:?}");
    }
}
```

For every single `ActiveRead`:

1. **Mint a fresh id.** `self.next_range_id` increments monotonically; ids are
   never reused over the worker's life. No atomics are required since
   `&mut self` provides exclusive access.
1. **Build the proto.** `r.as_proto(id)` extracts the read's current
   `RemainingRange`, meaning reconnects perfectly re-request only unreceived
   bytes (`active_read.rs:77`).
1. **Register in the HashMap.** Acquiring the `tokio::sync::Mutex`
   (`self.ranges.lock().await.insert(id, r)`) moves `r` into the routing table
   under its `id`, releasing the lock immediately at statement end.
1. **Collect the proto.**

After processing the loop, all protos are batched into a single
`BidiReadObjectRequest`. The `..BidiReadObjectRequest::default()` syntax fills
the remaining fields, critically setting `read_object_spec: None`. The spec is
only sent during the initial handshake or a full reconnect.

**Insert-Before-Send is the Design Hinge.** Crucially, the HashMap insert
(`:201`) happens *before* the `tx.send` (`:211`). If the send succeeds, the
routing is already prepped. If the send fails (because the wire-out is dead),
the entry safely *remains* in the HashMap. Branch A is on the verge of noticing
the dead connection and will trigger `reconnect` (`worker.rs:153`), which
systematically scoops up every range from the HashMap (`:157–163`) and
forcefully re-requests them in the new connection's opening message. Thus,
`tx.send` failures are intentionally logged and ignored; the robust reconnect
path catches them.

The governing pattern: **The HashMap is durable; the network wire is
disposable.** Reads secure a spot in the HashMap first, wire delivery is
inherently best-effort, and reconnect logic tirelessly re-sends anything the
network dropped. No read is ever lost.

### Branch A — Inbound (Wire-In → Routing / Reconnect)

Located at `worker.rs:71–87`:

```rust
m = rx.next_message() => {
    match self.handle_response(m).await {
        None => break None,
        Some(Err(e)) => break Some(e),
        Some(Ok(None)) => {},
        Some(Ok(Some(connection))) => {
            (rx, tx) = (connection.rx, connection.tx);
        }
    };
},
```

**`rx.next_message()`** is bound to the `TonicStreaming` trait
(`bidi.rs:38–48`), which exists primarily to facilitate mocking. In production,
it wraps Tonic's `Streaming::message()` in one line. One call strictly yields
one message, not a batch.

It returns `Result<Option<BidiReadObjectResponse>, tonic::Status>`, encoding
three states:

- `Ok(Some(response))` — a healthy data message.
- `Ok(None)` — a clean stream termination.
- `Err(status)` — a stream error (could be an error trailer or a broken socket,
  both synthesized into a `Status` by Tonic).

**`handle_response` and the `transpose()?` Trick** (`worker.rs:110–122`):

```rust
pub async fn handle_response(
    &mut self,
    message: TonicResult<Option<BidiReadObjectResponse>>,
) -> Option<LoopResult<Option<Connection<C::Stream>>>> {
    let response = match message.transpose()? {
        Ok(r) => r,
        Err(status) => return self.reconnect(status).await,
    };
    match self.handle_response_success(response).await {
        Err(e) => Some(Err(e)),
        Ok(_) => Some(Ok(None)),
    }
}
```

The elegant `message.transpose()` flips `Result<Option<T>, E>` into
`Option<Result<T, E>>`. The trailing `?` unwraps the outer `Option`. If it
encounters a `None` (representing an `Ok(None)` clean stream end), it
early-returns `None` out of `handle_response`, which the run loop perfectly
intercepts with its `None => break None` arm.

If it receives `Err(status)`, it immediately routes to
`self.reconnect(status).await`. If it gets `Ok(Some(r))`, it passes it down to
`self.handle_response_success(r).await`.

`handle_response_success` (`:124`) hands off to `handle_ranges`, which loops
over `handle_range_data` to reliably route bytes via the HashMap into the
correct byte channel. Routing failure yields `Some(Err(e))` (triggering a fatal
`break Some(e)` in the run loop), while success yields `Some(Ok(None))`
(instructing the run loop to continue using the same connection).

The `reconnect` method (`:153`) harvests every range from the HashMap and
executes `self.connector.reconnect(status, ranges)`. If the reconnect outright
fails, it yields a fatal `Some(Err(e))`. If it succeeds, it returns
`Some(Ok(Some(new_connection)))`, signaling the run loop to swap in the new wire
pair.

**The Four Match Arms in the Run Loop:**

| Arm                          | Meaning                                         | Action                            |
| ---------------------------- | ----------------------------------------------- | --------------------------------- |
| `None`                       | Clean termination of stream                     | `break None` — exit with success  |
| `Some(Err(e))`               | Fatal (corrupt data or total reconnect failure) | `break Some(e)` — exit with error |
| `Some(Ok(None))`             | Message routed successfully, connection healthy | fall through, continue loop       |
| `Some(Ok(Some(connection)))` | Reconnect succeeded, entirely new wire pair     | swap `rx`/`tx`, continue loop     |

**The Reconnect Swap:**

```rust
(rx, tx) = (connection.rx, connection.tx);
```

Using destructuring assignment, both wire halves are atomically replaced in a
single line. The durable HashMap (`self.ranges`) remains entirely unbothered by
the churn. On the very next iteration, `rx.next_message()` naturally reads from
the new stream, and `tx.clone()` uses the freshly minted sender.

### The Inbound Pipeline in Depth

When Branch A receives a successful message, it navigates a tight execution
chain:

```
rx.next_message()        →  handle_response(m)
                                 ↓ (Ok(Some(r)))
                            handle_response_success(r)
                                 ↓
                            handle_ranges(data)
                                 ↓ (for each chunk)
                            handle_range_data(ranges, chunk)
                                 ↓
                            ActiveRead::handle_data  →  Handler::send  (lock-free)
```

On an `Err(status)`, it detours to `reconnect`, which might invoke
`close_readers` if the reconnect fails entirely.

#### `handle_response_success` (worker.rs:124)

```rust
pub async fn handle_response_success(
    &mut self,
    response: BidiReadObjectResponse,
) -> LoopResult<()> {
    if let Err(e) = self.handle_ranges(response.object_data_ranges).await {
        // An error in the response. These are not recoverable.
        let error = Arc::new(e);
        self.close_readers(error.clone()).await;
        return Err(error);
    }
    Ok(())
}
```

This method receives a single response and routes its `object_data_ranges`. It
intentionally ignores `metadata` and `read_handle` here—those were purely for
the initial handshake. If the routing step fails, it wraps the error in an
`Arc`, fires `close_readers` to broadcast the failure to every registered
reader, and returns an `Err`. The comment "not recoverable" is vital: if the
server sends fundamentally corrupted *data*, a reconnect can't fix it
(reconnects only fix broken *wires*).

This is called in two places: inside `handle_response` for every standard data
message, and synchronously in `ObjectDescriptorTransport::new`
(`bidi/transport.rs:59`) for the handshake's first message, prior to the worker
being spawned.

#### `handle_ranges` (worker.rs:137)

```rust
async fn handle_ranges(&self, data: Vec<ObjectRangeData>) -> crate::Result<()> {
    let mut result = Ok(());
    for response in data {
        if let Err(e) = Self::handle_range_data(self.ranges.clone(), response).await {
            result = result.and(Err(e));     // lock onto the first error, but keep draining
        }
    }
    result.map_err(crate::Error::io)
}
```

This acts as the per-message dispatcher. It routes each `ObjectRangeData` piece
to `handle_range_data`. The clever `result.and(Err(e))` idiom elegantly captures
the **very first** error it encounters while ignoring subsequent ones, ensuring
the loop finishes draining without an abrupt early return (bad responses are
rare, so tracking multiple errors simply isn't worth the complexity overhead).
Finally, `map_err` converts the internal `ReadError` into the public
`crate::Error`.

#### `handle_range_data` (worker.rs:216)

```rust
async fn handle_range_data(
    ranges: Arc<Mutex<HashMap<i64, ActiveRead>>>,
    response: ObjectRangeData,
) -> ReadResult<()> {
    let range = response.read_range.ok_or(... "missing range")?;
    let handler = if response.range_end {
        let mut pending = ranges.lock().await.remove(&range.read_id).ok_or(...)?;
        pending.handle_data(response.checksummed_data, range, true)?
    } else {
        let mut guard = ranges.lock().await;
        let pending = guard.get_mut(&range.read_id).ok_or(...)?;
        pending.handle_data(response.checksummed_data, range, false)?
    };
    handler.send().await;
    Ok(())
}
```

This is an associated function (no `self`), taking the shared routing table and
a single chunk. It splits into two paths based on the `range_end: bool` flag:

| `range_end` Flag        | HashMap Operation      | Fate of Entry                          | Lock Scope                                          |
| ----------------------- | ---------------------- | -------------------------------------- | --------------------------------------------------- |
| `true` (final chunk)    | `lock().remove(...)`   | **Removed** entirely (owned)           | Drops at the semicolon, *before* `handle_data` runs |
| `false` (more expected) | `lock(); get_mut(...)` | **Stays** in HashMap (borrowed `&mut`) | Held exclusively through the body of `handle_data`  |

**The Lock-Scope Trick:** In the first path, the `remove` gives us ownership, so
the lock guard drops immediately at the semicolon, allowing `handle_data` to run
completely lock-free. In the second path, `pending` is borrowed directly from
the guard, meaning the lock *must* be held. Crucially, however, `handle_data` is
a plain `fn`, not an `async fn` (`active_read.rs:41`). The lock is only held
during rapid, synchronous memory operations (updating state, verifying CRC32C,
building the `Handler`). After the `if/else` block finishes, the guard is fully
released in both branches, allowing the async `handler.send().await` to execute
lock-free.

**The `Handler` Decoupling:** This design relies entirely on the `Handler` type
(`active_read.rs:83`). `handle_data` doesn't send bytes; it returns a `Handler`
representing *what needs to be sent*. This perfectly cleaves the process into a
locked "decision" phase and a lock-free "delivery" phase, guaranteeing that a
slow reader with a full channel can never stall the central routing table.

**Automatic Cleanup:** Path 1's `remove` keeps the HashMap incredibly lean.
Completed reads naturally vanish from the routing table the instant their final
chunk is processed. No separate "deregister" step is required.

#### `reconnect` (worker.rs:153) — Three Paths Out

```rust
async fn reconnect(&mut self, status: Status) -> Option<LoopResult<Option<Connection<C::Stream>>>> {
    let ranges = /* collects every active range from the HashMap */;
    let (response, _headers, connection) = match self.connector.reconnect(status, ranges).await {
        Err(e) => {
            let error = Arc::new(e);
            self.close_readers(error.clone()).await;
            return Some(Err(error));                  // Path 1: connector completely gave up
        }
        Ok(t) => t,
    };
    if let Err(e) = self.handle_ranges(response.object_data_ranges).await {
        let error = Arc::new(e);
        self.close_readers(error.clone()).await;
        return Some(Err(error));                      // Path 2: first new message was corrupted
    }
    Some(Ok(Some(connection)))                        // Path 3: flawless reconnect
}
```

There are three outcomes mapped to two return shapes:

| Path | Scenario                                    | Returns                      | Run Loop Arm                    |
| ---- | ------------------------------------------- | ---------------------------- | ------------------------------- |
| 1    | `connector.reconnect` exhausted retries     | `Some(Err(e))`               | `Some(Err(e)) => break Some(e)` |
| 2    | Reconnected, but first message was bad      | `Some(Err(e))`               | `Some(Err(e)) => break Some(e)` |
| 3    | Reconnected, first message routed perfectly | `Some(Ok(Some(connection)))` | Swaps `rx`/`tx` and continues   |

`Some(Ok(Some(connection)))` is **only** generated here. Thus, the run loop's
wire-swapping arm is strictly reserved for successful reconnects. Notably, both
failure paths invoke `close_readers` *before* returning, ensuring all readers
are proactively notified of the failure before the run loop unwinds.

#### `close_readers` (worker.rs:183) + `ActiveRead::interrupted`

```rust
async fn close_readers(&mut self, error: Arc<crate::Error>) {
    use futures::StreamExt;
    let mut guard = self.ranges.lock().await;
    let closing = futures::stream::FuturesUnordered::new();
    for (_, active) in guard.iter_mut() {
        closing.push(active.interrupted(error.clone()));
    }
    let _ = closing.count().await;
    guard.clear();
}
```

```rust
// active_read.rs:67
pub(super) async fn interrupted(&mut self, error: Arc<crate::Error>) {
    if let Err(e) = self
        .sender
        .send(Err(ReadError::UnrecoverableBidiReadInterrupt(error)))
        .await
    {
        tracing::error!("cannot notify reader (dropped?) about: {e:?}");
    }
}
```

The goal here is simple: push a terminal error to every registered reader, then
clear the routing table.

**The `FuturesUnordered` Pattern:** The call `active.interrupted(error.clone())`
just *returns* a future without running it. The loop pushes them all into a
`FuturesUnordered` collection, and `closing.count().await` drives them all
concurrently. This guarantees that one slow reader cannot block the termination
of the others, a huge improvement over a sequential `for ... { ... await }`
loop.

**Two-Step Termination per Reader:**

1. `interrupted` pushes an
   `Err(ReadError::UnrecoverableBidiReadInterrupt(Arc<Error>))` into the byte
   channel.
1. `guard.clear()` drops all `ActiveRead` objects, which inherently drops their
   `Sender`s, ultimately closing the channels.

The reader experiences: any buffered `Ok(Bytes)`, followed by the `Err`
(explaining *why*), and finally `None` (signaling *it's over*).

**Why `Arc<Error>`?** The error resides inside the channel message and lives on
wherever the reader stores it. A standard reference `&Error` would dangle, and
deep-cloning an error `N` times is a massive waste of resources. `Arc::clone`
provides an incredibly cheap atomic bump, giving us an owned, shared error
perfectly tailored for broadcasting.

### Wire-In Usage Map

| Location        | Purpose                                                              |
| --------------- | -------------------------------------------------------------------- |
| `bidi.rs:46`    | Core definition (`next_message` delegating to tonic's `message()`)   |
| `worker.rs:71`  | Branch A polling — `rx.next_message()` actively inside the `select!` |
| `worker.rs:84`  | The reconnect swap — `rx` is replaced by `connection.rx`             |
| `worker.rs:96`  | Deliberate `drop(rx)` immediately following loop exit                |
| `worker.rs:114` | Inside `handle_response`, safely unpacking via `transpose()?`        |

The flow is strictly unidirectional: data only travels *from* Tonic *into* the
worker. The worker has no visibility into Tonic's underlying HTTP/2 decoding
mechanisms.

## J. The Range-Type Stack

A stack of five specialized types collaborate to manage **one byte channel per
read range**. They elegantly divide the massive concern of *"I requested bytes,
I want them streamed, and reconnects cannot lose progress"* into precise,
single-responsibility layers.

### Cheat Sheet

| Type              | Layer                                              | Lifetime                             | Owner                                  |
| ----------------- | -------------------------------------------------- | ------------------------------------ | -------------------------------------- |
| `RequestedRange`  | "What the user asked for" (pre-resolution)         | Immutable after creation             | Buried in `RemainingRange::Requested`  |
| `NormalizedRange` | "What we are actively tracking" (post-first-chunk) | Mutates dynamically as bytes arrive  | Buried in `RemainingRange::Normalized` |
| `RemainingRange`  | State machine governing the two above              | One per `ActiveRead` (`state` field) | `ActiveRead.state`                     |
| `ActiveRead`      | State machine + the byte-channel **write end**     | One per range, living in the HashMap | Worker (`self.ranges`)                 |
| `RangeReader`     | Byte-channel **read end** + keepalive signal       | One per range                        | The user (inside `ReadObjectResponse`) |

Two ironclad invariants tie this system together:

1. **One range maps to one byte channel.** `ActiveRead` and `RangeReader` are
   born together (`bidi/transport.rs:82–85`), live apart, and die together.
1. **The state machine is strictly unidirectional:** `Requested` permanently
   pivots to `Normalized` upon receiving the first chunk.

### `RequestedRange` — The User's Wish

From `model_ext.rs:291–296`:

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum RequestedRange {
    Offset(u64),                          // "From byte N onward"
    Tail(u64),                            // "The final N bytes"
    Segment { offset: u64, limit: u64 },  // "N bytes beginning at offset M"
}
```

These are user-friendly units (positive `u64`), perfectly mapping to GCS's three
native read shapes.

**The `ReadRange` Newtype** (`model_ext.rs:171`):

```rust
pub struct ReadRange(pub(crate) RequestedRange);
```

The inner field is **crate-private**. This wrapper provides four critical
benefits:

1. **API Stability:** The internal enum can safely evolve without breaking
   downstream applications.
1. **Validated Factories:** Users must construct them correctly (e.g.,
   `ReadRange::offset(...)`, `::tail(...)`, `::head(...)`, `::segment(...)`,
   `::all()`).
1. **Obscured Proto Idioms:** Users ask for `ReadRange::tail(100)` without ever
   knowing that it transmits as `read_offset: -100` on the wire.
1. **Rich Documentation:** Each constructor function gets its own dedicated
   rustdoc and runnable example.

### `as_proto` — Wire Encoding and Routing

From `bidi/requested_range.rs:24–42`:

```rust
pub fn as_proto(&self, id: i64) -> ProtoRange {
    match self {
        Self::Offset(o)                 => ProtoRange { read_id: id, read_offset:  *o as i64, ..default() },
        Self::Tail(o)                   => ProtoRange { read_id: id, read_offset: -(*o as i64), ..default() },
        Self::Segment { offset, limit } => ProtoRange { read_id: id, read_offset: *offset as i64, read_length: *limit as i64 },
    }
}
```

It embeds two protocol quirks:

- **`Tail` maps to a negative offset**, matching the GCS protobuf convention for
  "from the end."
- **`read_length: 0` is the sentinel for "to EOF"**, utilized seamlessly by
  `Offset` and `Tail`.

**Why not implement `From<RequestedRange>`?** A standard `From::from` takes only
one argument, leaving no room to pass the **`id`** (the routing key generated at
`worker.rs:197–198`). `as_proto` cleverly handles serialization *and* routing
tagging simultaneously.

### The `read_id` Round Trip — Demystifying Multiplexing

The `id` is completely **opaque to the server**; it merely echoes back whatever
the SDK sends.

```
SDK side                              Wire                            GCS server
─────────────────────────────────────────────────────────────────────────────────
worker.rs:197
  id = self.next_range_id++
worker.rs:201
  ranges.insert(id, ActiveRead)       ← HashMap entry secured
worker.rs:200
  r.as_proto(id)                      ──▶ ProtoRange { read_id: 7, ... }
                                                                       Server processes read
                                      ◀── ObjectRangeData {
                                            read_range: { read_id: 7, ... },   ← Echoed back
                                            checksummed_data: { ... }
                                          }
worker.rs:216 handle_range_data
  ranges.lock().get_mut(&7)           ← Looks up by id → finds correct byte channel
```

This is the entire magic trick behind multiplexing multiple concurrent reads
over a single connection. Every chunk is tagged with its `read_id`, and the
worker's HashMap operates like a tiny switchboard routing the payload.

### `NormalizedRange` — The Server-Anchored Truth

From `normalized_range.rs:40–44`:

```rust
pub struct NormalizedRange {
    offset: i64,
    length: Option<i64>,
}
```

Once absolute coordinates are established, we drop the `Tail` concept entirely.
`offset` is strictly the absolute, non-negative byte offset. `length` is
`Some(n)` for exact remaining bytes, or `None` representing an unbounded read
("to EOF").

Using `i64` instead of `u64` aligns perfectly with the protobuf definition,
bypassing continuous bounds-checking. The use of `Option<i64>` elegantly
mitigates the footgun of relying on `0` as the "to EOF" sentinel, forcing
explicit type-level handling everywhere.

### `NormalizedRange::update` — The Heartbeat of Progress

Located at `normalized_range.rs:91`, this function executes **every single time
a chunk arrives**:

```rust
pub fn update(&mut self, response: ProtoRange) -> ReadResult<()> {
    let update = NormalizedRange::from_proto(response)?;            // 1. parse the incoming chunk's range
    if update.offset != self.offset {
        return Err(ReadError::bidi_out_of_order(...));              // 2. enforce in-order delivery
    }
    self.length = match (self.length, update.length) {              // 3. precisely subtract bytes
        (None, _)                                       => None,
        (Some(l), None)                                 => Some(l),
        (Some(expected), Some(got)) if got <= expected  => Some(expected - got),
        (Some(expected), Some(got))                     => return Err(LongRead { ... }),
    };
    self.offset = update.offset + update.length().unwrap_or_default(); // 4. aggressively advance cursor
    Ok(())
}
```

After `update` returns, `(offset, length)` describes **exactly the bytes still
pending** — `offset` is the next byte expected, `length` is how many remain.
That makes the state self-describing for a resume: calling `as_proto` at any
point emits the correct "resume from here" range with no extra bookkeeping.

Walk through a concrete case. Say you requested
`Segment { offset: 1000, limit: 500 }` on a 10,000-byte object, so the state
starts at `offset: 1000, length: Some(500)`. A chunk arrives carrying
`read_offset: 1000, read_length: 200`:

| Step | Operation                                                                 | State after                                  |
| ---- | ------------------------------------------------------------------------- | -------------------------------------------- |
| 1    | Parse the incoming range → `update = { offset: 1000, length: Some(200) }` | (no change to `self` yet)                    |
| 2    | Check `update.offset == self.offset` (1000 == 1000)                       | OK                                           |
| 3    | Subtract length: `Some(500) - Some(200)` → `Some(300)`                    | `self.length = Some(300)`                    |
| 4    | Advance cursor: `self.offset = 1000 + 200 = 1200`                         | `self = { offset: 1200, length: Some(300) }` |

```
byte:   0      1000   1200          1500              10000
        ├───────┼──────┼─────────────┼──────────────────┤
                xxxxxxx╞═════════════╡
                received  what's left to receive
```

Call `as_proto` at this moment and you get
`{read_offset: 1200, read_length: 300}` — precisely the re-request payload a
reconnect needs.

The length match (step 3) in plain terms:

| `self.length`                      | `update.length` | Result                 | Meaning                                                                 |
| ---------------------------------- | --------------- | ---------------------- | ----------------------------------------------------------------------- |
| `None`                             | anything        | `None`                 | An unbounded request stays unbounded.                                   |
| `Some(l)`                          | `None`          | `Some(l)`              | Chunk reported zero size — leave the expectation untouched (defensive). |
| `Some(expected)`, `got ≤ expected` | `Some(got)`     | `Some(expected - got)` | Normal case — subtract the delta.                                       |
| `Some(expected)`, `got > expected` | `Some(got)`     | `LongRead` error       | Server overran the limit — bail.                                        |

`unwrap_or_default()` in step 4 yields the inner value when `Some`, or
`i64::default() == 0` otherwise. So if `update.length` is `None`, the cursor
stays put (`update.offset + 0`, and step 2 already proved
`update.offset == self.offset`). A valid data chunk normally carries
`read_length > 0`; the `None` branch is just a safety net for cases
`active_read.rs:48–51` should already have routed through `handle_empty`.

### `RemainingRange` — The State Machine

From `remaining_range.rs:27–31`:

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RemainingRange {
    Requested(RequestedRange),     // Prior to the very first response
    Normalized(NormalizedRange),   // Following the first response
}
```

This enum handles the reality that a reconnect might strike at two wildly
different moments: before the server has sent a single chunk (requiring us to
re-send the original `Tail(100)` request), or mid-stream (requiring us to send
the precise normalized coordinates).

On the first chunk, the `update` method (`:34`) instantaneously pivots the enum
from `Requested` to `Normalized`, immediately applies the chunk's delta, and
commits the state. From that point on, everything flows into the simpler
`Normalized` pathway.

### `ActiveRead` — The Worker's Bundle

From `active_read.rs:24–28`:

```rust
#[derive(Debug)]
pub(crate) struct ActiveRead {
    state: RemainingRange,                       // ← The self-healing state machine
    sender: Sender<ReadResult<bytes::Bytes>>,    // ← The byte channel's write end
}
```

This represents the server-facing end of a single range, residing in the
worker's HashMap. Both fields **survive a reconnect untouched**.

### The `Handler` Decoupling

The `active_read.rs:82–101` block defines the `Handler` struct. This is the
crucial vehicle that ferries the decision across the lock boundary. It holds a
cloned `tx` and the payload `Bytes`. By returning this `Handler` from
`handle_data`, the worker can execute `.send().await` safely outside the lock,
protecting the global routing table from getting bottlenecked by a single slow
reader.

### `RangeReader` — The User's Faucet

From `range_reader.rs:26–33`:

```rust
#[derive(Debug)]
pub struct RangeReader {
    inner: Receiver<Result<bytes::Bytes, ReadError>>,
    object: Arc<Object>,
    // Unused, holding to a copy prevents the worker task from terminating
    // early.
    _tx: Sender<ActiveRead>,
}
```

**The `_tx` Keepalive Trick:** The `_tx` field is an intentional dummy
reference. By holding a clone of the read-request channel's sender, the reader
artificially props up the channel's "open" status. The worker uses the "all
senders dropped" condition of the read-request channel as its signal to
gracefully shut down (`worker.rs:88–93`). By keeping a clone here, we creatively
repurpose standard channel liveness semantics into a foolproof reference
counting mechanism.

The reader exposes `object()` for synchronous, I/O-free metadata access (thanks
to the `Arc<Object>`), and `next()` for async byte extraction, utilizing a
clever `?` to gracefully convert closed channels (`None`) directly into the
function's expected `None` return type.

## K. Redirects and the Resilience Machinery

Sometimes, the server decides to redirect a bidirectional read mid-stream. The
machinery that gracefully handles this lives across three small files and the
`Connector`. Let's map it out.

### The Protocol — What a Redirect Actually Looks Like

Google Cloud Storage doesn't use a dedicated "redirect" gRPC status code.
Instead, redirects masquerade as **errors** that carry a structured payload in
their details:

```
tonic::Status {
    code:    Code::Aborted,                    ← It looks like a permanent failure at first glance
    message: "...",
    details: <bytes that decode to google.rpc.Status>
        RpcStatus {
            details: [
                Any::<BidiReadObjectRedirectedError> {
                    routing_token: Option<String>,    ← The new value to use for x-goog-request-params
                    read_handle:   Option<...>,       ← A server-issued resume handle
                }
            ]
        }
}
```

This represents two layers of "status" (reminiscent of the two-metadatas
pattern):

- `tonic::Status`: The transport envelope containing the code, message, and raw
  bytes.
- `google.rpc.Status`: The rich protobuf error payload.
  `RpcStatus::decode(tonic_status.details())` cracks open the envelope.

**Why use `Aborted`?** A standard retry policy generally treats `Aborted` as a
permanent failure to avoid hammering a dead endpoint. By hijacking this
"permanent" code, GCS efficiently signals, "This specific stream is dead—but
here is exactly where you should connect next." A specialized decorator pattern
then intentionally overrides that "give up" verdict.

### `redirect.rs` — Recognize and React

This file houses two distinct functions:

**`handle_redirect(spec, status)`** (`redirect.rs:25`): This function extracts
the redirect information from the status and intentionally **mutates** the
shared `Arc<Mutex<BidiReadObjectSpec>>`. This guarantees that any subsequent
dial will carry the new routing token forward. It always returns the
GAX-converted error, regardless of whether a redirect was actually found (acting
as a safe no-op for standard errors).

**`is_redirect(&error)`** (`redirect.rs:40`): This is a pure predicate function
with zero side effects. It checks four sequential gates: the code must be
`Aborted`, it must wrap a `tonic::Status`, the details must cleanly decode as a
`google.rpc.Status`, and at least one detail must be a
`BidiReadObjectRedirectedError`.

**Why split this into two functions?** They serve different layers.
`handle_redirect` operates right on the wire boundary, updating state for the
*next* attempt. `is_redirect` operates deep inside policy decorators where the
error is already a `gax::Error`; it decides whether there *will be* a next
attempt by overriding the policy's verdict.

### Call Site Map

`handle_redirect` is unconditionally called in three places within
`connector.rs` during error paths where the raw `tonic::Status` is still
available:

- `connector.rs:213` (Early dial failure)
- `connector.rs:230` (First-message handshake failure)
- `connector.rs:120` (Mid-stream worker failure)

`is_redirect` is called purely inside policy decorators:

- `retry_redirect.rs:47` (`RetryRedirect::on_error`, used by the inner retry
  loop)
- `resume_redirect.rs:45` (`ResumeRedirect::on_error`, used directly by
  `connector.reconnect`)

### The Decorator Pattern

Both `RetryRedirect` and `ResumeRedirect` utilize the exact same shape:

```rust
fn on_error(&self, state: &..., error: Error) -> RetryResult {
    match self.inner.on_error(state, error) {
        RetryResult::Permanent(e) if is_redirect(&e) => RetryResult::Continue(e),
        result => result,
    }
}
```

This pattern **selectively overrides `Permanent` to `Continue` strictly for
redirect errors**. It gracefully respects `Exhausted` (if the attempt budget is
tapped out, redirects halt), and it respects genuine `Permanent` errors (like
auth failures).

Decorators are mandatory here because they inject logic directly *inside* the
external `retry_loop` (from the `gax` crate), which is only accessible through
the `RetryPolicy` trait.

## L. End-to-End Redirect Trace & The Durable-State Principle

To put it all together, let's trace a redirect mid-stream.

Imagine three concurrent reads are happily humming along. Here is the internal
state the instant before the redirect fires — one read from `send_and_read`, two
added via `read_range`, the worker parked in its `select!`:

```
Spec (Arc<Mutex<BidiReadObjectSpec>>)
    bucket:        "projects/_/buckets/my-bucket"
    object:        "my-object"
    generation:    789               ← resolved by the server at the first handshake
    read_handle:   Some(handle_v1)   ← server-issued at the first handshake
    routing_token: None              ← no redirect yet

Worker routing HashMap (self.ranges)
    { 0: ActiveRead { state: Normalized { offset: 9700, length: None      }, sender: <byte_tx_a> },
      1: ActiveRead { state: Normalized { offset: 2500, length: None      }, sender: <byte_tx_b> },
      2: ActiveRead { state: Normalized { offset: 5800, length: Some(200) }, sender: <byte_tx_c> } }

Worker
    connector.reconnect_attempts: 0
    rx: <Streaming v1>   tx: <Sender v1>      ← current wire halves
    parked in tokio::select! at worker.rs:70
```

Now, phase by phase:

1. **Interruption.** GCS closes the stream with
   `tonic::Status { code: Aborted, details: <BidiReadObjectRedirectedError { routing_token: Some("rt-v2"), read_handle: Some(handle_v2) }> }`.
   Tonic surfaces it as the next value from `rx.next_message()`.

1. **Detection.** Branch A (`worker.rs:71`) resolves to `m = Err(status)`. In
   `handle_response`, `message.transpose()?` takes the
   `Err(status) => return self.reconnect(status).await` arm
   (`worker.rs:114–117`).

1. **Snapshot progress.** `reconnect` maps every range to `r.as_proto(*id)`
   (`worker.rs:157–163`). Because `NormalizedRange::update` advanced each cursor
   as chunks arrived, the protos ask only for what is still outstanding:

   ```
   [ ProtoRange { read_id: 0, read_offset: 9700, read_length: 0   },   ← open tail
     ProtoRange { read_id: 1, read_offset: 2500, read_length: 0   },   ← open offset
     ProtoRange { read_id: 2, read_offset: 5800, read_length: 200 } ]  ← bounded segment
   ```

   No already-received byte is re-requested — the payoff of per-chunk cursor
   tracking.

1. **Mutate the spec.** `handle_redirect(self.spec.clone(), status)`
   (`connector.rs:120`) decodes the redirect detail and writes it through:

   ```
   routing_token: Some("rt-v2")     ← updated
   read_handle:   Some(handle_v2)   ← updated   (all other fields unchanged)
   ```

1. **Policy verdict: continue.** `self.reconnect_attempts += 1`, then
   `ResumeRedirect::on_error` runs (`connector.rs:121–128`). The inner policy
   sees `Aborted` and returns `Permanent`; the decorator calls `is_redirect`
   (true) and overrides it to `Continue`, so `reconnect` calls
   `self.connect(ranges)`.

1. **Re-dial with the updated spec.** The `connect` retry loop fires
   `connect_attempt` (`connector.rs:140`), which snapshots the now-updated spec
   into the opening message and builds the routing header from it:

   ```
   x-goog-request-params: bucket=projects/_/buckets/my-bucket&routing_token=rt-v2
   ```

   That header is what steers the GCS load balancer to the new backend. A fresh
   `(tx_v2, rx_v2)` pair is created, the opening message preloaded, and
   `client.start(...)` opens a new stream. The first message is read and the
   spec re-enriched — `generation` must still be `789`; `read_handle` may update
   again.

1. **First response data.** Back in `reconnect`, any range bytes the server
   packed into the handshake go through `handle_ranges` (`worker.rs:173`) just
   like a mid-stream message — looked up by `read_id` in the same HashMap,
   pushed down the same byte channels, cursors advanced. `reconnect` returns
   `Some(Ok(Some(connection_v2)))`.

1. **Atomic wire swap.** Branch A's matching arm runs
   `(rx, tx) = (connection.rx, connection.tx)` (`worker.rs:84`). The dead v1
   wires drop; the v2 wires take their place. The HashMap is untouched.

1. **Resume.** The loop re-enters `select!` on the new wires. Chunks arriving on
   `rx_v2` carry the same `read_id`s, route to the same `ActiveRead`s, and flow
   down the same user-facing channels. The user's parked `reader.next().await`
   wakes and yields the next chunk — no error, no panic, just a brief latency
   pause.

### The Durable / Disposable Split

The architecture works beautifully because of a rigid separation of concerns:

- **The Durable Layer (Survives Reconnects):** The Spec (holding identity and
  routing), the HashMap (holding state machines), and the user-facing
  read-request and byte channels.
- **The Disposable Layer (Discarded on Reconnect):** The wire-in stream, the
  wire-out sender, and the connection wrapper.

### The Core Architectural Principle

The server is entirely stateless; it retains no per-read progress state across
streams. The server acts as a dumb, incredibly fast pipe. **The entire
conversation state lives on the client.**

Because every dial is completely self-contained, any backend can serve any
portion of any read at any given time. Backends can vanish without warning, and
the client gracefully self-heals.

> **Takeaway:** The server is the dumb-but-fast pipe; the client is the
> smart-but-stateful brain. The SDK's worker, equipped with its
> `HashMap<i64, ActiveRead>`, is literally that brain in code form—a live,
> dynamic model tracking exactly "where we are in every conversation."

______________________________________________________________________

# Part 3 — After `send()` Returns

When the dust settles and `send()` returns, three entities remain alive and
active:

1. Your `ObjectDescriptor` (holding the `tx` to the worker).
1. The detached `Worker` task (owning the connection, the connector, and the
   read-id HashMap).
1. The enriched spec, living safely behind its `Arc<Mutex>`.

When you issue a new `read_range()` down the line, a new byte channel is minted
and packaged into an `ActiveRead`. It flows down the read-request channel, wakes
up Branch B of the worker loop, gets tagged with a fresh read-id, and is routed
to the server. The server responds, the worker intercepts the chunks, looks up
the read-id in the HashMap, and shoves the bytes down the exact right channel.

The worker task operates as the central hinge. It consumes `ActiveRead`s from
the read-request channel, pushes outbound messages to the wire, catches
returning responses, and routes them with extreme precision back to the user.

______________________________________________________________________

## Where to Go Next

- **`RangeReader` Internals (`bidi/range_reader.rs`):** Investigate how
  `reader.next()` polls the byte channel and relies on the read-request clone.
- **Resume/Redirect Policy (`bidi/resume_redirect.rs`, `bidi/redirect.rs`):**
  Dive deeper into how `reconnect` navigates retry logic versus giving up
  entirely.
- **`read_object`:** Contrast this incredibly sophisticated streaming path with
  the simpler, unary read implementation.
