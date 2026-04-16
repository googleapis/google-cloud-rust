# Pub/Sub Architecture Guide

This document describes the high-level architecture of the Cloud Pub/Sub client
library in `google-cloud-rust`. Its main audience are developers and
contributors making changes and additions to this specific library.

## Overview

The `pubsub` crate provides clients for interacting with Google Cloud Pub/Sub.
Unlike many other libraries in this repository which are purely generated, the
Pub/Sub library features hand-crafted layers to handle client-side logic such
as:

- High-performance asynchronous batching for publishing.
- Starting and resuming a message stream.
- Managing leases for received messages.
- Graceful subscriber shutdown.
- Support for at-least-once and exactly-once delivery semantics.

## Hand-Crafted vs Generated Code

The library contains both generated and hand-crafted code.

- **Fully Generated Clients**: There are 3 fully generated clients in the crate
  (SchemaService, TopicAdmin, and SubscriptionAdmin). While the service definitions
  in protobuf combine administrative and data-plane operations on the same gRPC
  service, we split them and do some renaming in the librarian config. Thus,
  data-plane operations are not generated for these administrative clients.
- **Hand-Crafted Clients**: The `Publisher` and `Subscriber` are hand-crafted to
  provide features like batching and lease management.
- **Private Dependencies**: The hand-crafted `Publisher` and `Subscriber` depend
  on **private** generated GAPIC clients to perform the actual gRPC calls. The
  fully generated administrative clients are located in `src/generated/gapic`,
  while the data-plane specific clients used by the hand-crafted layers are
  in `src/generated/gapic_dataplane`.

## Core Concepts

### Asynchronous and Non-Blocking

All operations are asynchronous and integrate with the Tokio runtime. The
library avoids blocking calls and relies on background tasks (spawns) for
operations like batch flushing and lease extending.

### Task Communication via Channels

The clients use channels to handle communication between tasks.

- **Message Passing**: The `Publisher` uses an MPSC (Multi-Producer,
  Single-Consumer) channel to send messages to the background worker.
- **Ack/Nack Coordination**: The `Subscriber` uses channels to coordinate Ack
  and Nack operations between the user-facing handles and the background lease
  management loop.
- **One-shot Notifications**: We use oneshot channels to communicate the result
  of publishing a message or performing a confirmed acknowledgement (e.g., notifying
  a `PublishFuture` that a message has been sent).
- **Cancellation**: The `Subscriber` also uses a `CancellationToken` to signal
  between tasks:
  - To initiate shutdown when the application cancels a stream.
  - To clean up the keepalive task when a gRPC stream fails with a transient error.

## Publisher Architecture

The publisher buffers messages and sends them in batches.

### The Actor Pattern

Publishing does not immediately trigger an RPC. Instead, the `Publisher` uses an
actor-like pattern:

- **Frontend**: The `Publisher` struct (in `src/publisher/client.rs`) provides
  the public API. Calls to `publish()` send messages to a background task via a
  channel.
- **Background Worker**: An actor/worker task (in `src/publisher/actor.rs`)
  receives these messages, buffers them, and decides when to flush them based on
  size or time constraints. To support message ordering, the library spawns a
  separate `BatchActor` for each ordering key (plus a default one for unordered
  messages).
- **Futures-Based Completion**: The `publish()` method returns a `PublishFuture`
  (in `src/publisher/future.rs`). When the background worker successfully
  completes the batch RPC, it notifies the handle with the server-assigned
  message ID.
- **Lifecycle**: The background worker task is spawned immediately when the
  `Publisher` is created (via `Publisher::builder().build()`).

## Subscriber Architecture

The subscriber manages continuous stream connections and lease extensions.

### Streaming Pull

The subscriber maintains a bidirectional streaming pull connection with the
Pub/Sub service. This stream yields messages as they become available.

#### The gRPC Stream

The `Stream` (in `src/subscriber/stream.rs`) wraps the raw gRPC streaming pull RPC. It handles retries and backoffs for attempts to open the stream. It also spawns a keepalive task to send heartbeats to the server to keep idle streams alive.

#### The Message Stream

The `MessageStream` (in `src/subscriber/message_stream.rs`) is a stream-like interface. Applications ask it for messages.

Under the surface, it handles:

- Opening new streams (either initially, or after a transient failure).
- Pulling messages from the stream and...
  - Forwarding the messages to the lease management task.
  - Storing the messages in a pool to give to the application.
- Shutting down gracefully if signaled by the application.

### Lease Management

Pub/Sub requires messages to be acknowledged within a certain deadline. To
prevent messages from expiring while the application is still processing them,
the library implements automatic lease management:

- **`LeaseState`** (in `src/subscriber/lease_state.rs`): Tracks all messages
  currently under lease by the client.
- **`LeaseLoop`** (in `src/subscriber/lease_loop.rs`): A background lease event
  loop that:
  - Forwards messages from the stream into lease management.
  - Periodically sends `modifyAckDeadline` requests to the server to extend the
    lease of all active messages.
  - Processes actions from the application (acks/nacks) and periodically flushes them.
  - Processes the results of confirmed acks.

### Exactly-Once Delivery

When Exactly-Once Delivery is enabled on a subscription, the client must ensure
that Ack and Nack operations are confirmed by the server before informing the
user.

- **State Transitions**: When an application calls `ack()` or `nack()`, the
  message state is updated in `LeaseState` but the message is not immediately
  removed since it holds the send channel for the result future.
- **RPC Confirmation**: The client performs the Ack/Nack RPC and awaits
  confirmation. Once confirmed, the message is removed from `LeaseState` and the
  user's future completes.
- **Lease Extension During Ack**: For Exactly-Once Delivery, messages with a
  pending Ack continue to have their leases extended. This prevents the lease
  from expiring while the confirmed Ack RPC is retrying. Messages that are
  currently being nacked are not extended since this was the desired action from
  the user.

## Advanced Features

### Message Ordering

When ordered delivery is enabled, the library ensures that messages with the
same ordering key are delivered in the order they were published.

- **Publisher**: The actor ensures that messages for a specific key are batched
  and sent sequentially if needed, or at least that failures handle ordering
  constraints (e.g., pausing on error).
- **Subscriber**: The stream preserves the order of messages yielded by the
  server.

## Where is the code?

- [`src/publisher/`](src/publisher/): Contains the publisher implementation.
  - [`client.rs`](src/publisher/client.rs): The high-level `Publisher`.
  - [`actor.rs`](src/publisher/actor.rs): The background batching worker.
  - [`batch.rs`](src/publisher/batch.rs): Batching logic.
- [`src/subscriber/`](src/subscriber/): Contains the subscriber implementation.
  - [`client.rs`](src/subscriber/client.rs): The high-level `Subscriber`.
  - [`message_stream.rs`](src/subscriber/message_stream.rs): The core streaming
    pull loop.
  - [`lease_state.rs`](src/subscriber/lease_state.rs): State tracking for active
    leases.
  - [`lease_loop.rs`](src/subscriber/lease_loop.rs): The background lease event
    loop.
  - [`handler.rs`](src/subscriber/handler.rs): APIs that let the application
    ack/nack messages. They forward actions (acks/nacks) from the application
    to the lease loop. They are opaque wrappers over a message's ack ID.
  - [`transport.rs`](src/subscriber/transport.rs): An extension of the generated
    gRPC stub to handle bidi-streaming RPC.
  - [`stub.rs`](src/subscriber/stub.rs): An abstraction of the `service Subscriber`
    (for testing purposes).
  - [`leaser.rs`](src/subscriber/leaser.rs): A thing that knows how to perform
    lease actions. This is abstracted for testing purposes. The default
    implementation is a thin wrapper over a transport stub.
  - [`stream.rs`](src/subscriber/stream.rs): A wrapper over the gRPC stream that
    adds retries and keepalives.
