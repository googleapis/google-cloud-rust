# Pub/Sub Architecture Guide

This document describes the high-level architecture of the Cloud Pub/Sub client
library in `google-cloud-rust`. Its main audience are developers and
contributors making changes and additions to this specific library.

## Overview

The `pubsub` crate provides a client for interacting with Google Cloud Pub/Sub.
Unlike many other libraries in this repository which are purely generated, the
Pub/Sub library features hand-crafted layers to handle client-side logic such
as:

- High-performance asynchronous batching for publishing.
- Automatic lease management for received messages.
- Support for Exactly-Once Delivery semantics.

## Hand-Crafted vs Generated Code

The library contains both generated and hand-crafted code.

- **Fully Generated Clients**: There are 3 fully generated clients in the crate
  (SchemaService, TopicAdmin, and SubscriptionAdmin). The data-plane operations
  are not generated for these clients.
- **Hand-Crafted Clients**: The `Publisher` and `Subscriber` are hand-crafted to
  provide features like batching and lease management.
- **Private Dependencies**: The hand-crafted `Publisher` and `Subscriber` depend
  on **private** generated GAPIC clients to perform the actual gRPC calls. These
  clients only include the RPCs necessary for data-plane operations.

## Core Concepts

### Asynchronous and Non-Blocking

All operations are asynchronous and integrate with the Tokio runtime. The
library avoids blocking calls and relies on background tasks (spawns) for
operations like batch flushing and lease extending.

### Task Communication via Channels

Communication between the public API (frontend) and background tasks, as well as
between different background loops, is handled via channels.

- **Message Passing**: The `Publisher` uses an MPSC (Multi-Producer,
  Single-Consumer) channel to send messages to the background worker.
- **Ack/Nack Coordination**: The `Subscriber` uses channels to coordinate Ack
  and Nack operations between the user-facing handles and the background lease
  management loop.
- **One-shot Notifications**: Completion signals (like notifying a
  `PublishFuture` that a message has been sent) use oneshot channels.

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

#### The Message Stream

The `MessageStream` (in `src/subscriber/message_stream.rs`) wraps the raw gRPC
streaming pull response and provides a `Stream` of messages. It handles:

- Establishing and maintaining the bidirectional stream.
- Pushing incoming messages to the user.
- Interacting with `LeaseState` to ensure all received messages are tracked
  immediately.
- **Lifecycle**: The `LeaseLoop` task is spawned when the `MessageStream` is
  constructed. The underlying gRPC stream is lazily initialized when the
  application first requests a message. Once opened, a background keepalive task
  is also spawned to prevent the stream from timing out.

### Lease Management

Pub/Sub requires messages to be acknowledged within a certain deadline. To
prevent messages from expiring while the application is still processing them,
the library implements automatic lease management:

- **`LeaseState`** (in `src/subscriber/lease_state.rs`): Tracks all messages
  currently outstanding (leased) by the client.
- **`LeaseLoop`** (in `src/subscriber/lease_loop.rs`): A background loop that
  periodically sends `modifyAckDeadline` requests to the server to extend the
  lease of all active messages.

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
  - [`lease_loop.rs`](src/subscriber/lease_loop.rs): The background lease
    extension loop.
  - [`handler.rs`](src/subscriber/handler.rs): Implementation of Ack/Nack
    handlers.
