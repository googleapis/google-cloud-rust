<!-- Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Using Google Cloud Pub/Sub

**WARNING:** this crate is under active development. We expect multiple breaking
changes in the upcoming releases. Testing is also incomplete, we do **not**
recommend that you use this crate in production. We welcome feedback about the
APIs, documentation, missing features, bugs, etc.

Google [Cloud Pub/Sub] is an asynchronous and scalable messaging service.

This guide shows you how to get started publishing messages with the
`google-cloud-pubsub` crate.

Receiving messages is not yet supported by this crate.

## Quickstart

This guide will show you how to create a Pub/Sub topic and publish messages to
this topic.

### Prerequisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled].

### Add Dependencies

Add the client library as a dependency as well as other libraries needed for
this guide:

```sh
cargo add google-cloud-pubsub
cargo add tokio -F full
cargo add anyhow
```

## Creating a topic

The client to perform operations on topics is called `TopicAdmin`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:topicadmin}}
```

We will create a topic for `project_id` and `topic_id`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:createtopic}}
```

## Publishing a Message

`google-cloud-pubsub` exposes a high-level client that can batch messages
together for a given topic. To create these batched `Publisher`s, first create a
`PublisherFactory`, which configures and manages low-level gRPC client and use
it to create a `Publisher`.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:publisher}}
```

We can then publish messages to the topic. These messages are batched in a
background worker before being sent to the service.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:publish}}
```

To get the results of the publish, `.await` on the `PublishHandle` that is
returned from `publish()`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:publishresults}}
```

### Cleanup

Finally we remove the topic to cleanup all the resources used in this guide:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:cleanup}}
```

## Full program

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/pubsub/quickstart.rs:quickstart}}
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud pub/sub]: https://cloud.google.com/pubsub
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
