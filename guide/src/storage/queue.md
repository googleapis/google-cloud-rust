<!-- 
Copyright 2025 Google LLC

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

# Push data on object uploads

The client API to upload [Cloud Storage] objects pulls the upload data from a
type provided by the application. Some applications generate the upload data in
a thread and would rather "push" the object payload to the service.

This guide will show you how to upload an object to [Cloud Storage] using a push
data source.

## Prerequisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled], and a Cloud Storage bucket in that project.

## Add the client library as a dependency

```toml
{{#include ../../samples/Cargo.toml:storage}}
```

## Convert a queue to an upload source

The key idea is to use a queue to separate the task pushing new data from the
task pulling the upload data. This tutorial uses a Tokio [mpsc queue], but you
can use any queue that integrates with Tokio's async runtime.

First wrap the receiver in our own type:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:wrapper-struct}}
```

Then implement the trait required by the Google Cloud client library:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:impl-streaming-source}}
```

In this tutorial you write the rest of the code in a function that accepts the
bucket and object name as parameters:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:begin-sample-function}}
    // ... code goes here ...
{{#rustdoc_include ../../samples/tests/storage/queue.rs:end-sample-function}}
```

As usual you initialize a client for the upload:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:client}}
```

Create a queue, obtaining the receiver and sender:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:create-queue}}
```

Use the client to upload the data received from this queue. Note that we do not
`await` the future created in the `upload_object()` method.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:create-upload}}
```

Create a task to process the queue and upload the data in the background:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:create-task}}
```

In the main task, send some data to upload:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:send-data}}
```

Once you have finished sending the data, drop the sender to close the sending
side of the queue:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:close}}
```

Now you can wait for the task to finish and extract the result:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:wait}}
```

## Full program

Putting all these steps together you get:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/queue.rs:all}}
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud storage]: https://cloud.google.com/storage
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[mpsc queue]: https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html
