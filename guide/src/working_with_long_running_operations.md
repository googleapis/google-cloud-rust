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

# Working with long-running operations

Occasionally, an API may need to expose a method that takes a significant amount
of time to complete. In these situations, it's often a poor user experience to
simply block while the task runs. It's usually better to return some kind of
promise to the user and allow the user to check back later.

The Google Cloud Client Libraries for Rust provide helpers to work with these
long-running operations (LROs). This guide shows you how to start LROs and wait
for their completion.

## Prerequisites

The guide uses the [Cloud Storage] service to keep the code snippets concrete.
The same ideas work for any other service using LROs.

The guide assumes you have an existing [Google Cloud project] with
[billing enabled].

For complete setup instructions for the Rust libraries, see
[Setting up your development environment].

## Dependencies

Declare Google Cloud dependencies in your `Cargo.toml` file:

```shell
cargo add google-cloud-storage google-cloud-lro google-cloud-longrunning
```

You'll also need several `tokio` features:

```shell
cargo add tokio --features full,macros
```

## Starting a long-running operation

To start a long-running operation, you'll
[initialize a client](./initialize_a_client.md) and then make the RPC. But
first, add some use declarations to avoid the long package names:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:use}}
```

Now create the client:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:client}}
```

You'll use [rename folder] for this example. This operation may take a long time
when using large folders, but it is relatively fast with smaller folders.

In the Rust client libraries, each request is represented by a method that
returns a request builder. First, call the right method on the client to create
the request builder:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:request-builder}}
```

The sample functions accept the bucket and folder names as arguments:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:manual-arguments}}
```

Make the request and wait for an [Operation][longrunning::model::operation] to
be returned. This `Operation` acts as a promise for the result of the
long-running request:

```rust,ignore
    let operation =
        // ... ...
{{#include ../samples/tests/storage/lros.rs:send}}
```

While this request has started the operation in the background, you should wait
until the operation completes to determine if it was successful or failed.
Continue reading to learn how you use the client library polling loops, or how
to write your own.

You can find the
[full function](#automatically-polling-a-long-running-operation-complete-code)
below.

## Automatically polling a long-running operation

To configure automatic polling, prepare the request as you did to start a
long-running operation. The difference comes at the end, where instead of
sending the request using `.send().await` you create a `Poller` and wait until
it is done:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:automatic-poller-until-done}}
```

Let's review the code step-by-step. First, introduce the `Poller` trait in scope
via a `use` declaration:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:automatic-use}}
```

Then initialize the client and prepare the request as before:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:automatic-prepare}}
```

And then poll until the operation is completed and print the result:

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:automatic-print}}
```

You can find the
[full function](#automatically-polling-a-long-running-operation-complete-code)
below.

## Polling a long-running operation with intermediate results

While `.until_done()` is convenient, it omits some information: long-running
operations may report partial progress via a "metadata" attribute. If your
application requires such information, use the poller directly:

```rust,ignore
    let mut poller = client
        .rename_object()
        /* more stuff */
        .poller();
```

Then use the poller in a loop:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:polling-loop}}
```

Note how this loop explicitly waits before polling again. The polling period
depends on the specific operation and its payload. You should consult the
service documentation and/or experiment with your own data to determine a good
value.

The poller uses a policy to determine what polling errors are transient and may
resolve themselves. The [Configuring polling policies] chapter covers this topic
in detail.

You can find the
[full function](#polling-a-long-running-operation-complete-code) below.

## Manually polling a long-running operation

In general, we recommend that you use the previous two approaches in your
application. Alternatively, you can manually poll a long-running operation, but
this can be quite tedious, and it is easy to get the types wrong. If you do need
to manually poll a long-running operation, this section walks you through the
required steps. You may want to read the
[Operation][longrunning::model::operation] message reference documentation, as
some of the fields and types are used below.

Recall that you started the long-running operation using the client:

```rust,ignore
    let mut operation = client
        .rename_folder()
        /* more stuff */
        .send()
        .await?;
```

Start a loop to poll the `operation`, and check if the operation completed using
the `done` field:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-if-done}}
```

In most cases, when the operation is completed it contains a result. However,
the field is optional because the service could return `done` as true and no
result. For example, if the operation deletes resources and a successful
completion has no return value. In this example, using the Storage service, you
can ignore this nuance and assume a value will be present:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-match-none}}
```

Starting a long-running operation successfully does not guarantee that it will
complete successfully. The result may be an error or a valid response. You need
to check for both. First check for errors:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-match-error}}
```

The error type is a [Status][rpc::model::status] message type. This does **not**
implement the standard `Error` interface. You need to manually convert it to a
valid error. You can use [Error::service] to perform this conversion.

Assuming the result is successful, you need to extract the response type. You
can find this type in the documentation for the LRO method, or by reading the
service API documentation:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-match-success}}
```

Note that extraction of the value may fail if the type does not match what the
service sent.

All types in Google Cloud may add fields and branches in the future. While this
is unlikely for a common type such as `Operation`, it happens frequently for
most service messages. The Google Cloud Client Libraries for Rust mark all
structs and enums as `#[non_exhaustive]` to signal that such changes are
possible. In this case, you must handle this unexpected case:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-match-default}}
```

If the operation has not completed, then it may contain some metadata. Some
services just include initial information about the request, while other
services include partial progress reports. You can choose to extract and report
this metadata:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-metadata}}
```

As the operation has not completed, you need to wait before polling again.
Consider adjusting the polling period, maybe using a form of truncated
[exponential backoff]. This example simply polls every 500ms:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-backoff}}
```

If the operation has not completed, you need to query its status:

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual-poll-again}}
```

For simplicity, the example ignores all errors. In your application you may
choose to treat only a subset of the errors as non-recoverable, and may want to
limit the number of polling attempts if these fail.

You can find the
[full function](#manually-polling-a-long-running-operation-complete-code) below.

## What's next

- To learn about customizing error handling and backoff periods for LROs, see
  [Configuring polling policies](/configuring_polling_policies.md).
- To learn how to simulate LROs in your unit tests, see
  [How to write tests for long-running operations](/mocking_lros.md).

## Starting a long-running operation: complete code

```rust,ignore
{{#include ../samples/tests/storage/lros.rs:start}}
```

## Automatically polling a long-running operation: complete code

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:automatic}}
```

## Polling a long-running operation: complete code

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:polling}}
```

## Manually polling a long-running operation: complete code

```rust,ignore
{{#rustdoc_include ../samples/tests/storage/lros.rs:manual}}
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud storage]: https://cloud.google.com/storage
[configuring polling policies]: ./configuring_polling_policies.md
[error::service]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/error/struct.Error.html
[exponential backoff]: https://en.wikipedia.org/wiki/Exponential_backoff
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[longrunning::model::operation]: https://docs.rs/google-cloud-longrunning/latest/google_cloud_longrunning/model/struct.Operation.html
[rename folder]: https://cloud.google.com/storage/docs/rename-hns-folders
[rpc::model::status]: https://docs.rs/google-cloud-rpc/latest/google_cloud_rpc/model/struct.Status.html
[setting up your development environment]: setting_up_your_development_environment.md
