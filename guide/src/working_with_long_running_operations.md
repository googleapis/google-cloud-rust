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

The guide uses the [Speech-To-Text V2] service to keep the code snippets
concrete. The same ideas work for any other service using LROs.

We recommend that you first follow one of the service guides, such as
[Transcribe speech to text by using the command line]. These guides cover
critical topics such as ensuring your project has the API enabled, your account
has the right permissions, and how to set up billing for your project (if
needed). Skipping the service guides may result in problems that are hard to
diagnose.

For complete setup instructions for the Rust libraries, see
[Setting up your development environment].

## Dependencies

Declare Google Cloud dependencies in your `Cargo.toml` file:

```shell
cargo add google-cloud-speech-v2 google-cloud-lro google-cloud-longrunning
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
{{#include ../samples/src/lro.rs:use}}
```

Now create the client:

```rust,ignore
{{#include ../samples/src/lro.rs:client}}
```

You'll use [batch recognize] for this example. While this is designed for long
audio files, it works well with small files too.

In the Rust client libraries, each request is represented by a method that
returns a request builder. First, call the right method on the client to create
the request builder. You'll use the default [recognizer] (`_`) in the `global`
region.

```rust,ignore
{{#include ../samples/src/lro.rs:request-builder}}
```

Then initialize the request to use a publicly available audio file:

```rust,ignore
{{#include ../samples/src/lro.rs:audio-file}}
```

Configure the request to return the transcripts inline:

```rust,ignore
{{#include ../samples/src/lro.rs:transcript-output}}
```

Then configure the service to transcribe to US English, using the [short model]
and some other default configuration:

```rust,ignore
{{#include ../samples/src/lro.rs:configuration}}
```

Make the request and wait for an [Operation][longrunning::model::operation] to
be returned. This `Operation` acts as a promise for the result of the
long-running request:

```rust,ignore
{{#include ../samples/src/lro.rs:send}}
```

Finally, poll the promise until it completes:

```rust,ignore
{{#include ../samples/src/lro.rs:call-manual}}
```

You'll examine the `manually_poll_lro()` function in the
[Manually polling a long-running operation] section.

You can find the
[full function](#automatically-polling-a-long-running-operation-complete-code)
below.

## Automatically polling a long-running operation

To configure automatic polling, you prepare the request just like you did to
start a long-running operation. The difference comes at the end, where instead
of sending the request to get the `Operation` promise:

```rust,ignore
{{#include ../samples/src/lro.rs:send}}
```

... you create a `Poller` and wait until it is done:

```rust,ignore
{{#include ../samples/src/lro.rs:automatic-poller-until-done}}
```

Let's review the code step-by-step.

First, introduce the trait in scope via a `use` declaration:

```rust,ignore
{{#include ../samples/src/lro.rs:automatic-use}}
```

Then initialize the client and prepare the request as before:

```rust,ignore
{{#include ../samples/src/lro.rs:automatic-prepare}}
```

And then poll until the operation is completed and print the result:

```rust,ignore
{{#include ../samples/src/lro.rs:automatic-print}}
```

You can find the
[full function](#automatically-polling-a-long-running-operation-complete-code)
below.

## Polling a long-running operation

While `.until_done()` is convenient, it omits some information: long-running
operations may report partial progress via a "metadata" attribute. If your
application requires such information, you need to use the poller directly:

```rust,ignore
    let mut poller = client
        .batch_recognize(/* stuff */)
        /* more stuff */
        .poller();
```

Then use the poller in a loop:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:polling-loop}}
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
        .batch_recognize(/* stuff */)
        /* more stuff */
        .send()
        .await?;
```

You are going to start a loop to poll the `operation`, and you need to check if
the operation completed immediately (this is rare but does happen). The `done`
field indicates if the operation completed:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-if-done}}
```

In most cases, if the operation is done it contains a result. However, the field
is optional because the service could return `done` as true and no result: maybe
the operation deletes resources and a successful completion has no return value.
In this example using the Speech-to-Text service, you can treat this as an
error:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-match-none}}
```

Starting a long-running operation successfully does not guarantee that it will
complete successfully. The result may be an error or a valid response. You need
to check for both. First check for errors:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-match-error}}
```

The error type is a [Status][rpc::model::status] message type. This does **not**
implement the standard `Error` interface. You need to manually convert it to a
valid error. You can use [Error::service] to perform this conversion.

Assuming the result is successful, you need to extract the response type. You
can find this type in the documentation for the LRO method, or by reading the
service API documentation:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-match-success}}
```

Note that extraction of the value may fail if the type does not match what the
service sent.

All types in Google Cloud may add fields and branches in the future. While this
is unlikely for a common type such as `Operation`, it happens frequently for
most service messages. The Google Cloud Client Libraries for Rust mark all
structs and enums as `#[non_exhaustive]` to signal that such changes are
possible. In this case, you must handle this unexpected case:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-match-default}}
```

If the operation has not completed, then it may contain some metadata. Some
services just include initial information about the request, while other
services include partial progress reports. You can choose to extract and report
this metadata:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-metadata}}
```

As the operation has not completed, you need to wait before polling again.
Consider adjusting the polling period, maybe using a form of truncated
[exponential backoff]. This example simply polls every 500ms:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-backoff}}
```

Then you can poll the operation to get its new status:

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual-poll-again}}
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
{{#include ../samples/src/lro.rs:start}}
```

## Automatically polling a long-running operation: complete code

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:automatic}}
```

## Polling a long-running operation: complete code

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:polling}}
```

## Manually polling a long-running operation: complete code

```rust,ignore
{{#rustdoc_include ../samples/src/lro.rs:manual}}
```

[batch recognize]: https://cloud.google.com/speech-to-text/v2/docs/batch-recognize
[configuring polling policies]: ./configuring_polling_policies.md
[error::service]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/error/struct.Error.html
[exponential backoff]: https://en.wikipedia.org/wiki/Exponential_backoff
[longrunning::model::operation]: https://docs.rs/google-cloud-longrunning/latest/google_cloud_longrunning/model/struct.Operation.html
[manually polling a long-running operation]: #manually-polling-a-long-running-operation
[recognizer]: https://cloud.google.com/speech-to-text/v2/docs/recognizers
[rpc::model::status]: https://docs.rs/google-cloud-rpc/latest/google_cloud_rpc/model/struct.Status.html
[setting up your development environment]: setting_up_your_development_environment.md
[short model]: https://cloud.google.com/speech-to-text/v2/docs/transcription-model
[speech-to-text v2]: https://cloud.google.com/speech-to-text/v2
[transcribe speech to text by using the command line]: https://cloud.google.com/speech-to-text/v2/docs/transcribe-api
