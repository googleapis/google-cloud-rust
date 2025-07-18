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

# How to write tests for long-running operations

The Google Cloud client libraries for Rust have helpers that simplify
interaction with long-running operations (henceforth, LROs).

Simulating the behavior of LROs in tests involves understanding the details
these helpers hide. This guide shows how to do that.

## Prerequisites

This guide assumes you are familiar with the previous chapters:

- [Working with long-running operations](working_with_long_running_operations.md)
- [How to write tests using a client](mock_a_client.md)

## Tests for automatic polling

Let's say our application code awaits `lro::Poller::until_done()`. In previous
sections, we called this "automatic polling".

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:app-fn}}
```

Note that our application only cares about the final result of the LRO. We do
not need to test how it handles intermediate results from polling the LRO. Our
tests can simply return the final result of the LRO from the mock.

### Creating the `longrunning::model::Operation`

Let's say we want our call to result in the following response.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:expected-response}}
```

You may have noticed that the stub returns a `longrunning::model::Operation`,
not a `BatchRecognizeResponse`. We need to pack our desired response into the
`Operation::result`.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:finished-op}}
```

Note also that we set the `done` field to `true`. This indicates to the `Poller`
that the operation has completed, thus ending the polling loop.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:set-done-true}}
```

### Test code

Now we are ready to write our test.

First we define our mock class, which implements the
[`speech::stub::Speech`][speech-stub] trait.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:mockall-macro}}
```

Now in our test we create our mock, and set expectations on it.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:mock-expectations}}
```

Finally, we create a client from the mock, call our function, and verify the
response.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:client-call}}
```

## Tests for manual polling with intermediate metadata

Let's say our application code manually polls, and does some processing on
partial updates.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_manual.rs:app-fn}}
```

We want to simulate how our application acts when it receives intermediate
metadata. We can achieve this by returning in-progress operations from our mock.

### Creating the `longrunning::model::Operation`

The `BatchRecognize` RPC returns partial results in the form of a
`speech::model::OperationMetadata`. Like before, we will need to pack this into
the returned `longrunning::model::Operation`, but this time into the
`Operation::metadata` field.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_manual.rs:partial-op}}
```

### Test code

First we define our mock class, which implements the
[`speech::stub::Speech`][speech-stub] trait. Note that we override
`get_operation()`. We will see why shortly.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_manual.rs:mockall-macro}}
```

Now in our test we create our mock, and set expectations on it.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_manual.rs:mock-expectations}}
```

These expectations will return partial results (25%, 50%, 75%), then return our
desired final outcome.

Now a few things you probably noticed.

1. The first expectation is set on `batch_recognize()`, whereas all subsequent
   expectations are set on `get_operation()`.

   The initial `BatchRecognize` RPC starts the LRO on the server-side. The
   server returns some identifier for the LRO. This is the `name` field which is
   omitted from the test code, for simplicity.

   From then on, the client library just polls the status of that LRO. It does
   this using the `GetOperation` RPC.

   That is why we set expectations on different RPCs for the initial response
   vs. all subsequent responses.

1. Expectations are set in a [sequence].

   This allows `mockall` to verify the order of the calls. It is also necessary
   to determine which `expect_get_operation` is matched.

Finally, we create a client from the mock, call our function, and verify the
response.

```rust,ignore
{{#rustdoc_include ../samples/tests/mocking_lros_manual.rs:client-call}}
```

## Simulating errors

Errors can arise in an LRO from a few places.

If your application uses automatic polling, the following cases are all
equivalent: `until_done()` returns the error in the `Result`, regardless of
where it originated.
[Simulating an error starting an LRO](#simulating-an-error-starting-an-lro) will
yield the simplest test.

Note that the stubbed out client does not have a retry or polling policy. In all
cases, the polling loop will terminate on the first error, even if the error is
typically considered transient.

### Simulating an error starting an LRO

The simplest way to simulate an error is to have the initial request fail with
an error.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:expectation-initial}}
```

For manual polling, an error starting an LRO is returned via the completed
branch. This ends the polling loop.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:completed-branch}}
```

### Simulating an LRO resulting in an error

If you need to simulate an LRO resulting in an error, after intermediate
metadata is returned, we need to return the error in the final
`longrunning::model::Operation`.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:error-op}}
```

We set our expectations to return the `Operation` from `get_operation` as
before.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:expectation-final}}
```

An LRO ending in an error will be returned via the completed branch. This ends
the polling loop.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:completed-branch}}
```

### Simulating a polling error

Polling loops can also exit because the polling policy has been exhausted. When
this happens, the client library can not say definitively whether the LRO has
completed or not.

If your application has custom logic to deal with this case, we can exercise it
by returning an error from the `get_operation` expectation.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:expectation-polling-error}}
```

An LRO ending with a polling error will be returned via the polling error
branch.

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:polling-error-branch}}
```

______________________________________________________________________

## Automatic polling - Full test

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_auto.rs:all}}
```

## Manual polling with intermediate metadata - Full test

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_manual.rs:all}}
```

## Simulating errors - Full tests

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/mocking_lros_error.rs:all}}
```

[sequence]: https://docs.rs/mockall/latest/mockall/struct.Sequence.html
[speech-stub]: https://docs.rs/google-cloud-speech-v2/latest/google_cloud_speech_v2/stub/trait.Speech.html
