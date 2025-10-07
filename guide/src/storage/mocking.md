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

# How to write tests using the Storage client

The Google Cloud Client Libraries for Rust provide a way to stub out the real
client implementations, so a mock can be injected for testing.

Applications can use mocks to write controlled, reliable unit tests that do not
involve network calls, and do not incur billing.

In this guide, you will learn:

- Why the design of the `Storage` client deviates from the design of other
  Google Cloud clients
- How to write testable interfaces using the `Storage` client
- How to mock reads
- How to mock writes

This guide is specifically for mocking the `Storage` client. For a generic
mocking guide (which applies to the `StorageControl` client), see
[How to write tests using a client](../mock_a_client.md).

## Design rationale

### Other clients

Most clients, such as `StorageControl` hold a boxed, `dyn`-compatible
implementation of the stub trait internally. They use dynamic dispatch to
forward requests from the client to their stub (which could be the real
implementation or a mock).

Because dynamic dispatch is used, the exact type of the stub does not need to be
known by the compiler. The clients do not need to be templated on their stub.

### `Storage` client

In order to have a `dyn`-compatible trait, the size of all types must be known.

The `Storage` client has complex types in its interfaces.

- `write_object` accepts a generic payload.
- `read_object` returns a stream-like thing.

Thus, if we wanted to use the same dynamic dispatch approach for the `Storage`
client, we would have to end up boxing all generics / trait `impl`s. Each box is
an extra heap allocation, plus the dynamic dispatch.

Because we want the `Storage` client to be as performant as possible, we decided
it was preferable to template the client on a non-`dyn`-compatible, concrete
implementation of the stub trait.

## Testable interfaces

Applications that do not need to test their code can simply write all interfaces
in terms of `Storage`. The default `T` is the real implementation of the client.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:prod-only-interface}}
```

Applications that need to test their code, should write their interfaces in
terms of the generic `T`.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:testable-interface}}
```

## Mocking reads

Let's say we have an application function which downloads an object and counts
how many newlines it contains.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:count-newlines}}
```

We want to test our code against a known response from the server. We can do
this by faking the `ReadObjectResponse`.

A `ReadObjectResponse` is essentially a stream of bytes. You can create a fake
`ReadObjectResponse` in tests by supplying a payload to
`ReadObjectResponse::from_source`. The library accepts the same payload types as
`Storage::write_object`.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:fake-read-object-resp}}
```

We define the mock as usual, using `mockall`.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:mockall}}
```

We write a unit test, which calls into our `count_newlines` function.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:test-count-lines}}
```

## Mocking writes

Let's say we have an application function which uploads an object from memory.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:upload}}
```

To test this function, we define the mock as usual, using `mockall`.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:mockall}}
```

We write a unit test, which calls into our `upload` function.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:test-upload}}
```

### Details

Because our function calls `send_unbuffered()`, we should use the corresponding
`write_object_unbuffered()`.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:expect-unbuffered}}
```

Generics in `mockall::mock!` are treated as different functions. We need to
provide the exact payload type, so the compiler knows which function to use.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:explicit-payload-type}}
```

______________________________________________________________________

## Full application code and test suite

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/mocking.rs:all}}
```