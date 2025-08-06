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

# How to write tests using a client

The Google Cloud Client Libraries for Rust provide a way to stub out the real
client implementations, so a mock can be injected for testing.

Applications can use mocks to write controlled, reliable unit tests that do not
involve network calls, and do not incur billing.

This guide shows how.

## Dependencies

There are several [mocking frameworks] in Rust. This guide uses [`mockall`],
which seems to be the most popular.

```shell
cargo add --dev mockall
```

This guide will use a [`Speech`][speech-client] client. Note that the same ideas
in this guide apply to all of the clients, not just the `Speech` client.

We declare the dependency in our `Cargo.toml`. Yours will be similar, but
without the custom `path`.

```shell
cargo add google-cloud-speech-v2 google-cloud-lro
```

## Mocking a client

First, some `use` declarations to simplify the code:

```rust,ignore
{{#include ../samples/tests/mocking.rs:use}}
```

Let's assume our application has a function that uses the `Speech` client to
make an RPC, and process the response from the server.

```rust,ignore
{{#include ../samples/tests/mocking.rs:my_application_function}}
```

We want to test how our code handles different responses from the service.

First we will define the mock class. This class implements the
[`speech::stub::Speech`][speech-stub] trait.

```rust,ignore
{{#include ../samples/tests/mocking.rs:mockall_macro}}
```

Next, we create an instance of the mock. Note that the
[`mockall::mock!`][mock-macro] macro prepends a `Mock` prefix to the name of our
struct from above.

```rust,ignore
{{#include ../samples/tests/mocking.rs:mock_new}}
```

Next we will set expectations on the mock. We expect `GetRecognizer` to be
called, with a particular name.

If that happens, we will simulate a successful response from the service.

```rust,ignore
{{#include ../samples/tests/mocking.rs:mock_expectation}}
```

Now we are ready to create a `Speech` client with our mock.

```rust,ignore
{{#include ../samples/tests/mocking.rs:client_from_mock}}
```

Finally, we are ready to call our function...

```rust,ignore
{{#include ../samples/tests/mocking.rs:call_fn}}
```

... and verify the results.

```rust,ignore
{{#include ../samples/tests/mocking.rs:validate}}
```

### Simulating errors

Simulating errors is no different than simulating successes. We just need to
modify the result returned by our mock.

```rust,ignore
{{#include ../samples/tests/mocking.rs:error}}
```

Note that a client built `from_stub()` does not have an internal retry loop. It
returns all errors from the stub directly to the application.

______________________________________________________________________

## Full program

Putting all this code together into a full program looks as follows:

```rust,ignore,noplayground
{{#include ../samples/tests/mocking.rs:all}}
```

[mock-macro]: https://docs.rs/mockall/latest/mockall/macro.mock.html
[mocking frameworks]: https://blog.logrocket.com/mocking-rust-mockall-alternatives/
[speech-client]: https://docs.rs/google-cloud-speech-v2/latest/google_cloud_speech_v2/client/struct.Speech.html
[speech-stub]: https://docs.rs/google-cloud-speech-v2/latest/google_cloud_speech_v2/stub/trait.Speech.html
[`mockall`]: https://docs.rs/mockall/latest/mockall/
