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

# Configuring polling policies

The Google Cloud Client Libraries for Rust provide helper functions to simplify
waiting and monitoring the progress of
[LROs (Long-Running Operations)](working_with_long_running_operations.md). These
helpers use policies to configure the polling frequency and to determine what
polling errors are transient and may be ignored until the next polling event.

This guide will walk you through the configuration of these policies for all the
long-running operations started by a client, or just for one specific request.

There are two different policies controlling the behavior of the LRO loops:

- The polling backoff policy controls how long the loop waits before polling the
  status of a LRO that is still in progress.
- The polling error policy controls what to do on an polling error. Some polling
  errors are unrecoverable, and indicate that the operation was aborted or the
  caller has no permissions to check the status of the LRO. Other polling errors
  are transient, and indicate a temporary problem in the client network or the
  service.

Each one of these policies can be set independently, and each one can be set for
all the LROs started on a client or changed for just one request.

## Prerequisites

The guide uses the [Speech-To-Text V2] service to keep the code snippets
concrete. The same ideas work for any other service using LROs.

We recommend you first follow one of the service guides, such as
[Transcribe speech to text by using the command line]. These guides will cover
critical topics such as ensuring your project has the API enabled, your account
has the right permissions, and how to set up billing for your project (if
needed). Skipping the service guides may result in problems that are hard to
diagnose.

## Dependencies

As it is usual with Rust, you must declare the dependency in your `Cargo.toml`
file. We use:

```toml
{{#include ../samples/Cargo.toml:speech}}

{{#include ../samples/Cargo.toml:lro}}
```

## Configuring the polling frequency for all requests in a client

If you are planning to use the same polling backoff policy for all (or even
most) requests with the same client then consider setting this as a client
option.

To configure the polling frequency you use a type implementing the
[PollingBackoffPolicy] trait. The client libraries provide [ExponentialBackoff]:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-backoff-use}}
```

Then initialize the client with the configuration you want:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-backoff-client}}
```

Unless you override the policy with a [per-request setting] this policy will be
in effect for any long-running operation started with the client. In this
example, if you make a call such as:

```rust,ignore
    let mut operation = client
        .batch_recognize(/* stuff */)
        /* more stuff */
        .send()
        .await?;
```

The client library will first wait for 500ms, after the first polling attempt,
then for 1,000ms (or 1s) for the second attempt, and sub-sequent attempts will
wait 2s, 4s, 8s and then all attempts will wait 10s.

See
[below](#configuring-the-polling-frequency-for-all-requests-in-a-client-complete-code)
for the complete code.

## Configuring the polling frequency for a specific request

As described in the previous section. We need a type implementing the
[PollingBackoffPolicy] trait to configure the polling frequency. We will also
use [ExponentialBackoff] in this example:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff-use}}
```

The configuration of the request will require bringing a trait within scope:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff-builder-trait}}
```

You create the request builder as usual:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff-builder}}
```

And then configure the polling backoff policy:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff-rpc-polling-backoff}}
```

You can issue this request as usual. For example:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff-print}}
```

See
[below](#configuring-the-polling-frequency-for-a-specific-request-complete-code)
for the complete code.

## Configuring the retryable polling errors for all requests in a client

To configure the retryable errors we need to use a type implementing the
[PollingErrorPolicy] trait. The client libraries provide a number of them, a
conservative choice is [Aip194Strict]:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-errors-use}}
```

If you are planning to use the same polling policy for all (or even most)
requests with the same client then consider setting this as a client option.

Add the polling policies that you will use for all long running operations:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-errors-client}}
```

You can also add retry policies to handle errors in the initial request:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-errors-client-retry}}
```

Unless you override the policy with a [per-request setting] this policy will be
in effect for any long-running operation started with the client. In this
example, if you make a call such as:

```rust,ignore
    let mut operation = client
        .batch_recognize(/* stuff */)
        /* more stuff */
        .send()
        .await?;
```

The client library will only treat `UNAVAILABLE` (see [AIP-194]) as a retryable
error, and will stop polling after 100 attempts or 300 seconds, whichever comes
first.

See
[below](#configuring-the-retryable-polling-errors-for-all-requests-in-a-client-complete-code)
for the complete code.

## Configuring the retryable polling errors for a specific request

To configure the retryable errors we need to use a type implementing the
[PollingErrorPolicy] trait. The client libraries provide a number of them, a
conservative choice is [Aip194Strict]:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-errors-use}}
```

The configuration of the request will require bringing a trait within scope:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-errors-builder-trait}}
```

You create the request builder as usual:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-errors-builder}}
```

And then configure the polling backoff policy:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-errors-rpc-polling-errors}}
```

You can issue this request as usual. For example:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-errors-print}}
```

Consider adding a retry policy in case the initial request to start the LRO
fails:

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-errors-client}}
```

See
[below](#configuring-the-retryable-polling-errors-for-a-specific-request-complete-code)
for the complete code.

## Configuring the polling frequency for all requests in a client: complete code

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-backoff}}
```

## Configuring the polling frequency for a specific request: complete code

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff}}
```

## Configuring the retryable polling errors for all requests in a client: complete code

```rust,ignore
{{#include ../samples/src/polling_policies.rs:client-backoff}}
```

## Configuring the retryable polling errors for a specific request: complete code

```rust,ignore
{{#include ../samples/src/polling_policies.rs:rpc-backoff}}
```

[aip-194]: https://google.aip.dev/194
[aip194strict]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/polling_error_policy/struct.Aip194Strict.html
[exponentialbackoff]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/exponential_backoff/struct.ExponentialBackoff.html
[per-request setting]: #configuring-the-polling-frequency-for-a-specific-request
[pollingbackoffpolicy]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/polling_backoff_policy/trait.PollingBackoffPolicy.html
[pollingerrorpolicy]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/polling_error_policy/trait.PollingErrorPolicy.html
[speech-to-text v2]: https://cloud.google.com/speech-to-text/v2
[transcribe speech to text by using the command line]: https://cloud.google.com/speech-to-text/v2/docs/transcribe-api
