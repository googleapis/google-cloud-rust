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

# Configuring Retry Policies

The Google Cloud client libraries for Rust can automatically retry operations
that fail due to transient errors. However, the clients do not automatically
enable the retry loop. The application must set the retry policy to enable this
feature.

This guide shows you how to enable the retry loop. First you'll learn how to
enable a common retry policy for all requests in a client, and then how to
override this default for a specific request.

## Prerequisites

The guide uses the [Secret Manager] service. That makes the examples more
concrete and therefore easier to follow. With that said, the same ideas work for
any other service.

You may want to follow the service [quickstart]. This guide will walk you
through the steps necessary to enable the service, ensure you have logged in,
and that your account has the necessary permissions.

## Dependencies

As usual with Rust, you must declare dependencies in your `Cargo.toml` file:

```toml
{{#include ../samples/Cargo.toml:secretmanager}}
```

## Configuring the default retry policy

This example uses the [`Aip194Strict`] policy. As the name implies, this policy
is based on the guidelines in [`AIP-194`], which documents the conditions under
which a Google API client should automatically retry a request. The policy is
fairly conservative, and will not retry any error that indicates the request
*may* have reached the service, **unless** the request is idempotent. As such,
the policy is safe to use as a default. The only downside may be additional
requests to the service, consuming some quota and billing.

To make this the default policy for the service, set the policy during the
client initialization:

```rust,ignore
{{#include ../samples/src/retry_policies.rs:client-retry-client}}
```

Then use the service as usual:

```rust,ignore
{{#include ../samples/src/retry_policies.rs:client-retry-request}}
```

See [below](#configuring-the-default-retry-policy-complete-code) for the
complete code.

## Configuring the default retry policy with limits

The [`Aip194Strict`] policy does not limit the number of retry attempts or the
time spent retrying requests. However, it can be decorated to set such limits.
For example, you can limit *both* the number of attempts and the time spent in
the retry loop using:

```rust,ignore
{{#include ../samples/src/retry_policies.rs:client-retry-full-client}}
```

Requests work as usual too:

```rust,ignore
{{#include ../samples/src/retry_policies.rs:client-retry-full-request}}
```

See [below](#configuring-the-default-retry-policy-with-limits-complete-code) for
the complete code.

## Override the retry policy for one request

Sometimes applications need to override the retry policy for a specific request.
For example, the application developer may know specific details of the service
or application and determine it is safe to tolerate more errors.

For example, deleting a secret is idempotent, because it can only succeed once.
But the client library assumes all delete operations are unsafe. The application
can override the policy for one request:

```rust,ignore
{{#include ../samples/src/retry_policies.rs:request-retry-request}}
```

See [below](#configuring-the-default-retry-policy-with-limits-complete-code) for
the complete code.

## Configuring the default retry policy: complete code

```rust,ignore
{{#include ../samples/src/retry_policies.rs:client-retry}}
```

## Configuring the default retry policy with limits: complete code

```rust,ignore
{{#include ../samples/src/retry_policies.rs:client-retry-full}}
```

## Override the retry policy for one request: complete code

```rust,ignore
{{#include ../samples/src/retry_policies.rs:request-retry}}
```

[quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[secret manager]: https://cloud.google.com/secret-manager
[`aip-194`]: https://aip.dev/194
[`aip194strict`]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/retry_policy/struct.Aip194Strict.html
