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

# Error handling

Sometimes applications need to branch based on the type and details of the error
returned by the client library. This guide shows you how to write code to handle
such errors.

> **Retryable errors:** One of the most common reasons to handle errors in
> distributed systems is to retry requests that fail due to transient errors.
> The Google Cloud Client Libraries for Rust implement a policy-based retry
> loop. You only need to configure the policies to enable the retry loop, and
> the libraries implement common retry policies. Consult the
> [Configuring Retry Policies] section before implementing your own retry loop.

## Prerequisites

The guide uses the [Secret Manager] service to demonstrate error handling, but
the concepts apply to other services as well.

You may want to follow the service [quickstart], which shows you how to enable
the service and ensure that you've logged in and that your account has the
necessary permissions.

For complete setup instructions for the Rust libraries, see
[Setting up your development environment].

### Dependencies

Add the Secret Manager library to your `Cargo.toml` file:

```shell
cargo add google-cloud-secretmanager-v1
```

In addition, this guide uses `crc32c` to calculate the checksum:

```shell
cargo add crc32c
```

## Motivation

In this guide you'll create a new [secret version]. Secret versions are
contained in [secrets]. You must create the secret before adding secret
versions. A common pattern in cloud services is to use a resource as if the
container for it existed, and only create the container if there is an error. If
the container exists most of the time, such an approach is more efficient than
checking if the container exists before making the request. Checking if the
container exists consumes more quota, results in more RPC charges, and is slower
when the container already exists.

## Handling the error

Here's the code for updating a secret version:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret}}
```

First make an attempt to create a new secret version:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-initial-attempt}}
```

If [`update_attempt`](#update_attempt) succeeds, you can just print the
successful result and return:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-success}}
```

The request may have failed for many reasons: because the connection dropped
before the request was fully sent, or the connection dropped before the response
was received, or because it was impossible to create the authentication tokens.

The retry policies can deal with most of these errors. Here we are interested
only in errors returned by the service:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-svc-error}}
```

and then only in errors that correspond to a missing secret:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-not-found}}
```

If this is a "not found" error, you can try to create the secret. This simply
returns on failure:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-create}}
```

Assuming [`create_secret`](#create_secret) is successful, you can try to add the
secret version again, this time just returning an error if anything fails:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-try-again}}
```

## What's next

Learn more about error handling:

- [Examine error details]
- [Handling binding errors]

______________________________________________________________________

## Code samples

For the complete sample, see [error_handling.rs].

### `update_attempt`

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-attempt}}
```

### `create_secret`

```rust,ignore
{{#include ../samples/src/error_handling.rs:create-secret}}
```

[configuring retry policies]: /configuring_retry_policies.md
[error_handling.rs]: https://github.com/pcoet/google-cloud-rust/blob/main/guide/samples/src/error_handling.rs
[examine error details]: examine_error_details.md
[handling binding errors]: binding_errors.md
[quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[secret manager]: https://cloud.google.com/secret-manager
[secret version]: https://cloud.google.com/secret-manager/docs/add-secret-version
[secrets]: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
[setting up your development environment]: setting_up_your_development_environment.md
