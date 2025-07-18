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
returned by the client library. This guide will show you how to write code to
handle such errors.

> **Retryable errors:** one of the most common reasons to handle errors in
> distributed systems is to retry requests that fail due to transient errors.
> The Google Cloud client libraries for Rust implement a policy-based retry
> loop. You only need to configure the policies to enable the retry loop, and
> the libraries implement common retry policies. Consult the
> [Configuring Retry Policies] section before implementing your own retry loop.

## Prerequisites

The guide uses the [Secret Manager] service, that makes the examples more
concrete and therefore easier to follow. With that said, the same ideas work for
any other service.

You may want to follow the service [quickstart]. This guide will walk you
through the steps necessary to enable the service, ensure you have logged in,
and that your account has the necessary permissions.

### Dependencies

As it is usual with Rust, you must declare the dependency in your `Cargo.toml`
file. We use:

```toml
{{#include ../samples/Cargo.toml:secretmanager}}
```

In addition, this guide uses `crc32c` to calculate the checksum:

```toml
{{#include ../samples/Cargo.toml:crc32c}}
```

## Motivation

In this guide we will create a new *secret version*. Secret versions are
contained in *secrets*. One must create the secret before creating secret
versions. A common pattern in cloud services is to use a resource as-if the
container for it existed, and only create the container if there is an error. If
the container exists most of the time, such an approach is more efficient than
checking if the container exists before making the request. Checking if the
container exists consumes more quota, results in more RPC charges, and is slower
when the container already exists.

## Handling the error

First make an attempt to create a new secret version:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-initial-attempt}}
```

If this succeeds, we can just print the successful result and return:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-success}}
```

The request may have failed for many reasons: because the connection dropped
before the request was fully sent, or the connection dropped before the response
was received, or because it was impossible to create the authentication tokens.

The retry policies can deal with most of these errors, here we are interested
only in errors returned by the service:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-svc-error}}
```

and then only in errors that correspond to a missing secret:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-not-found}}
```

If this is a "not found" error, we try to create the secret. This will simply
return on failures:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-create}}
```

Assuming the creation of the secret is successful, we can try to create the
secret version again, this time just returning an error if anything fails:

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret-try-again}}
```

______________________________________________________________________

## Error handling: complete code

```rust,ignore
{{#include ../samples/src/error_handling.rs:update-secret}}
```

[configuring retry policies]: /configuring_retry_policies.md
[quickstart]: https://cloud.google.com/secret-manager/docs/quickstart
[secret manager]: https://cloud.google.com/secret-manager
