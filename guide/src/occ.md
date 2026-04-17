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

# How to work with Optimistic Concurrency Control (OCC)

Optimistic Concurrency Control (OCC) is a strategy used to manage shared
resources and prevent "lost updates" or race conditions when multiple users or
processes attempt to modify the same resource simultaneously.

As an example, consider systems like Google Cloud IAM, where
the shared resource is an **IAM Policy** applied to a resource (like a Project,
Bucket, or Service). To implement OCC, systems typically use a version number or
an `etag` (entity tag) field on the resource struct.

## Introduction to OCC

Imagine two processes, A and B, try to update a shared resource at the same
time:

1. Process **A** reads the current state of the resource.

2. Process **B** reads the *same* current state.

3. Process **A** modifies its copy and writes it back to the server.

4. Process **B** modifies its copy and writes it back to the server.

Because Process **B** overwrites the resource *without* knowing that Process
**A** already changed it, Process **A**'s updates are **lost**.

OCC solves this by introducing a unique fingerprint which changes every time an
entity is modified. In many systems (like IAM), this is done
using an `etag`. The server checks this tag on every write:

1. When you read the resource, the server returns an `etag` (a unique
fingerprint).

2. When you send the modified resource back, you must include the original
`etag`.

3. If the server finds that the stored `etag` does **not** match the `etag` you
sent (meaning someone else modified the resource since you read it), the write
operation fails with an `ABORTED` or `FAILED_PRECONDITION` error.

This failure forces the client to **retry** the entire process—re-read the *new*
state, re-apply the changes, and try the write again with the new `etag`.

## Implementing the OCC loop

The core of the OCC implementation is a loop that handles the retry logic. You
should set a reasonable maximum number of retries to prevent infinite loops in
cases of high contention.

### Steps of the loop:

| **Step** | **Action** | **Implementation example** |
| --- | --- | --- |
| **Read** | Fetch the current resource state, including the `etag`. | `let mut policy = client.get_iam_policy(request).await?;` |
| **Modify** | Apply the changes to the local struct. | `policy.bindings.push(new_binding);` |
| **Write/Check** | Attempt to save the modified resource using the old `etag`. This action is checked for specific error codes. | `match client.set_iam_policy(request).await { Ok(p) => return Ok(p), Err(e) => { /* retry logic */ } }` |
| **Success/Retry** | If the write succeeds, exit the loop. If it fails with a concurrency error, increment the retry counter and continue the loop (go back to the Read step). |  |

The following code provides an example of how to implement the OCC loop using an
IAM policy on a Project resource as the target.

**Note**: This example assumes the use of the Secret Manager client, but the
same OCC pattern applies to any service or database that implements versioned
updates.

### Example

As usual with Rust, you must declare the dependency in your `Cargo.toml` file:

```shell
cargo add google-cloud-secretmanager-v1
```

```rust,ignore
{{#include ../samples/src/occ/set_iam_policy.rs:occ-loop}}
```
