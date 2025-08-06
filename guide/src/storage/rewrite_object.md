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

# Rewriting objects

Rewriting a [Cloud Storage] object can require multiple client requests,
depending on the [details] of the operation. In such cases, the service will
return a response representing its progress, along with a `rewrite_token` which
the client must use to continue the operation.

This guide will show you how to fully execute the rewrite loop for a Cloud
Storage object.

## Prerequisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled], and a Cloud Storage bucket in that project.

## Add the client library as a dependency

```shell
cargo add google-cloud-storage
```

## Rewriting an object

### Prepare client

First, create a client.

The service recommends an [overall timeout] of at least 30 seconds. In this
example, we use a `RetryPolicy` that does not set any timeout on the operation.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:client}}
```

### Prepare builder

Next we prepare the request builder.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:builder}}
```

Optionally, we can limit the maximum amount of bytes written per call, before
the service responds with a progress report. Setting this option is an
alternative to increasing the attempt timeout.

Note that the value used in this example is intentionally small to force the
rewrite loop to take multiple iterations. In practice, you would likely use a
larger value.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:limit-bytes-per-call}}
```

Rewriting an object allows you to copy its data to a different bucket, copy its
data to a different object in the same bucket, change its encryption key, and/or
change its [storage class]. The rewrite loop is identical for all these
transformations. We will change the storage class to illustrate the code.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:change-storage-class}}
```

Note that there is a [minimum storage duration] associated with the new storage
class. While the object used in this example (3 MiB) incurs less than `$0.001`
of cost, the billing may be noticeable for larger objects.

### Introduce rewrite loop helpers

Next, we introduce a helper function to perform one iteration of the rewrite
loop.

We send the request and process the response. We log the progress made.

If the operation is `done`, we return the object metadata, otherwise we return
the rewrite token.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:make-one-request}}
```

### Execute rewrite loop

Now we are ready to perform the rewrite loop until the operation is done.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:loop}}
```

Note how if the operation is incomplete, we supply the rewrite token returned by
the server to the next request.

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:set-rewrite-token}}
```

Also note that the rewrite token can be used to continue the operation from
another process. Rewrite tokens are valid for up to one week.

## Full program

Putting all these steps together you get:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/rewrite_object.rs:all}}
```

We should see output similar to:

```norust
PROGRESS: total_bytes_rewritten=1048576; object_size=3145728
PROGRESS: total_bytes_rewritten=2097152; object_size=3145728
DONE:     total_bytes_rewritten=3145728; object_size=3145728
dest_object=Object { name: "rewrite-object-clone", ... }
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud storage]: https://cloud.google.com/storage
[details]: https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[minimum storage duration]: https://cloud.google.com/storage/pricing#early-delete
[overall timeout]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/retry_policy/trait.RetryPolicyExt.html#method.with_time_limit
[storage class]: https://cloud.google.com/storage/docs/storage-classes
