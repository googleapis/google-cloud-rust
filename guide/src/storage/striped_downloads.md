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

# Speed up large object downloads

In this tutorial you will learn how to use striped downloads to speed up
downloads of large [Cloud Storage] objects.

## Prerequisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled], and a Cloud Storage bucket in that project.

You will create some large objects during this tutorial, remember to clean up
any resources to avoid excessive billing.

The tutorial assumes you are familiar with the basics of using the client
library. If not, read the [quickstart guide].

## Add the client library as a dependency

```toml
{{#include ../../samples/Cargo.toml:storage}}
```

## Create source data

To run this tutorial you will need some large objects in Cloud Storage. You can
create such objects by seeding a smaller object and then repeatedly composing it
to create objects of the desired size.

You can put all the code for seeding the data in its own function. This function
will receive the storage and storage control clients as parameters. For
information on how to create these clients, consult the [quickstart guide]:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:seed-function}}
    // ... details omitted ...
{{#rustdoc_include ../../samples/tests/storage/striped.rs:seed-function-end}}
```

As usual, the function starts with some use declarations to simplify the code:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:seed-use}}
```

Using the storage client, you upload a 1MiB object:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:upload-1MiB}}
```

Then you use the storage control client to concatenate 32 copies of this object
into a larger object. This operation does not require downloading or uploading
any object data, it is performed by the service:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:compose-32}}
```

You can repeat the operation to create larger and larger objects:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:compose-1024}}
{{#rustdoc_include ../../samples/tests/storage/striped.rs:compose-GiB}}
```

## Striped downloads

Again, write a function to perform the striped download:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:download-function}}
    // ... details below ...
{{#rustdoc_include ../../samples/tests/storage/striped.rs:download-function-end}}
```

Use the storage control client to query the object metadata. This metadata
includes the object size and the current generation:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:get-metadata}}
```

We will split the download of each stripe to a separate function. You will see
the details of this function in a moment, for now just note that is `async`, so
it returns a `Future`:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-function}}
    // ... details below ...
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-function-end}}
```

You can compute the size of each stripe and then call `write_stripe()` to
download each of these stripes. Then collect the results into a vector:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:compute-stripes}}
```

You can use the standard Rust facilities to concurrently await all these
futures:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:run-stripes}}
```

Once they complete, the file is downloaded.

Now you should complete writing the `write_stripe()` function. First, duplicate
the write object and prepare to write starting at the desired offset:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-seek}}
```

Start a download from Cloud Storage:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-reader}}
```

To restrict the download to the desired stripe, use `.with_read_offset()` and
`.with_read_limit()`:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-reader-range}}
```

You may also want to restrict the download to the right object generation. This
will avoid race conditions where another process writes over the object and you
get inconsistent reads:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-reader-generation}}
```

Then you read the data and write it to the local file:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:write-stripe-loop}}
```

## Next steps

- Consider optimizing the case where the last stripe only has a few bytes

## Expected performance

The performance of these downloads depends on:

- The I/O subsystem: if your local storage is not fast enough the downloads will
  be throttled by the writes to disk.
- The configuration of your VM: if you do not have enough CPUs the downloads
  will throttled trying to decrypt the on data, as Cloud Storage and the client
  library always encrypt the data in transit.
- The location of the bucket and the particular object: the bucket may store all
  of the objects (or some objects) in a region different from your VM's
  location. In this case, you may be throttled by the wide-area network
  capacity.

With a large enough VM, using SSD for disk, and with a bucket in the same region
as the VM you should get close to 1,000 MiB/s of effective throughput.

## Full program

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/striped.rs:all}}
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud storage]: https://cloud.google.com/storage
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[quickstart guide]: /storage.md#quickstart
