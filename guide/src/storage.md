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

# Using Google Cloud Storage

Google [Cloud Storage] is a managed service for storing unstructured data.

The Rust client library provides an idiomatic API to access this service. The
client library resumes interrupted downloads and uploads, and automatically
performs integrity checks on the data. For metadata operations, the client
library can retry failed requests, and automatically poll long-running
operations.

## Quickstart

This guide will help show you how to create a Cloud Storage bucket, upload an
object to this bucket, and then read the object back.

### Prerequisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled].

### Add the client library as a dependency

```toml
{{#include ../samples/Cargo.toml:storage}}
```

### Create a storage bucket

The client to perform operations on buckets and object metadata is called
`StorageControl`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:control-client}}
```

To create a bucket you must provide the project name and the desired bucket id:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:control-bucket-required}}
```

You can also provide other attributes for the bucket. For example, if you want
all objects in the bucket to use the same permissions, you can enable
[Uniform bucket-level access]:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:control-bucket-ubla}}
```

Then send this request and wait for the response:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:control-bucket-send}}
```

### Upload an object

The client to perform operations on object data is called `Storage`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:client}}
```

In this case we will create an object called `hello.txt`, with the traditional
greeting for a programming tutorial:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:upload}}
```

### Download an object

To download the contents of an object use `read_object()`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:download}}
```

### Cleanup

Finally we remove the object and bucket to cleanup all the resources used in
this guide:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:cleanup}}
```

## Full program

```rust,ignore,noplayground
{{#rustdoc_include ../samples/tests/storage/quickstart.rs:quickstart}}
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud storage]: https://cloud.google.com/storage
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[uniform bucket-level access]: https://cloud.google.com/storage/docs/uniform-bucket-level-access
