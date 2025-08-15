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

# Use errors to terminate uploads

In this guide you will learn how to use errors and custom data sources to
terminate an upload before it is finalized. This is useful when applications
want to upload data but may want to stop the client library from finalizing the
upload if there is some error condition.

## Prerequisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled], and a Cloud Storage bucket in that project.

The tutorial assumes you are familiar with the basics of using the client
library. If not, you may want to read the [quickstart guide] first.

## Add the client library as a dependency

```shell
cargo add google-cloud-storage
```

## Overview

The client library uploads data from any type implementing the `StreamingSource`
trait. The client library pulls data from implementations of the trait. The
library terminates the upload on the first error.

In this guide you will build a custom implementation of `StreamingSource` that
returns some data and then stops on an error. You will verify that an upload
using this custom data source returns an error.

## Create a custom error type

To terminate an upload without finalizing it, your [StreamingSource] must return
an error. In this example you will create a simple error type, in your
application code you can use any existing error type:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-error-type}}
```

The client library requires that your custom error type implements the standard
[Error] trait:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-error-impl-error}}
```

As you may recall, that requires implementing [Display] too:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-error-impl-display}}
```

## Create a custom `StreamingSource`

Create a type that generates the data to upload. In this example you will use
synthetic data using a counter:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-source}}
```

Implement the streaming source trait for your type:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-source-impl}}
    // ... more details below ...
}
```

Define the error type:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-source-impl-error}}
```

And implement the main function in this trait. Note how this function will
(eventually) return the error type you defined above:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:my-source-impl-next}}
```

## Perform an upload

As usual, you will need a client to interact with [Cloud Storage]:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:attempt-upload-client}}
```

Use the custom type to perform an upload:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:attempt-upload-upload}}
```

As expected, this upload fails. You can inspect the error details:

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:attempt-upload-inspect-error}}
```

## Next Steps

- [Push data on object uploads](queue.md)

## Full Program

```rust,ignore,noplayground
{{#rustdoc_include ../../samples/tests/storage/terminate_uploads.rs:all}}
```

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[cloud storage]: https://cloud.google.com/storage
[display]: https://doc.rust-lang.org/std/fmt/trait.Display.html
[error]: https://doc.rust-lang.org/std/error/trait.Error.html
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[quickstart guide]: /storage.md#quickstart
[streamingsource]: https://docs.rs/google-cloud-storage/latest/google_cloud_storage/streaming_source/trait.StreamingSource.html
