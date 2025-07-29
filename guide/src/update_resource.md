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

# Update a resource using a field mask

This guide shows you how to update a resource using a field mask, so that you
can control which fields on the resource will be updated. The guide uses a
secret from [Secret Manager] as a resource, but the concepts apply to other
resources and services as well.

## Prerequisites

To complete this tutorial, you need a Rust development environment with the
following dependencies installed:

* The Secret Manager client library
* [Tokio]

To get set up, follow the steps in [Setting up your development environment].

## Install well known types

The [google_cloud_wkt] crate contains well known types for Google Cloud APIs.
These types typically have custom JSON encoding, and may provide conversion
functions to and from native or commonly used Rust types. `google_cloud_wkt`
contains the field mask type, `FieldMask`, so you'll need to add the crate as a
dependency:

```shell
cargo add google-cloud-wkt
```

## `FieldMask`

A [FieldMask] represents a set of symbolic field paths. Field masks are used to
specify a subset of fields that should be returned by a get operation or
modified by an update operation.

A field mask in an update operation specifies which fields of the targeted
resource should be updated. The API is required to change only the values of the
fields specified in the mask and leave the others untouched. If a resource is
passed in to describe the updated values, the API ignores the values of all
fields not covered by the mask. If a field mask is not present on update, the
operation applies to all fields (as if a field mask of all fields had been
specified).

In order to reset a field to the default value, you must include the field in
the mask and set the default value in the provided resource. Thus, in order to
reset all fields of a resource, provide a default instance of the resource and
set all fields in the mask, or don't provide a mask.

## Update fields on a resource

First, initialize a Secret Manager client and create a secret:

```rust,ignore
{{#include ../samples/src/update_resource.rs:create}}
```

If you examine the output from the create operation, you'll see that both the
`labels` and `annotations` fields are empty.

The following code updates the `labels` and `annotations` fields:

```rust,ignore
{{#include ../samples/src/update_resource.rs:update}}
```

The `set_etag` method lets you set an [etag] on the secret, which prevents
overwriting concurrent updates.

Having set labels and annotations on the updated secret, you pass a field mask
to `set_update_mask` specifying the field paths to be updated:

```rust,ignore
{{#include ../samples/src/update_resource.rs:set-update-mask}}
```

In the output from the update operation, you can see that the fields have been
updated:

```none
labels: {"updated": "your-label"},
...
annotations: {"updated": "your-annotations"},
```

See [below](#update-field-complete-code) for the complete code.

## What's next

In this guide, you updated a resource using a field mask. The sample code uses
the Secret Manager API, but you can use field masks with other clients too. Try
one of the other Cloud Client Libraries for Rust:

* [Generate text using the Vertex AI Gemini API]
* [Using Google Cloud Storage: Push data on object uploads]

______________________________________________________________________

## Update field: complete code

```rust,ignore,noplayground
{{#include ../samples/src/update_resource.rs:update-field}}
```

[etag]: https://cloud.google.com/secret-manager/docs/etags
[fieldmask]: https://docs.rs/google-cloud-wkt/latest/google_cloud_wkt/struct.FieldMask.html
[generate text using the vertex ai gemini api]: generate_text_using_the_vertex_ai_gemini_api.md
[google_cloud_wkt]: https://docs.rs/google-cloud-wkt/latest/google_cloud_wkt/index.html
[secret manager]: https://cloud.google.com/secret-manager/docs/overview
[setting up your development environment]: setting_up_your_development_environment.md
[tokio]: https://tokio.rs/
[using google cloud storage: push data on object uploads]: storage/queue.md
