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

# Using the Compute Engine API

The [Compute Engine] API allows you to create and run virtual machines (VMs) on
Google Cloud.

This guide shows you how to initialize the Compute Engine client library for
Rust, and how to perform some basic operations using the library.

## Pre-requisites

The guide assumes you have an existing [Google Cloud project] with
[billing enabled].

## Add the client library as a dependency

Use `cargo` to add the necessary dependency:

```toml
cargo add google-cloud-compute-v1
```

## Create a virtual machine

The client to create, and manipulate virtual machines is called `Instances`:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/src/compute/quickstart.rs:client}}
```

The client has several methods. For example `insert()` creates a new virtual
machine:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/src/compute/compute_instances_create.rs:all}}
```

Note that this is a [long-running operation], you need to wait for its outcome,
and verify if there are any errors.

## Cleanup

Delete the virtual machine using the console, or the `delete()` method:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/src/compute/compute_instances_delete.rs:all}}
```

## Next Steps

- [Working with long-running operations](/working_with_long_running_operations.md)
- [Configuring polling policies](/configuring_polling_policies.md)
- [Compute Engine client libraries](https://cloud.google.com/compute/docs/api/libraries)

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[compute engine]: https://cloud.google.com/compute
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[long-running operation]: /working_with_long_running_operations.md
