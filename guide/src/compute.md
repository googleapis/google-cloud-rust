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

## List all the virtual machines

The client to create and manipulate virtual machines is called `Instances`. You
can list all the VMs in a project using the `list()` function of this type:

```rust,ignore,noplayground
{{#rustdoc_include ../samples/src/compute/quickstart.rs:all}}
```

## Next Steps

- [Compute Engine client libraries]
- [Working with long-running operations]
- [Configuring polling policies]

[billing enabled]: https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#confirm_billing_is_enabled_on_a_project
[compute engine]: https://cloud.google.com/compute
[google cloud project]: https://cloud.google.com/resource-manager/docs/creating-managing-projects
[Compute Engine client libraries]: https://cloud.google.com/compute/docs/api/libraries
[working with long-running operations]: working_with_long_running_operations.md
[configuring polling policies]: configuring_polling_policies.md
