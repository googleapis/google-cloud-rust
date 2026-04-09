<!--
Copyright 2026 Google LLC

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

# Configuring rustls crypto providers

Most Google Cloud Rust crates enable a default rustls crypto provider. This is
the simplest setup and works well for most applications.

Some applications need to control rustls provider selection themselves. For
example, they may need to:

- keep provider installation centralized in the application bootstrap path
- use a provider other than the crate default
- align the dependency graph with stricter internal cryptography requirements

## Use the default provider

If the default features meet your needs, you do not need any extra setup:

```toml
[dependencies]
google-cloud-aiplatform-v1 = "1.9.0"
```

## Install the provider in your application

If your application must manage the process-wide rustls provider itself,
disable the crate defaults that install one for you and install the provider
before creating any Google Cloud clients.

This example uses `aws-lc-rs`:

```toml
[dependencies]
google-cloud-aiplatform-v1 = { version = "1.9.0", default-features = false, features = ["prediction-service"] }
google-cloud-auth = { version = "1.8.0", default-features = false }
rustls = { version = "0.23", default-features = false, features = ["std", "aws_lc_rs"] }
```

```rust
fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("crypto provider should only be installed once at startup");

    // Build Google Cloud clients after the provider is installed.
    Ok(())
}
```

You can use the same pattern with other supported rustls providers.

## Notes for compliance-sensitive environments

These crates support externally managed rustls crypto providers, which helps
applications keep provider selection explicit and auditable.

That is not, by itself, a compliance guarantee. Validation depends on the full
application, dependency graph, feature selection, runtime environment, and how
the deployed binary is built and operated.
