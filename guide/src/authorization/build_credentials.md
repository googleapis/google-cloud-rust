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

# Build Credentials

This document outlines the methods for building credentials to authenticate with
Google Cloud APIs.

## Application Default Credentials

Application Default Credentials (ADC) provide a mechanism for applications to
automatically locate credentials based on the execution environment. This is the
recommended approach for most applications.

```rust
// ANCHOR: ADC
let credentials = Builder::default().build()?;
// ANCHOR_END: ADC
```

## Service Account Impersonation

Service account impersonation enables a service account to assume the identity
and permissions of another service account. This is a common practice for
implementing the principle of least privilege by granting temporary, limited
access to resources.

To utilize service account impersonation, a source credential (e.g., from ADC)
and the email address of the target service account are required. The email of
the service account to impersonate is provided via the
`impersonate_service_account_email` variable in the example below.

```rust
// ANCHOR: service-account-impersonation
let source_credentials = Builder::default().build()?;
let credentials =
    impersonated::Builder::from_source_credentials(source_credentials)
        .with_target_principal(impersonate_service_account_email)
        .build()?;
// ANCHOR_END: service-account-impersonation
```
