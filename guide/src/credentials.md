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

# Authentication

The Google Cloud client libraries for Rust automatically authenticate your
requests to Google Cloud services. This section shows you how use the different
authentication methods.

## Prerequisites

This guide uses the [Cloud Natural Language API]. The same concepts apply to the
client libraries for other services. You may want to follow the
[service quickstart], which shows you how to enable the service.

For complete setup instructions for the Rust libraries, see
[Setting up your development environment](/setting_up_your_development_environment.md).

### Dependencies

You must declare the dependencies in your `Cargo.toml` file:

```shell
cargo add google-cloud-language-v2 google-cloud-auth
```

## The default credentials

The recommended way to authenticate applications in Google Cloud is to use
Application Default Credentials. Without any configuration the client libraries
default to this credential type. See [How Application Default Credentials work]
for information about you can configure this default without any code changes in
your application.

First, add some use declarations to simplify the rest of the example:

```rust,ignore
{{#include ../samples/src/authentication/adc.rs:rust_auth_adc_use}}
```

Initialize the client using the defaults:

```rust,ignore
{{#include ../samples/src/authentication/adc.rs:rust_auth_adc_client}}
```

Use this client as usual:

```rust,ignore
{{#include ../samples/src/authentication/adc.rs:rust_auth_adc_call}}
```

## More Information

Learn about other authentication methods in the Rust client libraries:

- [Anonymous Credentials][anonymous builder]: to access services or resources
  that do not require authentication.
- [External Accounts][external account builder]: to use
  [Workload identity federation] with the Rust client libraries.
- [Service Accounts][service account builder]: to initialize credentials from a
  [service account key].
- [Override Credentials]: to override the default credentials.
- [ID Tokens]: obtains and verify [OIDC ID Tokens].

[anonymous builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/anonymous/struct.Builder.html
[cloud natural language api]: https://cloud.google.com/natural-language
[external account builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/external_account/struct.Builder.html
[how application default credentials work]: https://cloud.google.com/docs/authentication/application-default-credentials
[id tokens]: credentials/id_tokens.md
[oidc id tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
[override credentials]: credentials/override.md
[service account builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/impersonated/struct.Builder.html
[service account key]: https://cloud.google.com/iam/docs/service-account-creds#key-types
[service quickstart]: https://cloud.google.com/natural-language/docs/setup
[workload identity federation]: https://cloud.google.com/iam/docs/workload-identity-federation
