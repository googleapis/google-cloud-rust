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

# Override authentication credentials

The Google Cloud client libraries for Rust automatically authenticate your
requests to Google Cloud services. Some applications may need to override the
default authentication. This guide shows you how to override the default.

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

## Override the default credentials: API keys

[API keys] are text strings that grant access to some Google Cloud services.
Using API keys may simplify development as they require less configuration than
other [authentication methods]. There are some risks associated with API keys,
we recommended you read [Best practices for managing API keys] if you plan to
use them.

First, add some use declarations to simplify the rest of the example:

```rust,ignore
{{#include ../samples/src/authentication/api_key.rs:rust_auth_api_key_use}}
```

This example receives the API key string as an input parameter:

```rust,ignore
{{#include ../samples/src/authentication/api_key.rs:rust_auth_api_key_parameter}}
```

Use the API Keys [Builder][api keys builder] to create the credentials:

```rust,ignore
{{#include ../samples/src/authentication/api_key.rs:rust_auth_api_key_credentials}}
```

Initialize the client using the result:

```rust,ignore
{{#include ../samples/src/authentication/api_key.rs:rust_auth_api_key_client}}
```

Use this client as usual:

```rust,ignore
{{#include ../samples/src/authentication/api_key.rs:rust_auth_api_key_call}}
```

## Override the default credentials: service account impersonation

Service account impersonation allows you to make API calls on behalf of a
service account. [Use service account impersonation] discusses this form of
authentication in detail.

When you use service account impersonation, you start with an authenticated
principal (your user account or a service account) and request short-lived
credentials for a service account that has the authorization that your use case
requires.

It is more secure than downloading a service account key for the target service
account, as you do not need to hold the credentials in the file system or even
in memory.

First, add some use declarations to simplify the rest of the example:

```rust,ignore
{{#include ../samples/src/authentication/impersonation.rs:rust_auth_impersonation_use}}
```

This example receives the service account identifier as an input parameter. This
can be the service account email or the unique numeric id assigned by Google
when you created the service account:

```rust,ignore
{{#include ../samples/src/authentication/impersonation.rs:rust_auth_impersonation_parameter}}
```

Use the impersonated service account [Builder][impersonated builder] to create
the credentials:

```rust,ignore
{{#include ../samples/src/authentication/impersonation.rs:rust_auth_impersonation_credentials}}
```

Initialize the client using the result:

```rust,ignore
{{#include ../samples/src/authentication/impersonation.rs:rust_auth_impersonation_client}}
```

Use this client as usual:

```rust,ignore
{{#include ../samples/src/authentication/impersonation.rs:rust_auth_impersonation_call}}
```

## More Information

Learn about other authentication methods in the Rust client libraries:

- [Anonymous Credentials][anonymous builder]: to access services or resources
  that do not require authentication.
- [External Accounts][external account builder]: to use
  [Workload identity federation] with the Rust client libraries.
- [Service Accounts][service account builder]: to initialize credentials from a
  [service account key].

[anonymous builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/anonymous/struct.Builder.html
[api keys]: https://cloud.google.com/docs/authentication/api-keys
[api keys builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/api_key_credentials/struct.Builder.html
[authentication methods]: https://cloud.google.com/docs/authentication
[best practices for managing api keys]: https://cloud.google.com/docs/authentication/api-keys-best-practices
[cloud natural language api]: https://cloud.google.com/natural-language
[external account builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/external_account/struct.Builder.html
[how application default credentials work]: https://cloud.google.com/docs/authentication/application-default-credentials
[impersonated builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/impersonated/struct.Builder.html
[service account builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/impersonated/struct.Builder.html
[service account key]: https://cloud.google.com/iam/docs/service-account-creds#key-types
[service quickstart]: https://cloud.google.com/natural-language/docs/setup
[use service account impersonation]: https://cloud.google.com/docs/authentication/use-service-account-impersonation
[workload identity federation]: https://cloud.google.com/iam/docs/workload-identity-federation
