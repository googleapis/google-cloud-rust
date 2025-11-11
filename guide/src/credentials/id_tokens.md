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

# ID Tokens

This guide shows you how to generate, use, and verify [OIDC ID tokens] using the
`google-cloud-auth` crate.

ID tokens are a standardized way to verify the identity of a principal in a
secure and portable manner. Unlike access tokens, which are used to authorize
access to Google Cloud APIs, ID tokens are used for service-to-service
authentication. The requesting service can generate an ID token and include it
in the `Authorization` header of a request to the receiving service. The
receiving service can then verify the token to authenticate the caller.

ID tokens are particularly useful in scenarios where you need to authenticate to
a service that is not a Google Cloud API. For example, you can use ID tokens to
securely authenticate requests if your target service is running on [Cloud Run]
or behind an [Identity-Aware Proxy].

## Prerequisites

For complete setup instructions for the Rust libraries, see
[Setting up your development environment](/setting_up_your_development_environment.md).

> **Note on User Credentials:** The `idtoken::Builder` does not currently
> support generating audience-specific ID tokens from user credentials obtained
> via `gcloud auth application-default login` (which are of type
> `authorized_user`). For local development and testing, it is recommended to
> use a service account key file and set the `GOOGLE_APPLICATION_CREDENTIALS`
> environment variable or impersonate a service account using
> `gcloud auth application-default login --impersonate-service-account <service-account-email>`.

### Dependencies

You must declare the dependencies in your `Cargo.toml` file:

```shell
cargo add google-cloud-auth
```

## Obtaining ID Tokens

First, add a `use` declaration to simplify the rest of the example:

```rust,ignore
{{#include ../../samples/src/authentication/request_id_token.rs:request_id_token_use}}
```

This example receives the audience as an input parameter. The audience must
match the audience of the service that receives the token.

```rust,ignore
{{#include ../../samples/src/authentication/request_id_token.rs:request_id_token_parameters}}
```

Use the ID Token [Builder][id token builder] to create the credentials:

```rust,ignore
{{#include ../../samples/src/authentication/request_id_token.rs:request_id_token_client}}
```

Then, fetch the ID token. Note that the client libraries automatically cache the
token and refresh it as needed.

```rust,ignore
{{#include ../../samples/src/authentication/request_id_token.rs:request_id_token_call}}
```

Your application can now use this token to authenticate with other services. A
common use-case is to send the token in the `Authorization:` header. Here an
example using the [reqwest] crate.

```rust,ignore
{{#include ../../samples/src/authentication/request_id_token.rs:request_id_token_send}}
```

## Verify ID Tokens

A receiving service can verify an ID token to authenticate the service making
the request.

First, add a `use` declaration to simplify the rest of the example:

```rust,ignore
{{#include ../../samples/src/authentication/verify_id_token.rs:verify_id_token_use}}
```

This example receives the ID token string and the expected audience as input
parameters. The audience must match the audience of the service.

Use the ID Token [Verifier Builder] to create the verifier:

```rust,ignore
{{#include ../../samples/src/authentication/verify_id_token.rs:verify_id_token_verifier}}
```

Then, verify the token. If verification is successful, it returns the claims
from the token payload.

```rust,ignore
{{#include ../../samples/src/authentication/verify_id_token.rs:verify_id_token_verify_call}}
```

If the token is invalid (e.g., expired, incorrect signature, wrong audience),
the `verify` method will return an error.

## More Information

- [OIDC ID Tokens]
- [ID Token Builder]
- [Verifier Builder]

[cloud run]: https://cloud.google.com/run/
[id token builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/idtoken/struct.Builder.html
[identity-aware proxy]: https://cloud.google.com/security/products/iap
[oidc id tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
[reqwest]: https://docs.rs/reqwest/latest/reqwest/
[verifier builder]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/idtoken/verifier/struct.Builder.html
