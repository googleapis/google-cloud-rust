# Google Cloud Client Libraries for Rust - Authentication

This crate contains types and functions used to authenticate applications on
Google Cloud. The SDK clients consume an implementation of
[credentials::Credentials] and use these credentials to authenticate RPCs issued
by the application.

[Authentication methods at Google] is a good introduction on the topic of
authentication for Google Cloud services and other Google products. The guide
also describes the common terminology used with authentication, such as
[Principals], [Tokens], and [Credentials].

> This crate used to contain a different implementation, with a different
> surface. [@yoshidan](https://github.com/yoshidan) generously donated the crate
> name to Google. Their crate continues to live as [gcloud-auth].

# Features

- `idtoken`: disabled by default, this feature enables support to create and
  verify [OIDC ID Tokens]. This feature depends on the [jsonwebtoken] crate.
- `default-jsonwebtoken-backend`: enabled by default, this feature enables a
  default backend for the `jsonwebtoken` crate. Currently the default is
  `rust_crypto`, but we may change the default backend at any time, applications
  that have specific needs for this backend should not rely on the current
  default. To control the backend selection:
  - Configure this crate with `default-features = false`, and
    `features = ["idtoken"]`
  - Configure the `jsonwebtoken` crate to use the desired backend.

[authentication methods at google]: https://cloud.google.com/docs/authentication
[credentials]: https://cloud.google.com/docs/authentication#credentials
[credentials::credentials]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/struct.Credentials.html
[gcloud-auth]: https://crates.io/crates/gcloud-auth
[jsonwebtoken]: https://crates.io/crates/jsonwebtoken
[oidc id tokens]: https://cloud.google.com/docs/authentication/token-types#identity-tokens
[principals]: https://cloud.google.com/docs/authentication#principal
[tokens]: https://cloud.google.com/docs/authentication#token
