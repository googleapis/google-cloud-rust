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

[authentication methods at google]: https://cloud.google.com/docs/authentication
[credentials]: https://cloud.google.com/docs/authentication#credentials
[credentials::credentials]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/struct.Credentials.html
[gcloud-auth]: https://crates.io/crates/gcloud-auth
[principals]: https://cloud.google.com/docs/authentication#principal
[tokens]: https://cloud.google.com/docs/authentication#token
