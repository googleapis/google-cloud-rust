***
NOTE: this project is experimental and is supported on a best effort basis.
***

# Google Cloud API Client Libraries for Rust

> Rust idiomatic client libraries for [Google Cloud Platform](https://cloud.google.com/) services.

Libraries are available on GitHub and crates.io for developing Rust
applications that interact with individual Google Cloud services:

| Source code | Release Level | Version |
|------|----------|-----------------|
| TODO() | [![Experimental][experimental-stability]][launch-stages] | [![crates.io](https://img.shields.io/crates/v/google-cloud-todo)](https://crates.io/google-cloud-todo/) |

## Enabling APIs

Before you can interact with a given Google Cloud Service, you must enable its API.

Links are available for enabling APIs in each library's README.md.

## Authentication

### Download your Service Account Credentials JSON file

To use Application Default Credentials, you first need to download a set of JSON credentials for your project. Go to **APIs & Auth** > **Credentials** in the [Google Developers Console][devconsole] and select **Service account** from the **Add credentials** dropdown.

> This file is your *only copy* of these credentials. It should never be
> committed with your source code, and should be stored securely.

Once downloaded, store the path to this file in the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

### Other Authentication Methods

Other authentication methods are outlined in the README for TODO(),
which is the authentication library used by all Google Cloud Rust clients.

## Example Applications

TODO()

## Supported Rust Versions

Our client libraries are compatible with the current edition of Rust.

## Versioning

Our libraries follow [Semantic Versioning][semver].

Any release versioned `0.x.y` is subject to backwards-incompatible changes at any time.

**GA**: Libraries defined at the GA (general availability) quality level are stable. The code surface will not change in backwards-incompatible ways unless absolutely necessary (e.g. because of critical security issues) or with an extensive deprecation period. Issues and requests against GA libraries are addressed with the highest priority.

Please note that the auto-generated portions of the GA libraries (the ones in modules such as `v1` or `v2`) are considered to be of **Beta** quality, even if the libraries that wrap them are GA.

**Preview**: Libraries defined at the Preview quality level are expected to be mostly stable, while we work towards their release candidate. We will address issues and requests with a higher priority.

## Contributing

Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING][contributing] for more information on how to get started.

## License

Apache 2.0 - See [LICENSE][license] for more information.

[ga-stability]: https://img.shields.io/badge/stability-ga-green
[ga-description]: #ga
[preview-stability]: https://img.shields.io/badge/stability-preview-orange
[preview-description]: #preview
[experimental-stability]: https://img.shields.io/badge/stability-experimental-yellow
[launch-stages]: https://cloud.google.com/products#section-22
[semver]: http://semver.org
[contributing]: CONTRIBUTING.md
[license]: LICENSE
