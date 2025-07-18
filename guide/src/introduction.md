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

# Introduction

The Google Cloud Client Libraries for Rust is a collection of Rust crates to
interact with Google Cloud Services.

This guide is organized as a series of small tutorials showing how to perform
specific actions with the client libraries. Most Google Cloud services follow a
series of guidelines, collectively known as the [AIPs](https://google.aip.dev).
This makes the client libraries more consistent from one service to the next.
The functions to delete or list resources almost always have the same interface.

## Audience

This guide is intended for Rust developers who are familiar with the language
and the Rust ecosystem. We will assume you know how to use Rust and its
supporting toolchain.

At the risk of being repetitive, most of the guides do not assume you have used
any Google Service or client library before (in Rust or other language).
However, the guides will refer you to service specific tutorials to initialize
their projects and services.

## Service specific documentation

These guides are not intended as tutorials for each service or as extended
guides on how to design Rust applications to work on Google Cloud. They are
starting points to get you productive with the client libraries for Rust.

We recommend you read the service documentation at <https://cloud.google.com> to
learn more about each service. If you need guidance on how to design your
application for Google Cloud, the [Cloud Architecture Center] may have what you
are looking for.

## Reporting bugs

We welcome bugs about the client libraries or their documentation. Please use
[GitHub Issues](https://github.com/googleapis/google-cloud-rust/issues).

## License

The client libraries source and their documentation are release under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

[cloud architecture center]: https://cloud.google.com/architecture
