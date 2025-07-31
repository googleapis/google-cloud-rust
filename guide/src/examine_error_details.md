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

# Examine error details

Some Google Cloud services include additional error details when requests fail.
To help with any troubleshooting, the Google Cloud client libraries for Rust
always include these details when errors are formatted using
`std::fmt::Display`. Some applications may want to examine these details and
change their behavior based on their contents.

This guide will show you how to examine the error details returned by Google
Cloud services.

## Prerequisites

The guide uses the [Cloud Natural Language API], that makes the examples more
concrete and therefore easier to follow. With that said, the same ideas work for
any other service.

You may want to follow the service [quickstart]. This guide will walk you
through the steps necessary to enable the service, ensure you have logged in,
and that your account has the necessary permissions.

### Dependencies

As it is usual with Rust, you must declare the dependency in your `Cargo.toml`
file:

```shell
cargo add google-cloud-language-v2
```

## Examining error details

We will create a request that intentionally results in an error, and then
examine the error contents. First, create a client:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-client}}
```

Then send a request. In this case, a key field is missing:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-request}}
```

Extract the error from the result, using standard Rust functions. The error type
prints all the error details in human readable form:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-print}}
```

This should produce output similar to:

```text
request failed with error Error {
    kind: Service {
        status_code: Some(
            400,
        ),
        headers: Some(
            {
                "vary": "X-Origin",
                "vary": "Referer",
                "vary": "Origin,Accept-Encoding",
                "content-type": "application/json; charset=UTF-8",
                "date": "Sat, 24 May 2025 17:19:49 GMT",
                "server": "scaffolding on HTTPServer2",
                "x-xss-protection": "0",
                "x-frame-options": "SAMEORIGIN",
                "x-content-type-options": "nosniff",
                "alt-svc": "h3=\":443\"; ma=2592000,h3-29=\":443\"; ma=2592000",
                "accept-ranges": "none",
                "transfer-encoding": "chunked",
            },
        ),
        status: Status {
            code: InvalidArgument,
            message: "One of content, or gcs_content_uri must be set.",
            details: [
                BadRequest(
                    BadRequest {
                        field_violations: [
                            FieldViolation {
                                field: "document.content",
                                description: "Must have some text content to annotate.",
                                reason: "",
                                localized_message: None,
                                _unknown_fields: {},
                            },
                        ],
                        _unknown_fields: {},
                    },
                ),
            ],
        },
    },
}
```

### Programmatically examining the error details

Sometimes you may need to examine the error details programmatically. In the
rest of the example we will traverse the data structure and print the most
relevant fields.

Only errors returned by the service contain detailed information, so we first
query the error to see if it contains the correct error type. If it does, we can
break down some top-level information about the error:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-service-error}}
```

And then iterate over all the details:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-iterate}}
```

The client libraries return a [`StatusDetails`] enum with the different types of
error details. In this example we will only examine `BadRequest` errors:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-bad-request}}
```

A `BadRequest` contains a list of fields that are in violation, we can iterate
and print the details for each:

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details-each-field}}
```

Such information can be useful during development. Other branches of
`StatusDetails`, such as [`QuotaFailure`] may be useful at runtime to throttle
an application.

### Expected output

Typically the output from the error details will look like so:

```text
  status.code=400, status.message=One of content, or gcs_content_uri must be set., status.status=Some("INVALID_ARGUMENT")
  the request field document.content has a problem: "Must have some text content to annotate."
```

______________________________________________________________________

## Examining error details: complete code

```rust,ignore
{{#include ../samples/src/examine_error_details.rs:examine-error-details}}
```

[cloud natural language api]: https://cloud.google.com/natural-language
[quickstart]: https://cloud.google.com/natural-language/docs/quickstarts
[`quotafailure`]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/error/rpc/enum.StatusDetails.html#variant.QuotaFailure
[`statusdetails`]: https://docs.rs/google-cloud-gax/latest/google_cloud_gax/error/rpc/enum.StatusDetails.html
