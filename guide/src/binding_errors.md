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

# Handling binding errors

You might have tried to make a request and run into an error that looks like
this:

```norust
Error: cannot find a matching binding to send the request: at least one of the
conditions must be met: (1) field `name` needs to be set and match the template:
'projects/*/secrets/*' OR (2) field `name` needs to be set and match the
template: 'projects/*/locations/*/secrets/*'
```

This is a *binding error*, and this guide explains how to troubleshoot binding
errors.

## What causes a binding error

The Google Cloud Client Libraries for Rust primarily use HTTP to send requests
to Google Cloud services. An HTTP request uses a Uniform Resource Identifier
([URI]) to specify a resource.

Some RPCs correspond to multiple URIs. The contents of the request determine
which URI is used.

The client library considers all possible URIs, and only returns a binding error
if no URIs work. Typically this happens when a field is either missing or in an
invalid format.

The example error above was produced by trying to get a resource without naming
the resource. Specifically, the `name` field on a [`GetSecretRequest`] was
required but not set.

```rust,ignore
{{#include ../samples/src/binding_errors.rs:request}}
```

## How to fix it

In this case, to fix the error you'd set the `name` field to something matching
one of the templates shown in the error message:

- `'projects/*/secrets/*'`
- `'projects/*/locations/*/secrets/*'`

Either allows the client library to make a request to the server:

```rust,ignore
{{#include ../samples/src/binding_errors.rs:request-success-1}}
```

or

```rust,ignore
{{#include ../samples/src/binding_errors.rs:request-success-2}}
```

## Interpreting templates

The error message for a binding error includes a number of template strings
showing possible values for the request fields. Most template strings include
`*` and `**` as wildcards to match the field values.

### Single wildcard

The `*` wildcard alone means a non-empty string without a `/`. It can be
thought of as the regex `[^/]+`.

Here are some examples:

| Template                   | Input                         | Match?  |
| -------------------------- | ----------------------------- | ------- |
| `"*"`                      | `"simple-string-123"`         | `true`  |
| `"projects/*"`             | `"projects/p"`                | `true`  |
| `"projects/*/locations"`   | `"projects/p/locations"`      | `true`  |
| `"projects/*/locations/*"` | `"projects/p/locations/l"`    | `true`  |
| `"*"`                      | `""` (empty)                  | `false` |
| `"*"`                      | `"string/with/slashes"`       | `false` |
| `"projects/*"`             | `"projects/"` (empty)         | `false` |
| `"projects/*"`             | `"projects/p/"` (extra slash) | `false` |
| `"projects/*"`             | `"projects/p/locations/l"`    | `false` |
| `"projects/*/locations"`   | `"projects/p"`                | `false` |
| `"projects/*/locations"`   | `"projects/p/locations/l"`    | `false` |

### Double wildcard

Less common is the `**` wildcard, which means any string. The string can be
empty or contain any number of `/`'s. It can be thought of as the regex `.*`.

Also, when a template ends in `/**`, that initial slash is optionally included.

| Template          | Input                      | Match?  |
| ----------------- | -------------------------- | ------- |
| `"**"`            | `""`                       | `true`  |
| `"**"`            | `"simple-string-123"`      | `true`  |
| `"**"`            | `"string/with/slashes"`    | `true`  |
| `"projects/*/**"` | `"projects/p"`             | `true`  |
| `"projects/*/**"` | `"projects/p/locations"`   | `true`  |
| `"projects/*/**"` | `"projects/p/locations/l"` | `true`  |
| `"projects/*/**"` | `"locations/l"`            | `false` |
| `"projects/*/**"` | `"projects//locations/l"`  | `false` |

## Inspecting the error

If you need to inspect the error programmatically, you can do so by checking
that it is a binding error, then downcasting it to a `BindingError`.

```rust,ignore
{{#include ../samples/src/binding_errors.rs:inspect}}
```

[uri]: https://clouddocs.f5.com/api/irules/HTTP__uri.html
[`getsecretrequest`]: https://docs.rs/google-cloud-secretmanager-v1/latest/google_cloud_secretmanager_v1/model/struct.GetSecretRequest.html
