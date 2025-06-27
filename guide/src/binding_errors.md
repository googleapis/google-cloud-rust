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

You might have tried to make a request and run into an error that looks like:

```norust
Error: cannot find a matching binding to send the request: at least one of the
conditions must be met: (1) field `name` needs to be set and match:
'projects/*/secrets/*' OR (2) field `name` needs to be set and match:
'projects/*/locations/*/secrets/*'
```

This is a binding error. Let's break this down.

## When this happens

When a client cannot match the request to a [URI] for the service, it will fail
the request locally with a binding error.

Typically this happens when a field is either missing, or in an invalid format.

```rust,ignore
{{#include ../samples/src/binding_errors.rs:request}}
```

## How to fix it

In this example, the `name` field on the top level request was not set. To fix
the code, we will set it to something matching one of the above templates.
Either will allow us to make a request to the server.

```rust,ignore
{{#include ../samples/src/binding_errors.rs:request-success-1}}
```

OR

```rust,ignore
{{#include ../samples/src/binding_errors.rs:request-success-2}}
```

## Interpreting the templates

In the template strings, there are two special matchers.

### Single wildcard

The `*` alone means: a non-empty string without a `/`. It can be thought of as
the regex `[^/]+`.

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

Less common is the `**`, which means: any string. The string can be empty or
contain any number of `/`'s. It can be thought of as the regex `.*`.

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
