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

# Generate text using the Vertex AI Gemini API

In this guide, you send a text prompt request, and then a multimodal prompt and
image request to the Vertex AI Gemini API and view the responses.

## Prerequisites

Completing this guide requires you to set up a Google Cloud project and enable
the Vertex AI API. We recommend you use the [Vertex AI Quickstart] to complete
these steps.

## Adding the Vertex AI client library as a dependency

The Vertex AI client library includes many features. Compiling all of them is
relatively slow. To speed up compilation, you can just enable the features you
need:

```toml
{{#include ../samples/Cargo.toml:aiplatform}}
```

## Send a prompt to the Vertex AI Gemini API

First, initialize the client using the default settings:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:text-prompt-client}}
```

We need to build the model name. For simplicity, this example receives the
project ID as an argument and uses a fixed location (`global`) and model id
(`gemini-2.0-flash-001`).

If you want to run this function in your own code, use the project id (without
any `projects/` prefix) of the project you selected while going through the
prerequisites:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:text-prompt-model}}
```

With the client initialized we can send the request:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:text-prompt-request}}
```

And then print the response. We use the `:#?` format specifier to prettify the
nested response objects:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:text-prompt-response}}
```

See [below](#text-prompt-complete-code) for the complete code:

## Send a prompt and an image to the Vertex AI Gemini API

As in the previous example, initialize the client using the default settings:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image-client}}
```

And then build the model name:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image-model}}
```

The new request includes an image part:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image-image-part}}
```

And the prompt part:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image-prompt-part}}
```

We send the full request:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image-request}}
```

As in the previous example, we print the full response:

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image-response}}
```

See [below](#prompt-and-image-complete-code) for the complete code.

______________________________________________________________________

## Text Prompt: complete code

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:text-prompt}}
```

## Prompt and Image: complete code

```rust,ignore,noplayground
{{#include ../samples/src/gemini.rs:prompt-and-image}}
```

[vertex ai quickstart]: https://cloud.google.com/vertex-ai/generative-ai/docs/start/quickstarts/quickstart-multimodal
