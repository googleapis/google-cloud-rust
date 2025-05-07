# How-To Guide: Implementing a new Codec

This guide is intended for contributors to the `sidekick` project. It will walk
you through the steps necessary to add a new `codec`. If you are just looking
for instructions on how to use the `sidekick` consult the top-level README or
the specific guide for your SDK.

## What is a `codec`

In the context of `sidekick`: a golang package that outputs the syntax tree for
one specific "API". The most common case is to generate a client library (a
GAPIC) for a specific SDK. As usual with software, the same abstraction can be
used to output other things, like a text representation of the API syntax tree,
or a CLI, or maybe a partial client library that is wrapped by a veneer.

The basic data flows is:

- Sidekick parses the source specification of an API (Protobuf or OpenAPIv3) and
  converts this into a language-neutral, source-neutral abstract syntax tree. We
  often call this syntax tree "the model".
- Sidekick then calls the `Generate()` function in a codec, and passes the model
  (as a `api.API` data structure, and some configuration options).
- The codec annotates the syntax tree. Each node in the tree has a `Codec`
  field. The codec is expected to put its own data structure in that node.
- The code then loads a number of mustache templates and calls these templates
  to output the `api.API` data structure as described by these templates.

## What are the contents of an `api.API`?

The elements to be generated are in `api.API.Messages`, `api.API.Enums` and
`api.API.Services`. Note that `Services` is a list, and typically there may be
more than one service (in the "gRPC service" sense in the same `api.API`
invocation).

Sidekick requires that all the elements in an `api.API` are in the same
namespace. That is, all the elements in the generation lists must have the same
`.Package` value in their field, and none may have the same `.ID` field.

This is trivial for OpenAPI. For Protobuf this implies all the root messages,
root enums, and services must be part of the same package. If you need to
generate multiple packages that will require different calls to `Generate()`.

To support mixins, `api.API` may reference messages, enums and and services.
These are found in the `api.API.State.*ByID` maps.

## How does sidekick gets its configuration?

In a SDK repository the client libraries are generated in separate directories.
Each directory contains a `.sidekick.toml` file. This file describes where to
find the source for the client library, and may include some extra configuration
for the codec. The top-level directory contains a `.sidekick.toml` configuration
file which applies to all the generated client libraries.

## How is `api.API` built?

The `.sidekick.toml` file describes what parser to use for that directory, and
where to find the source. The most common parser is `protobuf`. This parser
typically receives a directory as the source, the parser uses all the `.proto`
files in that directory (without recursion). It is also possible to list
specific files, or to exclude some files.

The parser calls `protoc`, reads the resulting proto descriptors and the service
config YAML, and then converts that into a `api.API` instance. This is the input
into your codec.

## Implementing a `codec`

First, add a module, something like this would do:

```shell
ls -l generator/internal/codec_sample
```

The main entry point in that module is the `Generate()` function, found in
`generate.go`:

```shell
cat generator/internal/codec_sample/generate.go
```

This module has a single (and relatively simple) template file:

```shell
cat generator/internal/codec_sample/templates/readme/README.md.mustache
```

The codec is invoked from a single point in sidekick:

```shell
git grep -A 2 -B 2 codec_sample.Generate
```

## Basic integration tests

A simple integration test for this module is found in:

```shell
cat generator/internal/sidekick/sidekick_sample_test.go
```

## Unit tests

You can write tests for the codec as usual. There are some helpers to initialize
the `api.API` data structure. Look at the other codecs for examples.

## Annotations

Eventually your codec will need to add annotations to the `api.API` structure. A
simple annotation may be a boolean indicating if an API has any services. If you
needed such an annotation you would write your own `annotate()` function:

```go
type modelAnnotation {
  HasServices bool
}

func annotate(model *api.API) {
  model.Codec = &modelAnnotation{
    HasServices: len(model.Services) > 0
  }
}
```

You would need to call this function from your `Generate()` function. Note that
only the `.Codec` field is modified, and that the type is of your own choosing.

In a more interesting codec you would iterate over the `model` data structure
and set the `.Codec` field with annotations about methods, messages, enums, and
so forth. Look at the other codecs for examples. The most common annotations are
the names of the generated elements, as these often differ in non-trivial ways
from the source name.
