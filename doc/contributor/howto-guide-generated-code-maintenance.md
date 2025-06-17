# How-To Guide: Generated Code Maintenance

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to generate a new library, update libraries
with new changes in the proto specifications, and refresh the generated code
when the generator changes.

## Prerequisites

The generator and its unit tests use `protoc`, the Protobuf compiler. Ensure you
have `protoc >= v23.0` installed and it is found via your `$PATH`.

```bash
protoc --version
```

If not, follow the steps in [Protocol Buffer Compiler Installation] to download
a suitable version.

Make sure your workstation has up-to-date versions of Rust and Golang. Follow
the instructions in [Set Up Development Environment].

## Generate new library

First define the library's name:

```bash
library=... # e.g. websecurityscanner
```

Next, define the service config yaml path. The following is optimistic; it is
often right, but often wrong:

```bash
yaml="google/cloud/${library}/v1/${library}_v1.yaml"
```

Create a new branch in your fork:

```bash
git checkout -b feat-${library}-generate-library
```

This command will generate the library, add the library to Cargo and git, and
run the necessary tests:

```bash
go -C generator/ run ./cmd/sidekick rust-generate \
    -project-root .. \
    -service-config ${yaml}
```

Often we identify typos in the Protobuf comments. Add the typos to the ignore
list on `.typos.toml` and fix the problem upstream. Do not treat this as a
blocker.

Commit all these changes and send a PR to merge them:

```bash
git commit -m "feat(${library}): generate library"
```

### Generating a library with customized directories

We may need to customize the target or source directory for some generated
libraries. For example, you may need to leave room for other crates in the same
directory. In this case you cannot use `rust-generate` and need to manually
provide the source and output directories to the `generate` subcommand. We will
use `google/api` as an example.

```bash
cargo new --lib --vcs none src/generated/api/types
taplo fmt Cargo.toml
go -C generator/ run ./cmd/sidekick generate \
    -project-root .. \
    -specification-source google/api \
    -service-config google/api/serviceconfig.yaml \
    -output src/generated/api/types # This is non-standard
```

Add the files to `git`, compile them, and run the tests:

```bash
typos && cargo fmt && cargo build && cargo test && cargo doc
git add src/generated/cloud/api/types Cargo.toml Cargo.lock
```

Commit all these changes and send a PR to merge them:

```bash
git commit -m "feat(api/types): generate library"
```

### Testing library generation for an existing library

Sometimes it may be useful to re-generate an existing library, to test the
generation step, practice before generating a new library, or to test the
documentation.

We will use `websecurityscanner` as an example. Start by removing the existing
library:

```shell
sed -i.bak  '/websecurityscanner/d' Cargo.toml
rm Cargo.toml.bak
git rm -fr src/generated/cloud/websecurityscanner/
git commit -m"Remove for testing" Cargo.toml Cargo.lock src/generated/cloud/websecurityscanner/
```

Now add the library back:

```shell
go -C generator/ run ./cmd/sidekick rust-generate -project-root .. \
    -service-config google/cloud/websecurityscanner/v1/websecurityscanner_v1.yaml
```

## Update the code with new googleapis protos

Run:

```bash
go -C generator/ run ./cmd/sidekick update -project-root .. && taplo fmt .sidekick.toml && cargo fmt
```

Then run the unit tests and send a PR with whatever changed.

## Refreshing the code

### All libraries

Run:

```bash
go -C generator/ run ./cmd/sidekick refreshall -project-root .. && cargo fmt
```

Then run the unit tests and send a PR with whatever changed.

### Single library

When iterating, it can be useful to regenerate the code associated with a single
`.sidekick.toml`.

Run:

```bash
go -C generator/ run ./cmd/sidekick refresh \
    -output src/generated/cloud/secretmanager/v1 \
    -project-root .. && \
    cargo fmt -p google-cloud-secretmanager-v1
```

## The Glorious Future

Someday `sidekick` will be stable enough that (a) it will not be part of the
`google-cloud-rust` repository, and (b) we will be able to install it. At that
point we will be able to say:

```bash
go install github.com/googleapis/google-cloud-generator/sidekick@v1.0.0
```

And we will be able to issue shorter commands, such as:

```bash
sidekick update && taplo fmt .sidekick.toml && cargo fmt
```

[protocol buffer compiler installation]: https://protobuf.dev/installation/
[set up development environment]: /doc/contributor/howto-guide-set-up-development-environment.md
