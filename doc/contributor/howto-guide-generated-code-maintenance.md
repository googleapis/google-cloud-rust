# How-To Guide: Generated Code Maintenance

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to generate a new library, update libraries
with new changes in the proto specifications, and refresh the generated code
when the generator changes.

## Prerequisites

The generator uses the protobuf compiler, `protoc`, with the `--retain_options`
flag.

Ensure that you have `protoc` installed and that its version is >= v23.0.

```bash
protoc --version
```

If not, follow the steps in [Protocol Buffer Compiler Installation] to download
a suitable version.

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
provide the source and output directories to the `generate` subcommand.
We will use `google/api` as an example.

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

## Update the code with new googleapis protos

Run:

```bash
go -C generator/ run ./cmd/sidekick update -project-root .. && taplo fmt .sidekick.toml && cargo fmt
```

Then run the unit tests and send a PR with whatever changed.

## Refreshing the code

Run:

```bash
go -C generator/ run ./cmd/sidekick refreshall -project-root .. && cargo fmt
```

Then run the unit tests and send a PR with whatever changed.

## Update golden files

The golden files live under `generator/testdata`.

Run:

```bash
go -C generator test ./...
```

After updating the golden files, make sure to run `git diff generator/testdata`
to verify the changes before committing them.

## The Glorious Future

Someday `sidekick` will be stable enough that (a) it will not be part of the
`google-cloud-rust` repository, and we will be able to install it. At that
point we will be able to say:

```bash
go install github.com/googleapis/google-cloud-generator/sidekick@v1.0.0
```

And we will be able to issue shorter commands, such as:

```bash
sidekick update && taplo fmt .sidekick.toml && cargo fmt
```

[protocol buffer compiler installation]: https://protobuf.dev/installation/
