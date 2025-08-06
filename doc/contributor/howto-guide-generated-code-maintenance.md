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

Make sure your workstation has up-to-date versions of Rust and Go. Follow the
instructions in [Set Up Development Environment].

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
go run github.com/googleapis/librarian/cmd/sidekick@main rust-generate \
    -service-config ${yaml}
```

Commit all these changes and send a PR to merge them:

```bash
git commit -m "feat(${library}): generate library"
```

## Update the code with new googleapis protos

Run:

```bash
git checkout -b chore-update-googleapis-sha-circa-$(date +%Y-%m-%d)
go run github.com/googleapis/librarian/cmd/sidekick@main update && taplo fmt .sidekick.toml && cargo fmt
git commit -m"chore: update googleapis SHA circa $(date +%Y-%m-%d)" .
```

Then send a PR with whatever changed.

## Refreshing the code

### All libraries

Run:

```bash
go run github.com/googleapis/librarian/cmd/sidekick@main refreshall && cargo fmt
```

Then run the unit tests and send a PR with whatever changed.

### Single library

When iterating, it can be useful to regenerate the code associated with a single
`.sidekick.toml`.

Run:

```bash
go run github.com/googleapis/librarian/cmd/sidekick@main refresh \
    -output src/generated/cloud/secretmanager/v1 && \
    cargo fmt -p google-cloud-secretmanager-v1
```

## The Glorious Future

Someday `sidekick` will be stable enough that we will be able to install it. At
that point we will be able to say:

```bash
go install github.com/googleapis/librarian/sidekick@v0.1.1
```

And we will be able to issue shorter commands, such as:

```bash
sidekick update && taplo fmt .sidekick.toml && cargo fmt
```

## Special cases

### Making changes to `sidekick`

Clone the `librarian` directory:

```bash
git -C .. clone git@github.com:googleapis/librarian
git -C ../librarian checkout -b fancy-rust-feature
```

You can make changes in the `librarian` directory as usual. To test them change
the normal commands to use that directory. For example:

```bash
go -C ../librarian run ./cmd/sidekick refreshall -project-root $PWD && cargo fmt
```

Once the changes work then send a PR in the librarian repo to make your changes.
Wait for the PR to be approved and merged. Then finish your PR in
`google-cloud-rust` by running sidekick again:

```bash
go run github.com/googleapis/librarian/cmd/sidekick@main refreshall && cargo fmt
```

Then update any references in this document and in the `.github/workflows/*`
files.

### Generating a library with customized directories

We may need to customize the target or source directory for some generated
libraries. For example, you may need to leave room for other crates in the same
directory. In this case you cannot use `rust-generate` and need to manually
provide the source and output directories to the `generate` subcommand. We will
use `google/api` as an example.

```bash
cargo new --lib --vcs none src/generated/api/types
taplo fmt Cargo.toml
go run github.com/googleapis/librarian/cmd/sidekick@main generate \
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
go run github.com/googleapis/librarian/cmd/sidekick@main rust-generate \
    -service-config google/cloud/websecurityscanner/v1/websecurityscanner_v1.yaml
```

[protocol buffer compiler installation]: https://protobuf.dev/installation/
[set up development environment]: /doc/contributor/howto-guide-set-up-development-environment.md
