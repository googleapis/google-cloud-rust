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

First define the library's name.  Note this should match the directory path where the code lives delimited by "-" e.g. google-cloud-gax-internal:

```bash
library=... 
```
Create a new branch in your fork:

```bash
git checkout -b feat-${library}-generate-library
```

This command will generate the library, add the library to Cargo and git, and
run the necessary tests:

```bash
V=$(cat .librarian-version.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} create ${library}
```

Commit all these changes and send a PR to merge them:

```bash
git commit -m "feat(${library}): generate library"
```

## Update the code with new googleapis protos

Run:

```bash
git checkout -b chore-update-googleapis-sha-circa-$(date +%Y-%m-%d)
V=$(cat .librarian-version.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all
git commit -m"chore: update googleapis SHA circa $(date +%Y-%m-%d)" .
```

Then send a PR with whatever changed.

## Update the code with new discovery docs

For now you have to manually update the commit and sha256 in librarian.yaml under sources >> discovery

```
sources:
  discovery:
    commit: f85002671045f2d315ffcc140a8dc5ea8eb35769
    sha256: 5032016f41ba2d3f42536008fd9d67cd3ff4604f28822b2dab457e94cde16b98
```

## Bump all version numbers

Manually bump the version of `google-cloud-gax-internal`, if necessary, in:

- `Cargo.toml`
- `src/gax-internal/Cargo.toml`

Run:

```bash
git fetch upstream
git checkout -b chore-bump-version-numbers-circa-$(date +%Y-%m-%d)
V=$(cat .librarian.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} release --all
git add Cargo.lock '*Cargo.toml' '*README.md'
git restore . # Effectively a `cargo fmt`, but much faster.
git commit -m"chore: bump version numbers circa $(date +%Y-%m-%d)"
```

When running on Cloudtop, you might need to set
`CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt` in order for crates.io to
accept your certs.

## Refreshing the code

### All libraries

Run:

```bash
V=$(cat .librarian-version.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all 
```

Then run the unit tests and send a PR with whatever changed.

### Single library

When iterating, it can be useful to regenerate the code of a single library.
Get the library name from librarian.yaml.

Run:

```bash
V=$(cat .librarian-version.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate google-cloud-secretmanager-v1
```

## The Glorious Future

Someday `librarian` will be stable enough that we will be able to install it. At
that point we will be able to say:

```bash
V=$(cat .librarian-version.txt)
go install github.com/googleapis/librarian/cmd/librarian@${V}
```

And we will be able to issue shorter commands, such as:

```bash
librarian generate --all
```

## Special cases

### Making changes to `librarian`

Clone the `librarian` directory:

```bash
git -C .. clone git@github.com:googleapis/librarian
git -C ../librarian checkout -b fancy-rust-feature
```

You can make changes in the `librarian` directory as usual. To test them change
the normal commands to use that directory. For example:

```bash
go install ./cmd/librarian
cd {project root or use another terminal)
librarian generate --all
```

Once the changes work then send a PR in the librarian repo to make your changes.
Wait for the PR to be approved and merged.

Then finish your PR in `google-cloud-rust`.

1. Update the default librarian version:

   ```bash
   GOPROXY=direct go list -m -u -f '{{.Version}}' github.com/googleapis/librarian@main >.librarian-version.txt
   ```

1. Update the generated code:

   ```bash
   V=$(cat .librarian-version.txt)
   go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all
   ```

Use a single PR to update the librarian version and any generated code.

### Generating a library with customized directories

We may need to customize the target or source directory for some generated
libraries. For example, you may need to leave room for other crates in the same
directory. 

1. Update the librarian.yaml with the correct configuration.  

```
output: custom directory to generate code in
```

```
channels > path: custom path to read protos from in googleapis
```

example:
```
  - name: google-cloud-api
    version: 1.2.0
    channels:
      - path: google/api
    copyright_year: "2025"
    output: src/generated/api/types
```

2. run generate

```
bash
V=$(cat .librarian-version.txt)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate google-cloud-apps-script-type
```

3. Add the files to `git`, compile them, and run the tests:

```bash
typos && cargo fmt && cargo build && cargo test && cargo doc
git add src/generated/cloud/api/types Cargo.toml Cargo.lock
```

4. Commit all these changes and send a PR to merge them:

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

Now add the library back (get the library name from librarian yaml):

```shell
go run github.com/googleapis/librarian/cmd/librarian@main generate google-cloud-websecurityscanner-v1
```

[protocol buffer compiler installation]: https://protobuf.dev/installation/
[set up development environment]: /doc/contributor/howto-guide-set-up-development-environment.md
