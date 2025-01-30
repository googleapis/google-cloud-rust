# How-To Guide: Generated Code Maintenance

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to generate a new library, update libraries
with new changes in the proto specifications, and refresh the generated code
when the generator changes.

## Generate new library

We will use "Web Security Scanner" as an example, most likely you will generate
a different library, change the paths as needed. Start with a new branch in your
fork:

```bash
git checkout -b feat-websecurityscanner-generate-library
```

Create an empty library using `cargo`. The contents will be overwritten by
`sidekick`, but this automatically updates the top-level `Cargo.toml` file:

```bash
cargo new --lib --vcs none src/generated/cloud/websecurityscanner/v1
taplo fmt Cargo.toml
```

Generate the library:

```bash
go -C generator/ run ./cmd/sidekick generate \
    -project-root .. \
    -specification-source google/cloud/websecurityscanner/v1 \
    -service-config google/cloud/websecurityscanner/v1/websecurityscanner_v1.yaml \
    -output src/generated/cloud/websecurityscanner/v1
```

Add the files to `git`, compile them, and run the tests:

```bash
cargo fmt && cargo build && cargo test && cargo doc
git add src/generated/cloud/websecurityscanner/v1
```

Commit all these changes and send a PR to merge them.

```bash
git commit -m "feat(websecurityscanner): generate library"
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
