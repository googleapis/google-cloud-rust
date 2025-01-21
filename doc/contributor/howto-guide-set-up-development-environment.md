# Howto-Guide: Set Up Development Environment

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to set up your development workstation to
compile the code, run the unit tests, and formatting miscellaneous files.

## Installing Rust

We recommend that you follow the [Getting Started][getting-started-rust] guide.
Once you have `cargo` and `rustup` installed the rest is relatively easy.

## Installing Go

The code generator is implemented in [Go](https://go.dev). Follow the
[Download and install][golang-install] guide to install golang.

## IDE Recommendations

Whatever works for you. Several team members use Visual Studio Code, but Rust
can be used with many IDEs.

## Compile the Code

Just use cargo:

```bash
cargo build
```

## Running the unit tests

```bash
cargo test
```

## Running lints and unit tests

```bash
cargo fmt && cargo clippy -- --deny warnings && cargo test
git status # Shows any diffs created by `cargo fmt`
```

## Getting code coverage locally

### Install coverage tools (once)

You will need to install `cargo-tarpaulin`:

```bash
cargo install cargo-tarpaulin
```

On macOS you need to enable an extra feature:

```bash
cargo install cargo-tarpaulin --features vendored-openssl
```

### Getting coverage in cobertura format

```bash
cargo tarpaulin --out xml
```

If you prefer to exclude generated code:

```bash
cargo tarpaulin --out xml \
  --exclude-files 'generator/**' \
  --exclude-files 'src/generated/**' \
  --exclude-files 'src/integration-tests/**' \
  --exclude-files 'src/wkt/src/generated/**'
```

## Integration tests

This guide assumes you are familiar with the [Google Cloud CLI], you have access
to an existing Google Cloud Projects, and have enough permissions on that
project.

### One time set up

We use [Secret Manager] to run integration tests. Follow the
[Enable the Secret Manager API] guide to, as it says, enable the API and make
sure that billing is enabled in your projects.

Verify this is working with something like:

```bash
gcloud secrets list
```

It is fine if the list is empty, you just don't want an error.

The integration tests need a service account in your project. This service
account is used to:

- Run test that perform IAM operations, temporarily granting this service
  account some permissions.
- Configure the service account used for test workflows.

For a test project, just create the SA using the CLI:

```bash
gcloud iam service-accounts create rust-sdk-test \
    --display-name="Used in SA testing" \
    --description="This SA gets assigned to roles on short-lived resources during integration tests"
```

For extra safety, disable the service account:

```bash
GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
gcloud iam service-accounts disable rust-sdk-test@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com
```

### Running

Use `cargo test` to run the tests. The `run-integration-tests` features enables
running the integration tests. The default is to only run unit tests:

```bash
GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
env \
    GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT=rust-sdk-test@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com \
    GOOGLE_CLOUD_RUST_TEST_WORKFLOWS_RUNNER=rust-sdk-test@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com \
    GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT} \
  cargo test --features run-integration-tests --package integration-tests
```

There are (at the moment) six integration tests. All using secret manager. We
test the OpenAPI-generated client, the OpenAPI-generated client with locational
endpoints, and the Protobuf generated client. For each version we run the tests
with logging enabled and with logging disabled.

## Miscellaneous Tools

We use a number of tools to format non-Rust code. The CI builds enforce
formatting, you can fix any formatting problems manually (using the CI logs),
or may prefer to install these tools locally to fix formatting problems.

Typically we do not format these files for generated code, so local runs
requires skipping the generated files.

### Format TOML files

We use `taplo` to format the hand-crafted TOML files. Install with:

```bash
cargo install taplo-cli
```

use with:

```bash
git ls-files -z -- '*.toml' ':!:**/testdata/**' ':!:src/generated/**' | xargs -0 taplo fmt
```

### Detect typos in comments and code

We use `typos` to detect typos. Install with:

```bash
cargo install typos-cli
```

### Format Markdown files

We use `mdformat` to format hand-crafted markdown files. Install with:

```bash
python -m venv .venv
source .venv/bin/activate # Or whatever is the right command for you shell
pip install -r ci/requirements.txt
```

use with:

```bash
git ls-files -z -- '*.md' ':!:**/testdata/**' | xargs -0 -r -P "$(nproc)" -n 50 mdformat
```

### Format YAML files

We use `yamlfmt` to format hand-crafted YAML files (mostly GitHub Actions).
Install and use with:

```bash
go install github.com/google/yamlfmt/cmd/yamlfmt@v0.13.0
```

use with:

```bash
git ls-files -z -- '*.yaml' '*.yml' ':!:**/testdata/**' | xargs -0 yamlfmt
```

[enable the secret manager api]: docs/configuring-secret-manager
[getting-started-rust]: https://www.rust-lang.org/learn/get-started
[golang-install]: https://go.dev/doc/install
[google cloud cli]: https://cloud.google.com/cli
[secret manager]: https://cloud.google.com/secret-manager/
