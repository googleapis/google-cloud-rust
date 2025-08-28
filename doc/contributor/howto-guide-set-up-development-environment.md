# Howto-Guide: Set Up Development Environment

This guide is intended for contributors to the `google-cloud-rust` SDK. It will
walk you through the steps necessary to set up your development workstation to
compile the code, run the unit tests, and formatting miscellaneous files.

## Installing Rust

We recommend that you follow the [Getting Started][getting-started-rust] guide.
Once you have `cargo` and `rustup` installed the rest is relatively easy.

You will need rust >= 1.87 (released around 2025-05-15). Check the version you
have installed with:

```shell
rustc --version
```

If you need to upgrade, consider:

```shell
rustup update
```

## Installing Go

The code generator is implemented in [Go](https://go.dev). Follow the
[Download and install][golang-install] guide to install Golang.

## IDE Recommendations

Whatever works for you. Several team members use Visual Studio Code, but Rust
can be used with many IDEs.

### Recommended VS Code Configuration

The default configuration for VS Code is to `cargo check` all the code when you
save a file. As the project is rather large (almost 200 crates, over 1 million
lines of code), this can be rather slow. We recommend you override these
defaults in your `settings.json` file:

```json
{
    "rust-analyzer.cargo.buildScripts.overrideCommand": [
        "cargo",
        "check",
        "--quiet",
        "--profile=test",
        "--message-format=json",
        "--keep-going"
    ],
    "rust-analyzer.check.workspace": false
}
```

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
cargo fmt && cargo clippy --profile=test -- --deny warnings && cargo test
git status # Shows any diffs created by `cargo fmt`
```

If you are seeing errors when running locally that are not present in the CI,
you may need to update your local rust version.

## Generating the user guide

We use [mdBook] to generate a user guide: a series of short "how-to" documents.
Install the tool using `cargo install`:

```bash
cargo install mdbook
```

Then generate the documents with `mdbook build`:

```bash
mdbook build guide
```

You will find the generated book in `guide/book`. You can also test any code
snippets in the documentation using:

```bash
mdbook test guide
```

Some of the samples are integration tests, you can verify they build using:

```bash
cargo build --package user-guide-samples
```

and verify they run using the instructions in the
[Integration Tests](#integration-tests) section.

To format the example code in the user guide, run the following command:

```bash
cargo fmt -p user-guide-samples
```

### Using `mdbook serve`

If you are working on the user guide you may find this handy:

```bash
mdbook serve guide
```

This will serve the documentation on a local HTTP server (usually at
`http://localhost:3000/`). It will also automatically rebuild the documentation
as you modify it.

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

Generated code and test helpers are excluded by default. The configuration is in
the top level `.tarpaulin.toml`.

## Integration tests

This guide assumes you are familiar with the [Google Cloud CLI], you have access
to an existing Google Cloud Project, and have enough permissions on that
project.

### One time set up

We use [Secret Manager], [Workflows], [Firestore], [Speech-to-Text], and \[KMS\]
to run integration tests. Follow the [Enable the Secret Manager API] guide to,
as it says, enable the API and make sure that billing is enabled in your
projects. To enable the APIs you can run this command:

```bash
gcloud services enable workflows.googleapis.com firestore.googleapis.com speech.googleapis.com cloudkms.googleapis.com
```

Verify this is working with something like:

```bash
gcloud firestore databases list
gcloud secrets list
gcloud workflows list
```

It is fine if the list is empty, you just don't want an error.

### Create a service account

The integration tests need a service account (SA) in your project. This service
account is used to:

- Run tests that perform IAM operations, temporarily granting this service
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

### Create a database

The integration tests need the default Firestore database in your project. You
can create this database using:

```bash
gcloud firestore databases create --location=us-central1
```

If the database already exists you should verify that this is a Firestore native
database:

```bash
gcloud firestore databases describe --format='value(type)'
# Expected output:
# FIRESTORE_NATIVE
```

### Create a KMS key ring a crypto key

We use KMS keys with storage. The [Use customer-managed encryption keys] guide
covers how create and configure a crypto key for use with Cloud Storage. We
recommend you name the key ring after its location, but feel free to use a
different naming convention:

```bash
GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
gcloud kms keyrings create us-central1 --location=us-central1
gcloud kms keys create storage-examples \
    --keyring=us-central1 \
    --location=us-central1 \
    --purpose=encryption \
    --rotation-period=10d --next-rotation-time=+p10d
gcloud storage service-agent --project=${GOOGLE_CLOUD_PROJECT} \
    --authorize-cmek=projects/${GOOGLE_CLOUD_PROJECT}/locations/us-central1/keyRings/us-central1/cryptoKeys/storage-examples
```

### Running tests

Use `cargo test` to run the tests. The `run-integration-tests` features enables
running the integration tests. The default is to only run unit tests:

```bash
GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
env \
    GOOGLE_CLOUD_RUST_TEST_SERVICE_ACCOUNT=rust-sdk-test@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com \
    GOOGLE_CLOUD_RUST_TEST_WORKFLOWS_RUNNER=rust-sdk-test@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com \
    GOOGLE_CLOUD_RUST_TEST_STORAGE_KMS_RING=us-central1 \
    GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT} \
  cargo test --features run-integration-tests --package integration-tests --package user-guide-samples
```

Optionally, add the feature `log-integration-tests` to the test command to log
tracing information.

There are (at the moment) six integration tests. All using secret manager. We
test the OpenAPI-generated client, the OpenAPI-generated client with locational
endpoints, and the Protobuf generated client. For each version we run the tests
with logging enabled and with logging disabled.

## Miscellaneous Tools

We use a number of tools to format non-Rust code. The CI builds enforce
formatting, you can fix any formatting problems manually (using the CI logs), or
may prefer to install these tools locally to fix formatting problems.

Typically we do not format these files for generated code, so local runs
requires skipping the generated files.

### Format TOML files

We use `taplo` to format the hand-crafted TOML files. Install with:

```bash
cargo install taplo-cli
```

use with:

```bash
git ls-files -z -- \
    '*.toml' ':!:**/testdata/**' ':!:**/generated/**' | \
    xargs -0 taplo fmt
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
source .venv/bin/activate # Or whatever is the right command for your shell
pip install -r ci/requirements.txt
```

use with:

```bash
git ls-files -z -- \
    '*.md' ':!:**/testdata/**' ':!:**/generated/**' | \
    xargs -0 mdformat
```

### Format YAML files

We use `yamlfmt` to format hand-crafted YAML files (mostly GitHub Actions).
Install and use with:

```bash
go install github.com/google/yamlfmt/cmd/yamlfmt@v0.13.0
```

use with:

```bash
git ls-files -z -- \
    '*.yaml' '*.yml' ':!:**/testdata/**' ':!:**/generated/**' | \
    xargs -0 yamlfmt
```

### Format Terraform files

We use `terraform` to format `.tf` files. You will rarely have any need to edit
these files. If you do, you probably know how to [install terraform].

Format the files using:

```bash
git ls-files -z --
    '*.tf' ':!:**/testdata/**' ':!:**/generated/**' | \
    xargs -0 terraform fmt
```

[enable the secret manager api]: https://cloud.google.com/secret-manager/docs/configuring-secret-manager
[firestore]: https://cloud.google.com/firestore/
[getting-started-rust]: https://www.rust-lang.org/learn/get-started
[golang-install]: https://go.dev/doc/install
[google cloud cli]: https://cloud.google.com/cli
[install terraform]: https://developer.hashicorp.com/terraform/install
[mdbook]: https://rust-lang.github.io/mdBook/
[secret manager]: https://cloud.google.com/secret-manager/
[speech-to-text]: https://cloud.google.com/speech-to-text
[use customer-managed encryption keys]: https://cloud.google.com/storage/docs/encryption/using-customer-managed-keys
[workflows]: https://cloud.google.com/workflows/
