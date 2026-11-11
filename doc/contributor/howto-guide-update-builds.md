# Howto-Guide: Update CI builds on Google Cloud Build

This guide is intended for contributors to the `google-cloud-rust` SDK. It
describes how to test and configure new CI builds and integration test resources
on GCB (Google Cloud Build).

## Create new builds

Most CI builds on GCB use [`.gcb/complex.yaml`](../../.gcb/complex.yaml), which
sets up `sccache` (backed by Cloud Storage), fetches Cargo dependencies into a
shared volume, and delegates the build steps to `.gcb/scripts/${_SCRIPT}.sh`
inside `rust:bookworm` using the toolchain pinned in `rust-toolchain.toml`.

Suppose you want to add a new CI build, such as testing the feature matrix of a
handwritten crate like `google-cloud-spanner`. Follow these steps:

### Create a new build script in `.gcb/scripts`

Create a new script (or copy an existing one such as
`.gcb/scripts/features-gax-internal.sh` or `.gcb/scripts/test.sh`) and customize
the Cargo commands:

```bash
#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ev

export RUSTDOCFLAGS="${RUSTDOCFLAGS:-} -D warnings"

rustup component add clippy
cargo version
rustup show active-toolchain -v

cargo clean
echo "==== google-cloud-spanner ===="
cargo test --profile=ci --package google-cloud-spanner --no-default-features
cargo test --profile=ci --package google-cloud-spanner --all-features
cargo clippy --profile=ci --no-deps --package google-cloud-spanner --all-features --all-targets -- --deny warnings

echo "==== DONE ===="

/workspace/.bin/sccache --show-stats
```

Follow these script conventions:

1. Include the `#!/usr/bin/env bash` shebang and the Apache 2.0 copyright
   header.

1. Install any extra `rustup` components your script needs (such as `clippy` or
   `rustfmt`) and print `cargo version` and `rustup show active-toolchain -v` to
   aid debugging.

1. End with `/workspace/.bin/sccache --show-stats` so build logs report compiler
   cache hit rates.

1. Make sure the new script is executable, because `.gcb/complex.yaml` invokes
   it directly:

   ```shell
   chmod 755 .gcb/scripts/features-spanner.sh
   ```

> [!NOTE]
> If your build requires additional Docker containers or emulators (such as
> `.gcb/integration.yaml`), Secret Manager secrets (such as
> `.gcb/coverage.yaml`), or non-Rust builder images (such as
> `.gcb/format.yaml`), create a dedicated `.gcb/<name>.yaml` configuration
> instead of using `complex.yaml`. Any new `.yaml` file must include the Apache
> 2.0 copyright header (verified by `check-copyright`) and be formatted with
> `yamlfmt`.

### Test the new script on GCB

Run the build directly on GCB before opening or merging a pull request. You may
need to iterate on the script until it passes reliably:

```sh
gcloud builds submit --project=rust-sdk-testing --region=us-central1 \
  --config=.gcb/complex.yaml --substitutions=_SCRIPT=features-spanner
```

Note the following when testing with `gcloud builds submit`:

- **Worker pool**: `.gcb/complex.yaml` defaults `_POOL_ID` to `rust-sdk-pool`
  (`e2-standard-8`). If your build needs more CPU or memory, pass the larger
  `e2-standard-32` pool via `--substitutions`:
  ```sh
  gcloud builds submit --project=rust-sdk-testing --region=us-central1 \
    --config=.gcb/complex.yaml \
    --substitutions=_SCRIPT=features-spanner,_POOL_ID=rust-sdk-pool-large
  ```
- **Unstable `cfg` flags**: Pass `_UNSTABLE_CFG_FLAGS` in `--substitutions` if
  your build depends on `RUSTFLAGS` / `RUSTDOCFLAGS` `--cfg` flags.
- **Ignored local files**: [`.gcloudignore`](../../.gcloudignore) includes
  `.gitignore`, so local `target/` build artifacts are automatically excluded
  from the uploaded source archive.

### Edit the trigger files

Edit [`.gcb/builds/triggers/main.tf`](../../.gcb/builds/triggers/main.tf) to
register the new build trigger. Choose the appropriate map in `locals`:

- `common_builds`: creates both a Pull Request trigger (`gcb-pr-<name>`) and a
  Post-Merge trigger (`gcb-pm-<name>`).
- `pr_build_overrides`: builds or overrides that only run on Pull Requests.
- `pm_build_overrides`: builds that only run Post-Merge (used for slow,
  exhaustive jobs such as `workspace`, `workspace-minimal-versions`, and
  `ignored-samples`).

For example, adding `features-spanner` to `common_builds` looks like this:

```diff
diff --git a/.gcb/builds/triggers/main.tf b/.gcb/builds/triggers/main.tf
--- a/.gcb/builds/triggers/main.tf
+++ b/.gcb/builds/triggers/main.tf
@@ -92,6 +92,11 @@ locals {
       script = "features-gax-internal"
       pool   = "rust-sdk-pool-large"
     }
+    features-spanner = {
+      config = "complex.yaml"
+      script = "features-spanner"
+      pool   = "rust-sdk-pool-large"
+    }
     per-client-features = {
       config = "complex.yaml"
       script = "per-client-features"
```

Each build entry supports the following attributes:

- `config` (required): build configuration file under `.gcb/` (usually
  `"complex.yaml"`).
- `script` (optional): script basename under `.gcb/scripts/` without `.sh`
  (mapped to `_SCRIPT`).
- `pool` (optional): worker pool name. Set to `"rust-sdk-pool-large"` for
  `e2-standard-32` machines, or omit to use the default `"rust-sdk-pool"`
  (`e2-standard-8`).
- `flags` (optional): `RUSTFLAGS` and `RUSTDOCFLAGS` values (mapped to
  `_UNSTABLE_CFG_FLAGS`), such as `local.unstable_flags` or
  `local.tokio_unstable_flags`.
- `rust_version` (optional): Rust toolchain override mapped to `_RUST_VERSION`
  (used by `msrv.yaml`).
- `included_files` (optional, `pm_build_overrides` only): list of file globs
  that trigger the post-merge build (for example, `["**/generated/**"]`).

Format any modified Terraform files before committing so `.gcb/format.yaml`
passes in CI:

```shell
terraform fmt .gcb/builds/triggers/main.tf
```

### Send a PR to add the new files

Open a pull request with the new build script and the
`.gcb/builds/triggers/main.tf` change, and get it reviewed and merged.

> [!IMPORTANT]
> **Do not** run `terraform apply` before the PR is merged into `main`. Once a
> `gcb-pr-<name>` trigger is enabled in Cloud Build, it runs immediately on pull
> requests targeting `main`. Enabling it before the script is on `main` will
> break other open pull requests that do not yet contain the new script.

### Enable the new trigger

Once the PR is merged into `main`, and **not before**, enable the trigger by
applying the Terraform configuration in `.gcb/builds`:

```shell
cd .gcb/builds
terraform init
terraform plan -out /tmp/build.tfplan
terraform apply /tmp/build.tfplan
```

Make sure you do **not** have a local `backend_override.tf` file in
`.gcb/builds` when applying changes to the shared `rust-sdk-testing` project.

> [!NOTE]
> A scheduled Cloud Build trigger (`gcb-pubsub-terraform`, defined in
> `.gcb/terraform.yaml`) also runs `terraform apply` automatically against
> `main` once a week on Sundays at midnight UTC. Running `terraform apply`
> manually after merging activates the new trigger immediately and confirms the
> plan succeeds.

## Update resources for new integration tests

Integration test infrastructure for `rust-sdk-testing` is managed with Terraform
under `.gcb/builds/`:

- **APIs**: Enable required Google Cloud services in
  [`.gcb/builds/services/main.tf`](../../.gcb/builds/services/main.tf).
- **Persistent test resources**: Configure buckets, KMS keys, Firestore
  databases, or worker pools in
  [`.gcb/builds/resources/main.tf`](../../.gcb/builds/resources/main.tf).
- **IAM permissions**: Grant roles to the `integration-test-runner` service
  account in [`.gcb/builds/grants/main.tf`](../../.gcb/builds/grants/main.tf).
  If an integration test needs to create or delete project-level resources or
  external service accounts, you may also need to update the project
  configuration in overground.

To test new resources safely in a **personal** GCP project before modifying the
shared testing project, follow the `backend_override.tf` instructions in
[`.gcb/builds/README.md`](../../.gcb/builds/README.md).

Once your changes are ready, send and merge a PR with the `.tf` updates, then
apply them to `rust-sdk-testing`:

```shell
cd .gcb/builds
terraform init
terraform plan -out /tmp/build.tfplan
terraform apply /tmp/build.tfplan
```

Authentication integration tests maintain a separate GCB and Terraform
configuration targeting the `rust-auth-testing` project under
[`src/auth/.gcb/`](../../src/auth/.gcb/); follow the same workflow in
`src/auth/.gcb/builds` when updating auth-specific builds or test resources.
