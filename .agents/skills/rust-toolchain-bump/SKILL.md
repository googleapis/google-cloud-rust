---
name: rust-toolchain-bump
description: Checks for new stable Rust minor releases (e.g. 1.98.0), updates local stable toolchain, runs strict workspace clippy verification, and updates CI configurations across .gcb/*.yaml and GitHub Actions workflows in google-cloud-rust.
---

# Stable Rust Toolchain Bump (`google-cloud-rust`)

This skill automates checking for new stable minor compiler releases from the
Rust project (released every 6 weeks), verifying workspace clippy lints, and
synchronizing compiler version definitions across CI configurations.

> [!NOTE]
> Bumping the Minimum Supported Rust Version (MSRV) is managed independently
> under the 1-year policy.

______________________________________________________________________

## Step 1: Run Toolchain Check Script

Run the automated toolchain check script (requires network access /
`BypassSandbox: true`):

```bash
./scripts/check-rust-toolchain.sh
```

The script will:

1. Extract `CURRENT_RUST_VERSION` deterministically from
   `.github/workflows/rust-toolchain-check.yaml`.
1. Compare against the latest stable release in `RELEASES.md`.
1. Check out a feature branch (`chore-bump-rust-toolchain-1.XX`) based on
   `main`.
1. Update the local stable compiler (`rustup update stable`).
1. Attempt to automatically apply machine-applicable clippy suggestions
   (`cargo clippy --fix ...`).
1. Check if generated code (`**/generated/**`) was modified, failing early if
   action in `librarian` is required.
1. Run strict workspace clippy verification
   (`cargo clippy --all-features --all-targets --profile=test --workspace -- --deny warnings`).
1. Run `cargo semver-checks` on `google-cloud-wkt`.

______________________________________________________________________

## Step 2: Handle Diagnostics & Generated Code Changes

Check `git status` to inspect any changes made by automatic clippy fixes:

- **If `check-rust-toolchain.sh` exits successfully and ONLY handwritten crates
  (`src/auth`, `src/gax`, `src/storage`, etc.) were modified:**

  - Proceed directly to Step 3. These fixes will be included in the toolchain
    upgrade PR.

- **[CRITICAL] If `check-rust-toolchain.sh` fails because `cargo clippy --fix`
  modified generated code (`**/generated/**`):**

  - **Do NOT manually commit edits to generated files.**
  - Inspect the diff to see what needs to be updated in the code generator:
    ```bash
    git diff -- '**/generated/**'
    ```
  - **A separate PR is required first:** File an issue / PR in
    `googleapis/librarian` to update generator templates.
  - Discard local edits in generated directories before committing:
    ```bash
    git restore -- '**/generated/**'
    ```
  - Once the generator is updated and librarian regenerates the code in
    `google-cloud-rust`, resume the toolchain upgrade.

- **If `check-rust-toolchain.sh` fails on remaining warnings in handwritten
  crates:**

  - Fix code diagnostics directly in the working branch, then re-run
    `./scripts/check-rust-toolchain.sh` until clean.

- **If semver-checks fails with `unsupported rustdoc format vXX`:**

  - Bump `cargo-semver-checks` to the latest version in both
    `.gcb/scripts/semver-checks.sh` and `librarian.yaml`, then re-run.

______________________________________________________________________

## Step 3: Update CI Configuration Files

> [!NOTE]
> The list of CI configuration files below may change over time as new workflows
> or build steps are added. Always search the repository for the old compiler
> version number to ensure all references are found.

Search for all occurrences of the old compiler version across the repository
(escape the dot, e.g. `1\.97`):

```bash
git grep -n "1\.XX"
```

You can also search for key configuration variables:

```bash
git grep -n "_RUST_VERSION"
git grep -n "CURRENT_RUST_VERSION"
git grep -n "GHA_RUST_VERSIONS"
```

> [!WARNING]
> Do NOT modify MSRV configurations: `.gcb/msrv.yaml` or `Cargo.toml`
> (`rust-version`). Those track the MSRV, which is managed independently under
> the 1-year policy.

Common files to update include:

1. **GitHub Actions Workflows**:

   - `.github/workflows/sdk.yaml`
     (`GHA_RUST_VERSIONS: '{ "rust:current": "1.XX" }'`)
   - `.github/workflows/rust-toolchain-check.yaml`
     (`CURRENT_RUST_VERSION: '1.XX'`)

1. **Google Cloud Build Configurations**: Update all `_RUST_VERSION: '1.XX'`
   entries across `.gcb/` and `src/**/.gcb/` (excluding `msrv.yaml`):

   - `.gcb/format.yaml`
   - `.gcb/complex.yaml`
   - `.gcb/cryptoproviders.yaml`
   - `.gcb/coverage.yaml`
   - `.gcb/integration.yaml`
   - `src/auth/.gcb/integration.yaml`

______________________________________________________________________

## Step 4: Validate and Prepare PR

1. Verify formatting and check builds:
   ```bash
   cargo fmt --check
   cargo check --workspace --all-targets
   ```
1. Commit all changes following `CONTRIBUTING.md#commit-messages`:
   ```bash
   git commit -am "chore(ci): update Rust toolchain to 1.XX" -m "Update stable compiler version to 1.XX across GitHub Actions and Google Cloud Build configurations."
   ```
