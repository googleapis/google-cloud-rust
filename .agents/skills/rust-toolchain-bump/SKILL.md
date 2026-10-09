---
name: rust-toolchain-bump
description: Checks for new stable Rust minor releases (e.g. 1.98), updates the channel in rust-toolchain.toml, runs strict workspace clippy verification, and applies rustfmt changes in google-cloud-rust.
---

# Stable Rust Toolchain Bump (`google-cloud-rust`)

This skill automates checking for new stable minor compiler releases from the
Rust project (released every 6 weeks), updating the toolchain pinned in
`rust-toolchain.toml`, and verifying workspace clippy lints.

`rust-toolchain.toml` is the single source of truth for the stable compiler.
Local development, GitHub Actions, and Google Cloud Build all install the
toolchain pinned there. Only the minor version (`1.XX`) is pinned, so patch
releases do not require a bump.

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

1. Read the current version from `channel` in `rust-toolchain.toml`.
1. Compare against the latest stable release in `RELEASES.md`.
1. Check out a feature branch (`chore-bump-rust-toolchain-1.XX`) based on
   `main`.
1. Update `channel` in `rust-toolchain.toml` and install that toolchain
   (`rustup toolchain install`).
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

- **If the new `rustfmt` formats code differently:**

  - Run `cargo fmt` and commit the changes, including any changes to generated
    code. The generator formats its output with `cargo fmt`, so the result
    matches what it would produce.

______________________________________________________________________

## Step 3: Validate and Prepare PR

No CI configuration files need to be updated: the `channel` in
`rust-toolchain.toml` is the only version that changes.

> [!WARNING]
> Do NOT modify MSRV configurations: `.gcb/msrv.yaml` or `Cargo.toml`
> (`rust-version`). Those track the MSRV, which is managed independently under
> the 1-year policy.

1. Verify formatting and check builds:
   ```bash
   cargo fmt --check
   cargo check --workspace --all-targets
   ```
1. Commit all changes following `CONTRIBUTING.md#commit-messages`:
   ```bash
   git commit -am "chore: update Rust toolchain to 1.XX" -m "Update the stable compiler version in rust-toolchain.toml to 1.XX."
   ```
