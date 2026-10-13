# How-To Guide: Updating the Rust Toolchain

This guide describes how to update the Rust toolchain version in
`google-cloud-rust` when a new stable Rust compiler is released, and how Minimum
Supported Rust Version (MSRV) updates are managed.

## Overview

The repository tracks two distinct Rust version configurations:

1. **Current Stable Compiler (`rust-toolchain.toml`)**: Updated when the Rust
   project releases a new stable minor release (e.g. `1.97` -> `1.98`). Local
   development, CI checks, formatting, and clippy verification all use the
   toolchain pinned in this file. Only the minor version is pinned, so patch
   releases are picked up without changes.
1. **Minimum Supported Rust Version (MSRV)**: Managed independently under the
   **1-year policy** (`Cargo.toml` `rust-version = "1.XX.0"` and
   `.gcb/msrv.yaml`). Support for an MSRV release is dropped only after 1 year
   has passed since its initial release.

______________________________________________________________________

## Updating the Stable Compiler (6-Week Cycle)

When a new stable compiler is released (or when notified by the automated weekly
toolchain check issue), follow these steps to update the repository.

### Step 1: Run the Toolchain Check Script

Run the automated check script from the repository root:

```bash
./scripts/check-rust-toolchain.sh
```

The script will:

1. Compare the configured version against the latest release in `RELEASES.md`.
1. Automatically create and check out a branch `chore-bump-rust-toolchain-1.XX`
   based on `main`.
1. Update `channel` in `rust-toolchain.toml` and install that toolchain
   (`rustup toolchain install`).
1. Attempt to automatically apply machine-applicable clippy suggestions
   (`cargo clippy --fix`).
1. Verify that generated code was not modified.
1. Run strict workspace clippy verification (`--deny warnings`).
1. Run `cargo semver-checks` on `google-cloud-wkt`.

### Step 2: Resolve Diagnostics and Linter Warnings

Inspect `git status` after running the check script:

- **If the script exits successfully and only handwritten crates were
  modified:** Proceed to Step 3. These fixes will be committed alongside the
  `rust-toolchain.toml` update.
- **If the script fails because generated code (`**/generated/**`) was
  modified:** **Do not edit generated code manually.**
  1. Inspect the diff to see what changes the new compiler expects:
     ```bash
     git diff -- '**/generated/**'
     ```
  1. File an issue or send a PR in [librarian] to update the generator templates
     first.
  1. Discard local modifications to generated files:
     ```bash
     git restore -- '**/generated/**'
     ```
  1. Once the updated generator is published and regenerated in
     `google-cloud-rust`, resume the toolchain upgrade.
- **If clippy fails on remaining warnings in handwritten crates:** Fix the
  warnings manually in handwritten crates (`src/auth`, `src/gax`, `src/storage`,
  etc.) and re-run `./scripts/check-rust-toolchain.sh` until it passes.
- **If semver-checks fails with `unsupported rustdoc format vXX`:** Update
  `cargo-semver-checks` to the latest version in both
  `.gcb/scripts/semver-checks.sh` and `librarian.yaml`, then re-run.
- **If the new `rustfmt` formats code differently:** Run `cargo fmt` and commit
  the changes, including any changes to generated code. The generator formats
  its output with `cargo fmt`, so the result matches what it would produce.

### Step 3: Validate and Submit PR

The `channel` in `rust-toolchain.toml` is the only version that needs to change.
GitHub Actions and Google Cloud Build install the toolchain pinned in that file,
so no CI configuration files need to be updated.

> [!WARNING]
> Do NOT modify MSRV configurations: `.gcb/msrv.yaml` or `Cargo.toml`
> (`rust-version`). Those track the MSRV and are updated independently under the
> 1-year policy.

1. Verify formatting and workspace builds:
   ```bash
   cargo fmt --check
   cargo check --workspace --all-targets
   ```
1. Commit your changes following the commit message guidelines in
   [Contributing Guide]:
   ```bash
   git commit -am "chore: update Rust toolchain to 1.XX" -m "Update the stable compiler version in rust-toolchain.toml to 1.XX."
   ```

## Updating the Minimum Supported Rust Version (MSRV)

Under our MSRV policy, we can drop support for a Rust version **1 year after its
release date** (checked automatically weekly by
`.github/workflows/rust-toolchain-check.yaml` or manually at
[releases.rs](https://releases.rs/)).

To bump the MSRV, update `rust-version` in `Cargo.toml`.

Then search for the current MSRV version number across the repository and update
it everywhere else it is used (escape the dot, e.g., using the actual current
MSRV instead of 1.XX):

```bash
git grep -n "1\.XX\.0"
git grep -n "1\.XX"
```

Submit a PR with the MSRV update:

```bash
git checkout -b chore-bump-msrv-1.XX
git commit -am "chore(ci): update MSRV to 1.XX.0" -m "Update Minimum Supported Rust Version to 1.XX.0 across Cargo.toml, .clippy.toml, CI configurations, and documentation."
```

### Code Clean Ups

Consider applying any code cleanups or adopting standard library features
enabled by the newer MSRV. This is optional and not a required part of bumping
the version.

[contributing guide]: ../../CONTRIBUTING.md
[librarian]: https://github.com/googleapis/librarian
