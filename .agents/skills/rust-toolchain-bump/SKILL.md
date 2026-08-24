---
name: rust-toolchain-bump
description: Checks for new stable Rust minor releases (e.g. 1.98.0), updates local stable toolchain, runs strict workspace clippy verification, and updates CI configurations across .gcb/*.yaml and GitHub Actions workflows in google-cloud-rust.
---

# Stable Rust Toolchain Bump (`google-cloud-rust`)

This skill automates checking for new stable minor compiler releases from the Rust project (released every 6 weeks), verifying workspace clippy lints, and synchronizing compiler version definitions across CI configurations.

> [!NOTE]
> Bumping the Minimum Supported Rust Version (MSRV) is managed independently under the 1-year policy.

---

## Step 1: Determine Current Stable Compiler Version

1. Check current compiler version configured in GitHub Actions:
   - Search `.github/workflows/sdk.yaml` for `GHA_RUST_VERSIONS` (or check `CURRENT_RUST_VERSION` in `.github/workflows/rust-toolchain-check.yaml`).
2. Let `1.YY` be the current minor version (e.g. `1.97`).

---

## Step 2: Check Latest Stable Rust Release

1. Query recent releases from the official changelog:
   ```bash
   curl -sSL https://raw.githubusercontent.com/rust-lang/rust/master/RELEASES.md | head -n 30
   ```
2. **Evaluate Release**:
   - **Minor Releases (`1.XX.0`)**: Action required if `1.XX` > `1.YY`.
   - **Patch Releases (`1.XX.Y`)**: No action required per team policy (only minor versions are pinned).

---

## Step 3: Update Toolchain & Run Lints

1. Update the local stable Rust toolchain (requires network access / `BypassSandbox: true`):
   ```bash
   rustup update stable
   rustc --version
   ```
2. Run strict workspace clippy verification:
   ```bash
   cargo clippy --all-features --all-targets --profile=test --workspace -- --deny warnings
   ```
3. Test semver checks tooling against the new compiler:
   ```bash
   cargo semver-checks --all-features -p google-cloud-wkt
   ```
   *If this fails with `unsupported rustdoc format vXX`, bump `cargo-semver-checks` to the latest version in both `.gcb/scripts/semver-checks.sh` and `librarian.yaml`.*

4. **Handle Linter Diagnostics**:
   - **Zero Warnings**: Proceed directly to Step 4.
   - **Warnings in Handwritten Crates (`src/auth`, `src/gax`, `src/storage`, etc.)**: Fix code diagnostics directly in the working branch alongside the version updates.
   - **Warnings in Generated Code (`src/generated/`)**: Do NOT edit generated files manually. Stop and file an issue/PR to update generator templates in `librarian` first.

---

## Step 4: Update CI Configuration Files

Search for all occurrences of the old version `1.YY` across the repository to locate every CI configuration defining the compiler:

```bash
git grep "1\.YY"
```

> [!WARNING]
> Do NOT modify MSRV configurations: `.gcb/msrv.yaml` or `Cargo.toml` (`rust-version`). Those track the MSRV, which is managed independently under the 1-year policy.

1. **Update GitHub Actions Workflows**:
   - `.github/workflows/sdk.yaml` (`GHA_RUST_VERSIONS: '{ "rust:current": "1.XX" }'`)
   - `.github/workflows/rust-toolchain-check.yaml` (`CURRENT_RUST_VERSION: '1.XX'`)

2. **Update Google Cloud Build Configurations**:
   Update all `_RUST_VERSION: '1.YY'` entries across `.gcb/` and `src/**/.gcb/` (excluding `msrv.yaml`):
   - `.gcb/format.yaml`
   - `.gcb/complex.yaml`
   - `.gcb/cryptoproviders.yaml`
   - `.gcb/coverage.yaml`
   - `.gcb/integration.yaml`
   - `src/auth/.gcb/integration.yaml`

3. **Update Tooling Pins (if bumped in Step 3)**:
   - `.gcb/scripts/semver-checks.sh` (`cargo install --locked cargo-semver-checks@...`)
   - `librarian.yaml` (`tools.cargo` entry for `cargo-semver-checks`)

---

## Step 5: Validate and Prepare PR

1. Verify formatting and check builds:
   ```bash
   cargo fmt --check
   cargo check --workspace --all-targets
   ```
2. Create a feature branch and commit following `CONTRIBUTING.md#commit-messages`:
   ```bash
   git checkout -b chore-bump-rust-toolchain-1.XX
   git commit -m "chore(ci): update Rust toolchain to 1.XX" -m "Update stable compiler version to 1.XX across GitHub Actions and Google Cloud Build configurations."
   ```
