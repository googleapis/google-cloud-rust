---
name: rust-toolchain-bump
description: Checks for new stable Rust minor releases (e.g. 1.98.0), updates local stable toolchain, runs strict workspace clippy verification, and updates CI configurations across .gcb/*.yaml and GitHub Actions workflows in google-cloud-rust.
---

# Stable Rust Toolchain Bump (`google-cloud-rust`)

This skill automates checking for new stable minor compiler releases from the Rust project (released every 6 weeks), verifying workspace clippy lints, and synchronizing compiler version definitions across CI configurations.

> [!NOTE]
> Bumping the Minimum Supported Rust Version (MSRV) is managed independently under the 1-year policy.

---

## Step 1: Run Toolchain Check Script

Run the automated toolchain check script (requires network access / `BypassSandbox: true`):

```bash
.agents/skills/rust-toolchain-bump/scripts/check-rust-toolchain.sh
```

The script will:
1. Extract `CURRENT_RUST_VERSION` deterministically from `.github/workflows/rust-toolchain-check.yaml`.
2. Compare against the latest stable release in `RELEASES.md`.
3. Update the local stable compiler (`rustup update stable`).
4. Run strict workspace clippy verification (`cargo clippy --all-features --all-targets --profile=test --workspace -- --deny warnings`).
5. Run `cargo semver-checks` on `google-cloud-wkt`.

---

## Step 2: Handle Diagnostics & Tooling Issues

* **If `check-rust-toolchain.sh` exits successfully:** Proceed directly to Step 3.
* **If clippy fails in handwritten crates (`src/auth`, `src/gax`, `src/storage`, etc.):** Fix code diagnostics directly in the working branch, then re-run `.agents/skills/rust-toolchain-bump/scripts/check-rust-toolchain.sh` until clean.
* **If clippy fails in generated code (`src/generated/`):** Do NOT edit generated files manually. Stop and file an issue/PR to update generator templates in `librarian` first.
* **If semver-checks fails with `unsupported rustdoc format vXX`:** Bump `cargo-semver-checks` to the latest version in both `.gcb/scripts/semver-checks.sh` and `librarian.yaml`, then re-run.

---

## Step 3: Update CI Configuration Files

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

---

## Step 4: Validate and Prepare PR

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
