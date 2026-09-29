---
name: fix-issue
description: >-
  Use this skill to prepare a plan and implement a fix for an issue in google-cloud-rust
  (e.g., https://github.com/googleapis/google-cloud-rust/issues/NNNN or issue number NNNN).
  Enforces worktree isolation, adherence to GEMINI.md in google-cloud-rust and AGENTS.md in librarian,
  subagent PR review for librarian changes, a two-commit rule when code generator changes are involved,
  and commit titles strictly under 50 characters.
---

# Fix Issue Workflow for `google-cloud-rust`

This skill guides the agent through investigating, planning, and implementing
fixes for issues in
[`google-cloud-rust`](https://github.com/googleapis/google-cloud-rust).

It enforces:

1. **Worktree & Branch Isolation**: Always work in a dedicated worktree and
   branch.
1. **Project Guidelines**:
   - Follow [`GEMINI.md`](../../../GEMINI.md) and
     [`CONTRIBUTING.md`](../../../CONTRIBUTING.md) in `google-cloud-rust`.
   - Follow `AGENTS.md` in `librarian` whenever generator changes are involved.
1. **Subagent Code Review**: Use a subagent to run the `review-pr` skill on
   `librarian` changes.
1. **Two-Commit Rule for Librarian Changes**: Split generator output and Rust
   tests/edits into separate commits.
1. **Commit Title Constraint**: Keep all commit titles strictly **under 50
   characters**.

______________________________________________________________________

## Workflow Steps

### Step 1: Parse and Inspect the Issue

Extract the issue number (`<NNNN>`) from the user prompt or issue URL:

```bash
# Example: https://github.com/googleapis/google-cloud-rust/issues/1234 -> 1234
ISSUE_NUM="<NNNN>"
```

Inspect the issue using GitHub CLI:

```bash
gh issue view "${ISSUE_NUM}" --repo googleapis/google-cloud-rust --json number,title,body,labels,comments
```

Identify:

- The affected crate or component (e.g. `storage`, `pubsub`, `spanner`, `auth`,
  `gax`, or generated GAPIC crate under `src/generated/`).
- The root cause: Is it a bug in the code generator (`librarian`), or in
  hand-written code/tests in `google-cloud-rust`?
- Acceptance criteria and reproducer cases.

______________________________________________________________________

### Step 2: Worktree and Branch Setup in `google-cloud-rust`

Always perform work in a dedicated worktree and branch.

1. **Locate or Determine Worktree Location**:

   - Worktrees are typically created as siblings of `main`:
     `../fix-issue-${ISSUE_NUM}` (relative to `main`).
   - If ambiguous, ask the user.

1. **Create the Worktree and Branch**:

   ```bash
   BRANCH_NAME="fix-issue-${ISSUE_NUM}"
   WORKTREE_PATH="../${BRANCH_NAME}"

   # Fetch latest main and determine the base ref
   if git -C main fetch upstream main 2>/dev/null; then
     BASE_REF="upstream/main"
   else
     git -C main fetch origin main
     BASE_REF="origin/main"
   fi

   # Create worktree
   git -C main worktree add -b "${BRANCH_NAME}" "${WORKTREE_PATH}" "${BASE_REF}"
   ```

1. **Switch Context**: Perform all subsequent commands and modifications inside
   the newly created worktree.

______________________________________________________________________

### Step 3: Research and Formulate the Implementation Plan (`/plan`)

Before modifying code, research the problem and write an implementation plan
artifact adhering to the Jetski `/plan` standard.

#### Key Guidelines to Follow:

- **`google-cloud-rust` Rules**: Strictly follow
  [`GEMINI.md`](../../../GEMINI.md) and
  [`CONTRIBUTING.md`](../../../CONTRIBUTING.md):
  - **Formatting**: Must pass `cargo fmt --check`.
  - **Spell checking**: Must pass `typos`.
  - **Linting**:
    - Default build: `cargo clippy --all-targets -- -D warnings`
    - Tests build: `cargo clippy --profile test -- -D warnings`
    - Handwritten crates strict check: `cargo clippy-strict` (or crate-specific:
      `cargo clippy --all-features --no-deps -p <crate> -- -D missing_docs -D clippy::exhaustive_enums`)
  - **Testing**: Run unit tests with `cargo test -p ${crate_name}`.
  - **Async & Tokio**: Asynchronous RPCs and Tokio runtime conventions.
- **`librarian` Rules**: If the issue originates in the code generator, consult
  and follow `AGENTS.md` in `librarian`.

#### Check if Librarian Changes are Required:

- If code generator modifications are needed:
  1. Locate or create a worktree in `librarian` (sibling of `main` or in
     `librarian/worktrees/fix-issue-${ISSUE_NUM}`).
  1. Clearly document both the `librarian` changes and the `google-cloud-rust`
     changes in the plan.
  1. Note the two-commit structure and subagent review requirement in the plan.
- Create the plan artifact with `request_feedback: true` and obtain user
  approval before proceeding to implementation.

______________________________________________________________________

### Step 4: Implementation and Verification

#### Scenario A: Changes to `librarian` are Required

1. **Implement in `librarian` Worktree**:

   - Make the necessary changes in the generator following `librarian/AGENTS.md`
     and Go conventions.
   - Run Go checks and unit tests:
     ```bash
     gofmt -s -w .
     go tool goimports -w .
     go tool golangci-lint run
     go test -short ./...
     ```

1. **Test Generation Locally in `google-cloud-rust`**:

   - Test generating code with the local librarian binary directly against
     `google-cloud-rust`:
     ```bash
     go run <path-to-librarian-worktree>/cmd/librarian generate <library-name>
     # Or regenerate all if needed:
     go run <path-to-librarian-worktree>/cmd/librarian generate --all
     # Format librarian.yaml if modified:
     go run <path-to-librarian-worktree>/cmd/librarian tidy
     ```
   - Verify that the generated code fixes the issue and passes local formatting,
     typos, clippy, and cargo check:
     ```bash
     cargo fmt --check
     typos
     cargo check --workspace --all-targets
     ```

1. **Subagent Code Review for `librarian`**:

   - Ask a subagent via `invoke_subagent` to run the `review-pr` skill on the
     `librarian` worktree:
     ```json
     {
       "TypeName": "self",
       "Role": "Librarian PR Reviewer",
       "Prompt": "Run the review-pr skill in <path-to-librarian-worktree> to review the local changes for issue <NNNN> against project conventions."
     }
     ```
   - Address any actionable feedback identified by the subagent review.

1. **Coordinate PRs and Update `librarian.yaml`**:

   - Open a PR in `googleapis/librarian` (or wait for review/merge).
   - Once the target version or commit is available, update `librarian.yaml` and
     regenerate:
     ```bash
     V=$(GOPROXY=direct go list -m -f '{{.Version}}' github.com/googleapis/librarian@main)
     go run github.com/googleapis/librarian/cmd/librarian@${V} config set version ${V}
     go run github.com/googleapis/librarian/cmd/librarian@${V} generate --all
     go run github.com/googleapis/librarian/cmd/librarian@${V} tidy
     ```

1. **CRITICAL: The Two-Commit Rule in `google-cloud-rust`**: Split the changes
   into **two separate commits**:

   - **Commit 1: Automatic Librarian Changes**:
     - Stage only the generated code (`src/generated/`), `librarian.yaml`, and
       updated dependencies in `Cargo.toml`/`Cargo.lock`.
     - Commit title **MUST be under 50 characters**.
     - Example: `feat(generator): update generated code for #1234`
   - **Commit 2: Rust Hand-written Code & Tests**:
     - Stage tests, documentation, or other manual modifications in
       `google-cloud-rust`.
     - Commit title **MUST be under 50 characters**.
     - Example: `test(storage): add tests for #1234`

______________________________________________________________________

#### Scenario B: Pure `google-cloud-rust` Changes (No `librarian` Changes)

1. **Implement and Test in `google-cloud-rust`**:

   - Implement the fix in the appropriate crate under `src/` (e.g.
     `src/storage`, `src/gax`, etc.).
   - Add unit tests or integration tests.
   - Run targeted crate tests:
     ```bash
     cargo test -p <crate_name>
     ```
   - Run strict clippy on handwritten crates:
     ```bash
     cargo clippy-strict
     ```
   - Format and check typos:
     ```bash
     cargo fmt
     typos
     ```

1. **Commit Changes**:

   - Use Conventional Commits (`fix(pkg): ...`, `test(pkg): ...`).
   - Commit title **MUST be under 50 characters**.
   - Include issue reference in the commit body (`Fixes #<NNNN>`).

______________________________________________________________________

### Step 5: Pre-PR Validation Checklist

Before presenting the work or opening a PR, verify:

1. **Commit Title Length Check**: Confirm all commit titles on the branch are
   strictly under 50 characters:

   ```bash
   BASE_REF="upstream/main"
   git rev-parse --verify upstream/main >/dev/null 2>&1 || BASE_REF="origin/main"
   git log "${BASE_REF}..HEAD" --format="%s" | while read -r title; do
     len=${#title}
     if [ "$len" -gt 50 ]; then
       echo "ERROR: Commit title exceeds 50 chars ($len): $title"
     else
       echo "OK ($len chars): $title"
     fi
   done
   ```

1. **Formatting & Spell Check**:

   ```bash
   cargo fmt --check
   typos
   ```

1. **Clippy Verification**:

   ```bash
   cargo clippy --all-targets -- -D warnings
   cargo clippy --profile test -- -D warnings
   cargo clippy-strict
   ```

1. **Tests**:

   ```bash
   cargo test -p <crate_name>
   ```

1. **User Confirmation Before Push**:

   - Never automatically push to remote or open a PR without asking the user.
   - Summarize the commits and validation results, and ask the user if they want
     to push and open a PR.
