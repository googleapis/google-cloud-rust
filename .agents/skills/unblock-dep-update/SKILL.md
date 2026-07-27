---
name: unblock-dep-update
description: Use this skill when a dependabot dependency update PR fails.
---

# Task

You need to help unblock a dependency update.

The user should give you a PR number.

```
PR_NUMBER=... # e.g. 6133
```

## Triage the build failure

The first thing to do is to inspect the build failure.

You can determine the commit SHA with:

```shell
COMMIT_SHA=$(gh pr view "$PR_NUMBER" --json commits --jq '.commits[-1].oid')
```

You can enumerate the failed build IDs with:

```shell
FAILED_BUILDS=$(gcloud builds list \
    --project=rust-sdk-testing \
    --region=us-central1 \
    --filter="status='FAILURE'" \
    --limit=100 \
    --format="json" | jq -r --arg SHA "$COMMIT_SHA" '.[] | select(.substitutions.COMMIT_SHA == $SHA) | .id')
```

You can look at the last lines of output with:

```shell
BUILD_ID=${FAILED_BUILDS[0]} # Typically one build is sufficient.
gcloud builds log "$BUILD_ID" --project=rust-sdk-testing --region=us-central1 | tail -n 200
```

In the output, identify the specific command that failed, and why. e.g. a
`cargo test -p google-cloud-<package>`.

## Checkout the code

```shell
gh pr checkout ${PR_NUMBER}
```

## Fix the build failure

Reproduce the build failure locally. Then iteratively fix the error.

When the command succeeds, report success!

## Report success

Explain the changes and show the diff to the user.

Determine a good commit message for the changes.

```shell
COMMIT_MSG=... # e.g. "update foo to be more like bar"
```

Determine the upstream branch we checked out.

```shell
BRANCH=$(git branch --show-current)
```

Suggest to the user how to commit the changes and push them upstream. e.g.:

> To commit these changes and push them to the dependabot branch, run:
>
> ```
> git commit -m "${COMMIT_MSG}" .
> git push upstream HEAD:${BRANCH}
> ```
