# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Commit Messages

Commit messages for `google-cloud-rust` follow the conventions below. Note that
your PR title defaults to the first commit in your branch, and that the merge
commit is composed of your PR title and PR description by default.

Here is an example:

```
feat(storage): add support for inter-dimensional object teleportation

This change introduces the `teleport_object` method, allowing users to move
objects between different dimensional planes. This is an experimental feature
and may cause temporal paradoxes.

The `destination_dimension` parameter is required and must be a valid
dimensional identifier. The `safety_precautions` field in TeleportOptions
is highly recommended.

Fixes #12345
```

### First line

The first line of the change description is a short one-line summary of the
change, following the structure `<type>(<scope>): <description>`:

#### type

A structural element defined by the conventions at
[https://www.conventionalcommits.org/en/v1.0.0/#summary](https://www.conventionalcommits.org/en/v1.0.0/#summary).

Conventional commits are parsed by our release tooling to generate release
notes. See [Guidelines for Commit Types](#guidelines-for-commit-types) for more
details.

##### Guidelines for Commit Types

To ensure our release notes are focused and valuable to our users, please adhere
to the following guidelines when choosing a commit type:

- **`feat(...)`**: Use this for changes that are visible to the end-user. Avoid
  using it for internal implementation details or features that are not yet
  released.

- **`fix(...)`**: Use this for bug fixes in released code only.

- **`docs(...)`**: For changes to public documentation only.

- **`impl(...)`**: Use this for new features or functionality that are purely
  implementation details and not directly visible to the end-user.

- **`refactor(...)`**: Use this for code changes that neither fix a bug nor add
  a feature, but improve the design or structure of the code.

- **`cleanup(...)`**: For routine code maintenance, such as removing unused code
  or fixing linter warnings.

- **`test(...)`**: For improvements to tests, deflaking tests, and fixes to the
  tests themselves.

- **`ci(...)`**: For changes to our CI configuration and scripts.

#### scope

The name of the crate affected by the change, which should be provided in
parentheses before the colon. Please omit the `google-cloud-` prefix (e.g., use
`storage` instead of `google-cloud-storage`).

#### description

A short one-line summary of the change. It should complete written so to
complete the sentence "This change modifies the crate to ..." That means it does
not start with a capital letter, is not a complete sentence, and actually
summarizes the result of the change. Note that the verb after the colon is
lowercase, and there is no trailing period.

The first line should be kept as short as possible (many git viewing tools
prefer under ~76 characters).

Follow the first line by a blank line.

### Main content

The rest of the commit message should provide context for the change and explain
what it does. Write in complete sentences with correct punctuation.

Add any relevant information, such as benchmark data if the change affects
performance.

### Referencing issues

The special notation "Fixes #12345" associates the change with issue 12345 in
the `google-cloud-rust` issue tracker. When this change is eventually applied,
the issue tracker will automatically mark the issue as fixed.

If the change is a partial step towards the resolution of the issue, write "For
#12345" instead. This will leave a comment in the issue linking back to the pull
request, but it will not close the issue when the change is applied.

Please don’t use alternate GitHub-supported aliases like Close or Resolves
instead of Fixes.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).
