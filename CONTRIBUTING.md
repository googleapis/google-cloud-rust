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

This repository follows the
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
specification. Typically, the package name should be included in parentheses
after the commit type (e.g., `feat(storage):`).

### Guidelines for Commit Types

To ensure our release notes are focused and valuable to our users, please adhere
to the following guidelines when choosing a commit type:

- **`feat(...)`**: Use this for changes that are visible to the end-user. Avoid
  using it for internal implementation details or features that are not yet
  released.

- **`fix(...)`**: Use this for bug fixes in released code only.

- **`docs(...)`**: For changes to documentation only.

- **`impl(...)`**: Use this for new features or functionality that are purely
  implementation details and not directly visible to the end-user.

- **`refactor(...)`**: Use this for code changes that neither fix a bug nor add
  a feature, but improve the design or structure of the code.

- **`cleanup(...)`**: For routine code maintenance, such as removing unused code
  or fixing linter warnings.

- **`ci(...)`**: For changes to our CI configuration and scripts.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).
