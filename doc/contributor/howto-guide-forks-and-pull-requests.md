# How-to Guide: Forks and Pull Requests

We wrote this document because many Googlers do not use GitHub on a daily basis.
If you are an experienced GitHub contributor much of the information here will
be familiar to you, feel free to skip it. If you are new to GitHub or have not
used it in a while and want a refresher this might be useful.

## Creating a fork

In this project we use the (more or less) standard
[GitHub workflow][workflow-link]:

You create a [fork][fork-link] of [google-cloud-rust][repo-link]. You can think
of a "fork" as a full copy of the original repository, including all its history
and branches. Then [clone][about-clone] that fork into your workstation:

```console
git clone git@github.com:YOUR-USER-NAME/google-cloud-rust.git
```

This creates a *second* copy of the original repository, with all its history
and branches. You can create new branches in this copy, change the history of
branches (you can, but don't!), and generally do all the version control things
you may be used to. Note that these local changes do not affect either of the
previous two copies.

The cloned repo that you created in the previous step will have its `origin` set
to your forked repo. You should now tell git about the main `upstream` repo,
which you'll use to pull commits made by others in order to keep your local repo
and fork up to date.

```console
git remote add upstream git@github.com:googleapis/google-cloud-rust.git
git remote -v  # Should show 'origin' (your fork) and 'upstream' (main repo)
```

To pull new commits from `upstream` into your local repo and
[sync your fork][syncing-a-fork] you can do the following:

```console
git checkout main
git pull --ff-only upstream main
git push  # Pushes new commits up to your fork on GitHub
```

> :warning: you probably want to do this periodically, and almost certainly
> before starting any new branches. Keeping your default branch (aka `main`) in
> sync is important to make your pull requests easy to review.

## Preparing to make a pull requests

Changes to the main repository must go through a review by one of the project
owners (even project owners have their code reviewed by a peer). To submit your
change for review you need to create a pull request. Typically you start by:

1. Picking an existing [GitHub bug][mastering-issues] to work on.
1. Create a new [branch][about-branches] for each feature (or bug fix).
   ```console
   git checkout main
   git checkout -b my-feature-branch
   git push -u origin my-feature-branch  # Tells fork on GitHub about new branch
   # make your changes
   git push
   ```
1. And then submit a [pull-request][about-pull-requests] to merge your branch
   into `googleapis/google-cloud-rust`.
1. Your reviewers may ask questions, suggest improvements or alternatives. You
   address those by either answering the questions in the review or **adding
   more [commits][about-commits]** to your branch and `git push` -ing those
   commits to your fork.

## Merging the changes

Eventually the reviewers accept your changes, and they are merged into the
`main` branch. We use "squash commits", where all your commits become a single
commit into the default branch. A project owner needs to merge your changes, if
you are a project owner, the expectation is that you will perform the merge
operation, and update the commit comments to something readable.

## When the PR requires more work

The previous steps described a happy case for a PR (hopefully for most PRs),
where no build failures or conflicts are detected in the PR. The next two
sections explain what to do when things are not so rosy.

### Resolving Conflicts and Rebasing

From time to time your pull request may have conflicts with the destination
branch (likely `main`). If so, we request that you [rebase][about-rebase] your
branch instead of merging. The reviews can become very confusing if you merge
during a pull request. You should first ensure that your `main` branch has all
the latest commits by syncing your fork (see above), then do the following:

```shell
git checkout my-feature-branch
git rebase main
git push --force-with-lease
```

If there are conflicts, the `git rebase` command will show you the conflicts.
These will not be automatically resolved, if they were, `git rebase` would not
have required human intervention! You will need to edit the code to resolve
these conflicts, potentially `git add` or `git rm` files as needed. Once the
conflicts are resolved you `git add` the files to indicate the conflict
resolution is complete, and then continue the rebase with:

```
git rebase --continue
```

If there are multiple commits in your PR this process runs for each commit.

## Tips and tricks

### Popping latest commit

This pops the latest commit, leaving the code in exactly the same state.

```sh
git reset HEAD~
```

### Adding changes to the latest commit

```sh
git commit --amend
```

Or if you don't need to edit the commit message, you can:

```sh
git commit --amend --no-edit
```

### Rebasing with local changes

To rebase with local changes, we can push/pop them to the stash:

```sh
git stash push .
git rebase upstream/main
git stash pop
```

Some people prefer to do this by pushing and popping commits.

### Adding changes to a previous commit

Often when we send PRs with generated code, we make the manual changes in a
single commit, then generate code in a second commit. Sometimes it is convenient
to add changes to the original commit without playing the push/pop game.

Determine the SHA of the commit we want by looking at the log:

```sh
git reflog
```

Add these changes to the commit with SHA=`<SHA>`:

```sh
git commit --fixup <SHA>
git rebase --autosquash upstream/main
```

### Adding commits to someone else's PR

Sometimes it can be helpful to push commits to the branch of an external
contributor, or of a tool like dependabot. To do so, follow the
[Adding Commits to Someone Else's Pull Request][add-to-pr] guide.

[about-branches]: https://help.github.com/articles/about-branches/
[about-clone]: https://help.github.com/articles/cloning-a-repository/
[about-commits]: https://help.github.com/desktop/guides/contributing-to-projects/committing-and-reviewing-changes-to-your-project/#about-commits
[about-pull-requests]: https://help.github.com/articles/about-pull-requests/
[about-rebase]: https://help.github.com/articles/about-git-rebase/
[add-to-pr]: https://tighten.com/insights/adding-commits-to-a-pull-request/
[fork-link]: https://guides.github.com/activities/forking/
[mastering-issues]: https://guides.github.com/features/issues/
[repo-link]: https://github.com/googleapis/google-cloud-rust.git
[syncing-a-fork]: https://help.github.com/articles/syncing-a-fork/
[workflow-link]: https://guides.github.com/introduction/flow/
