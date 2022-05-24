# How to contribute

We're glad you're interested in contributing to the development of Víðarr and are happy to collaborate.
Please review this document to ensure that your work fits well into the Víðarr code base.

## Tickets

Create a ticket for your issue if one does not exist. As development of Víðarr is mainly done at OICR
currently, the ticket is usually on the internal OICR JIRA, but a GitHub Issue is also acceptable.
This ensures that we have a place to discuss the changes before the work is done. A ticket is not
necessary if the change is trivial, such as correcting a typo.

## Branches

Create a feature branch. The branch should be based on the `master` branch unless you have reason
to do otherwise. The branch name should begin with the ticket/issue number, and be followed by a brief hint
of what it is about. e.g. "#1234_fixLibraryPage"

## Code Formatting

Please format your code using `google-java-format`. This plugin can be installed in IntelliJ IDEA. Changes that are incorrectly formatted will cause the build to fail.

## Testing

We have several types of automated testing:

* Unit tests and integration tests
  * Run with `mvn clean test`
* REST API Integration tests
  * Run with `mvn clean test -Dtest=MainIntegrationTest`

You can run individual test classes or methods by adding `-Dtest=ClassName` or `Dtest=ClassName#methodName`

Please make sure to add or update the relevant tests for your changes.

## Commits

* Make sure your commit messages begin with the issue number. e.g. "#1234: Fix issue where unloading a file and loading in a new file with the same hash_id would cause the old file path to be used."
* Include change messages as described [here](changes/README.md) if your change will be user-facing

## Pull Requests

Changes should never be merged directly into `master`. Pull requests should be made into
the `master` branch for testing and review.

The pull request description will automatically be filled in with the 
[pull request template](.github/pull_request_template.md). Please link to the issue for this 
pull request and complete the checklist.

## Merging

Once all of the tests are passing, and your pull request has received two approvals, you are ready to
merge. To keep a clean commit history please

1. Squash your changes into one commit unless they are clearly separate changes.
2. Rebase on the `develop` branch so that your change appears after the changes that were previously
merged in the history.

You can usually use the 'Squash and merge' feature on GitHub to do this. Use the 'Rebase and merge'
feature if you would like to keep the commits separate. If you have a more complex situation, such as
wanting to squash some commits but not others, you should use `git rebase` and then re-run tests
before merging. If you do not have the necessary permissions to merge into `develop`, please request
for someone else to do the merge.

Please delete your feature branch after it is merged.

### Thank you for your contributions!
