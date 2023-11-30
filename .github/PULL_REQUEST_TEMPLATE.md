<!--

Thank you for contributing to SeaTunnel! Please make sure that your code changes
are covered with tests. And in case of new features or big changes
remember to adjust the documentation.

Feel free to ping committers for the review!

## Contribution Checklist

  - Make sure that the pull request corresponds to a [GITHUB issue](https://github.com/apache/seatunnel/issues).

  - Name the pull request in the form "[Feature] [component] Title of the pull request", where *Feature* can be replaced by `Hotfix`, `Bug`, etc.

  - Minor fixes should be named following this pattern: `[hotfix] [docs] Fix typo in README.md doc`.

-->

### Purpose of this pull request

<!-- Describe the purpose of this pull request. For example: This pull request adds checkstyle plugin.-->


### Does this PR introduce _any_ user-facing change?
<!--
Note that it means *any* user-facing change including all aspects such as the documentation fix.
If yes, please clarify the previous behavior and the change this PR proposes - provide the console output, description and/or an example to show the behavior difference if possible.
If possible, please also clarify if this is a user-facing change compared to the released SeaTunnel versions or within the unreleased branches such as dev.
If no, write 'No'.
If you are adding/modifying connector documents, please follow our new specifications: https://github.com/apache/seatunnel/issues/4544.
-->


### How was this patch tested?
<!--
If tests were added, say they were added here. Please make sure to add some test cases that check the changes thoroughly including negative and positive cases if possible.
If it was tested in a way different from regular unit tests, please clarify how you tested step by step, ideally copy and paste-able, so that other reviewers can test and check, and descendants can verify in the future.
If tests were not added, please describe why they were not added and/or why it was difficult to add.
If you are adding E2E test cases, maybe refer to https://github.com/apache/seatunnel/blob/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-cdc-mysql-e2e/src/test/resources/mysqlcdc_to_mysql.conf, here is a good example.
-->


### Check list

* [ ] If any new Jar binary package adding in your PR, please add License Notice according
  [New License Guide](https://github.com/apache/seatunnel/blob/dev/docs/en/contribution/new-license.md)
* [ ] If necessary, please update the documentation to describe the new feature. https://github.com/apache/seatunnel/tree/dev/docs
* [ ] If you are contributing the connector code, please check that the following files are updated:
  1. Update change log that in connector document. For more details you can refer to [connector-v2](https://github.com/apache/seatunnel/tree/dev/docs/en/connector-v2)
  2. Update [plugin-mapping.properties](https://github.com/apache/seatunnel/blob/dev/plugin-mapping.properties) and add new connector information in it
  3. Update the pom file of [seatunnel-dist](https://github.com/apache/seatunnel/blob/dev/seatunnel-dist/pom.xml)
* [ ] Update the [`release-note`](https://github.com/apache/seatunnel/blob/dev/release-note.md).