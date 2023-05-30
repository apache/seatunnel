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

## Purpose of this pull request

<!-- Describe the purpose of this pull request. For example: This pull request adds checkstyle plugin.-->

## Check list

* [ ] Code changed are covered with tests, or it does not need tests for reason:
* [ ] If any new Jar binary package adding in your PR, please add License Notice according
  [New License Guide](https://github.com/apache/seatunnel/blob/dev/docs/en/contribution/new-license.md)
* [ ] If necessary, please update the documentation to describe the new feature. https://github.com/apache/seatunnel/tree/dev/docs
* [ ] If you are contributing the connector code, please check that the following files are updated:
  1. Update change log that in connector document. For more details you can refer to [connector-v2](https://github.com/apache/seatunnel/tree/dev/docs/en/connector-v2)
  2. Update [plugin-mapping.properties](https://github.com/apache/seatunnel/blob/dev/plugin-mapping.properties) and add new connector information in it
  3. Update the pom file of [seatunnel-dist](https://github.com/apache/seatunnel/blob/dev/seatunnel-dist/pom.xml)
* [ ] Update the [`release-note`](https://github.com/apache/seatunnel/blob/dev/release-note.md).