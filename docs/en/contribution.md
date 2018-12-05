# Contribution Guide

## Coding Style

Scala Coding Style References:

> http://docs.scala-lang.org/style/

> https://github.com/databricks/scala-style-guide

We are using the sbt plugin [scalastyle](http://www.scalastyle.org/) as the coding style checker. Code commits that cannot be checked by the coding style will not be merged.

You can use [Scalafmt](https://scalameta.org/scalafmt/#IntelliJ) from your editor, build tool or terminal to help you formatting your code automatically.

If you use the [scalafmt Idea plugin](https://plugins.jetbrains.com/plugin/8236-scalafmt), we recommend you to set `Format on file save` when the plugin has installed. `Preferences > Tools > Scalafmt`

* for the current project (recommended): Preferences > Tools > Scalafmt
* for all new project: File > Other settings > Preferences for new projects > Tools > Scalafmt

## Workflow to be a Contributor

* Member of Interesting Lab:

1. Create a branch from master, the branch naming rules are as follows:
    - Feature: <username>.fea.<feature_name>
    - Bug Fix: <username>.fixbug.<bugname_or_issue_id>
    - Document: <username>.doc.<doc_name>
    - ...
2. Develop and submit your commit.
3. Click the button of `new pull request` to create a pull request from your branch on Waterdrop github project homepage.
4. After at least one member has approved the review and the travis-ci build has passed, the reviewer merges the branch.
5. Delete you branch.

* Not Member of Interesting Lab:

1. Fork waterdrop from https://github.com/InterestingLab/waterdrop
2. Develop
3. Submit you commit to your own project.
4. Click the button of `new pull request` to create a pull request from your branch on your own Waterdrop github project homepage.
5. After being approved by Interesting Lab, Your contribution will be included in the project code.

## Building and testing

This project uses [travis-ci](https://travis-ci.org/) as an automatic building tool.

Every commit of every branch and every pull request will trigger an automated building.
