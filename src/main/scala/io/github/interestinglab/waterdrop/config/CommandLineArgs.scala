package io.github.interestinglab.waterdrop.config

case class CommandLineArgs(
  master: String = "local[2]",
  configFile: String = "application.conf",
  testConfig: Boolean = false)
