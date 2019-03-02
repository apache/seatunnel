package io.github.interestinglab.waterdrop.config

case class CommandLineArgs(
  deployMode: String = "client",
  configFile: String = "application.conf",
  engine: String = "batch",
  testConfig: Boolean = false)
