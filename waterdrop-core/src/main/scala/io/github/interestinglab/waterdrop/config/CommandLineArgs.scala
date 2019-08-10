package io.github.interestinglab.waterdrop.config

case class CommandLineArgs(deployMode: String = "client",
                           configFile: String = "application.conf",
                           testConfig: Boolean = false)
