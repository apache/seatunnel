package io.github.interestinglab.waterdrop.config

import io.github.interestinglab.waterdrop.common.config.Common

object CommandLineUtils {

  /**
    * command line arguments sparkParser.
    * */
  val sparkParser = new scopt.OptionParser[CommandLineArgs]("start-waterdrop-spark.sh") {
    head("Waterdrop", "2.0.0")

    opt[String]('c', "config")
      .required()
      .action((x, c) => c.copy(configFile = x))
      .text("config file")
    opt[Unit]('t', "check")
      .action((_, c) => c.copy(testConfig = true))
      .text("check config")
    opt[String]('e', "deploy-mode")
      .required()
      .action((x, c) => c.copy(deployMode = x))
      .validate(x =>
        if (Common.isModeAllowed(x)) success
        else failure("deploy-mode: " + x + " is not allowed."))
      .text("spark deploy mode")
    opt[String]('m', "master")
      .required()
      .text("spark master")
    opt[String]('i', "variable")
      .optional()
      .text(
        "variable substitution, such as -i city=beijing, or -i date=20190318")
      .maxOccurs(Integer.MAX_VALUE)
  }

  val flinkParser = new scopt.OptionParser[CommandLineArgs]("start-waterdrop-flink.sh") {
    head("Waterdrop", "2.0.0")

    opt[String]('c', "config")
      .required()
      .action((x, c) => c.copy(configFile = x))
      .text("config file")
    opt[Unit]('t', "check")
      .action((_, c) => c.copy(testConfig = true))
      .text("check config")
    opt[String]('i', "variable")
      .optional()
      .text(
        "variable substitution, such as -i city=beijing, or -i date=20190318")
      .maxOccurs(Integer.MAX_VALUE)
  }
}
