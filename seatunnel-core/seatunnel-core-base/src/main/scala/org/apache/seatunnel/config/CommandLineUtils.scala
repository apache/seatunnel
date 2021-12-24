/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.config

import org.apache.seatunnel.common.config.Common

object CommandLineUtils {

  /**
   * command line arguments sparkParser.
   */
  val sparkParser = new scopt.OptionParser[CommandLineArgs]("start-seatunnel-spark.sh") {
    head("SeaTunnel", "2.0.0")

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

  val flinkParser = new scopt.OptionParser[CommandLineArgs]("start-seatunnel-flink.sh") {
    head("SeaTunnel", "2.0.0")

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
