package io.github.interestinglab.waterdrop.spark.structuredstream

import java.util.{List => JList}

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.env.Execution
import io.github.interestinglab.waterdrop.spark.{BaseSparkTransform, SparkEnvironment}

class StructuredStreamingExecution(environment: SparkEnvironment) extends Execution[StructuredStreamingSource, BaseSparkTransform, StructuredStreamingSink] {

  private var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(void: Void): Unit = {}

  override def start(sources: JList[StructuredStreamingSource],
                     transforms: JList[BaseSparkTransform],
                     sinks: JList[StructuredStreamingSink]): Unit = {

  }
}
