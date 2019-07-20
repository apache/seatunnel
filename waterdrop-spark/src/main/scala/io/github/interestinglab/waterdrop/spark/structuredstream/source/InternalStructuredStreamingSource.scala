package io.github.interestinglab.waterdrop.spark.structuredstream.source

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.plugin.CheckResult

abstract class InternalStructuredStreamingSource extends StructuredStreamingSource{

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig: Config = config

  override def checkConfig: CheckResult = new CheckResult(true,"")

  override def prepare(): Unit = {
  }
}
