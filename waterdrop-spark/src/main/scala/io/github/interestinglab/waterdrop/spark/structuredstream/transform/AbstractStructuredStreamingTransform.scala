package io.github.interestinglab.waterdrop.spark.structuredstream.transform

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseTransform
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.structuredstream.StructuredStreamingEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractStructuredStreamingTransform extends BaseTransform[Dataset[Row], Dataset[Row], StructuredStreamingEnv]{

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig: Config = config

  override def checkConfig: CheckResult = new CheckResult(true,"")

}
