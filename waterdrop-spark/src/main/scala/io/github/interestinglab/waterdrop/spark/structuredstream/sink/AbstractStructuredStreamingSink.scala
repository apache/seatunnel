package io.github.interestinglab.waterdrop.spark.structuredstream.sink

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSink
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.structuredstream.StructuredStreamingEnv
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

abstract class AbstractStructuredStreamingSink extends BaseSink[Dataset[Row], DataStreamWriter[Row], StructuredStreamingEnv]{

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true,"")

  override def prepare(): Unit = {}
}
