package io.github.interestinglab.waterdrop.spark.stream.sink

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSink
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkStreamingSink extends BaseSink[Dataset[Row], Unit, SparkStreamingEnv]{

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true,"")

  override def prepare(): Unit = {}
}
