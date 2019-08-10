package io.github.interestinglab.waterdrop.spark.stream.transform

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseTransform
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkStreamingTransform
    extends BaseTransform[Dataset[Row], Dataset[Row], SparkStreamingEnv] {

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}
}
