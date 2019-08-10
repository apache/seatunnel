package io.github.interestinglab.waterdrop.spark.batch.sink

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSink
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchEnv
import org.apache.spark.sql.{Dataset, Row}

abstract class AbstractSparkBatchSink
    extends BaseSink[Dataset[Row], Unit, SparkBatchEnv] {
  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}
}
