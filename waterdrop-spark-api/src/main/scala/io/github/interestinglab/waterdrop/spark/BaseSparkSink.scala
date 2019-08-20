package io.github.interestinglab.waterdrop.spark

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSink
import org.apache.spark.sql.{Dataset, Row}

trait BaseSparkSink[OUT] extends BaseSink[Dataset[Row], OUT, SparkEnvironment] {

  protected var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config
}
