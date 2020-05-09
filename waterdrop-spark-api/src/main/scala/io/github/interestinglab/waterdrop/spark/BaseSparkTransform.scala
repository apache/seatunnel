package io.github.interestinglab.waterdrop.spark

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseTransform
import org.apache.spark.sql.{Dataset, Row}

trait BaseSparkTransform extends BaseTransform[SparkEnvironment] {

  protected var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row];

}
