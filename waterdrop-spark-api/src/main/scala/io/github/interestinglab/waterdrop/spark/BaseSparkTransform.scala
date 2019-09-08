package io.github.interestinglab.waterdrop.spark

import com.typesafe.config.waterdrop.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseTransform
import org.apache.spark.sql.{Dataset, Row}

trait BaseSparkTransform extends BaseTransform[Dataset[Row], Dataset[Row], SparkEnvironment] {

  protected var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

}
