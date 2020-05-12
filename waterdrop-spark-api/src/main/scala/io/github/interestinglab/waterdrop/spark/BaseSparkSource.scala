package io.github.interestinglab.waterdrop.spark

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSource

trait BaseSparkSource[Data] extends BaseSource[SparkEnvironment] {

  protected var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  def getData(env: SparkEnvironment): Data;

}
