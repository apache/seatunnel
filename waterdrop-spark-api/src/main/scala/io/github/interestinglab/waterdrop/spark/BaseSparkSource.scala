package io.github.interestinglab.waterdrop.spark

import com.typesafe.config.waterdrop.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSource

trait BaseSparkSource[Data] extends BaseSource {

  protected var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  def getData(env: SparkEnvironment): Data;

  def name: String = this.getClass.getName
}
