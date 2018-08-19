package io.github.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{DataFrame, SparkSession}

class Repartition extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("num_partitions") && conf.getInt("num_partitions") > 0 match {
      case true => (true, "")
      case false => (false, "please specify [num_partitions] as Integer > 0")
    }
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    df.repartition(conf.getInt("num_partitions"))
  }
}
