package io.github.interestinglab.waterdrop.input.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Kudu extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("kudu_master") && config.hasPath("kudu_table") && config.hasPath("table_name") match {
      case true => (true, "")
      case false => (false, "please specify [kudu_master] and [kudu_table] and [table_name]")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val mapConf = Map("kudu.master" -> config.getString("kudu_master"), "kudu.table" -> config.getString("kudu_table"))

    val ds = spark.read
      .format("org.apache.kudu.spark.kudu")
      .options(mapConf)
      .kudu
    ds.createOrReplaceTempView(config.getString("table_name"))
    ds
  }
}
