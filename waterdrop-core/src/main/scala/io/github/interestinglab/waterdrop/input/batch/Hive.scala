package io.github.interestinglab.waterdrop.input.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive extends BaseStaticInput {
  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("table_name") && config.hasPath("pre_sql") match {
      case true => (true, "")
      case false => (false, "please specify [table_name] and [pre_sql]")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val regTable = config.getString("table_name")
    val ds = spark.sql(config.getString("pre_sql"))
    ds.createOrReplaceTempView(s"$regTable")
    ds
  }

}
