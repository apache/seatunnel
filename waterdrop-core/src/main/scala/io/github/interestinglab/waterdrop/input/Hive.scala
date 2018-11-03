package io.github.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hive extends BaseStaticInput{
  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("table_name") && config.hasPath("hive_db") && config.hasPath("hive_table") match {
      case true => (true, "")
      case false => (false, "please specify [table_name] and [hive_db] and [hive_db]")
    }
  }


  override def getDataset(spark: SparkSession): Dataset[Row] = {

    val db = config.getString("hive_db")
    val table = config.getString("hive_table")
    val table_name = config.getString("table_name")
    spark.sql(s"use $db")
    val ds = spark.sql(s"select * from $table")
    ds.createOrReplaceTempView(s"$table_name")
    ds
    }

}
