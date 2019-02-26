package io.github.interestinglab.waterdrop.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Mysql extends Jdbc {

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .load()
  }
}
