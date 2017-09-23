package org.interestinglab.waterdrop.filter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class Sql(var conf: Config) extends BaseFilter(conf) {

  override def checkConfig(): (Boolean, String) = {
    // TODO
    (true, "")
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView(this.conf.getString("table_name"))
    spark.sql(conf.getString("sql"))
  }
}
