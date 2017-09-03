package org.interestinglab.waterdrop.filter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}


class Sql(var conf : Config) extends BaseFilter(conf) {

  def checkConfig() : (Boolean, String) = {
    // TODO
    (true, "")
  }


  def process(dataFrame : DataFrame) : DataFrame = {

    dataFrame.createOrReplaceTempView(this.conf.getString("table_name"))
    this.sqlContext.sql(this.conf.getString("sql"))

  }
}
