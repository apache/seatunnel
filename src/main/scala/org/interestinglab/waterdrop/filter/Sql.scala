package org.interestinglab.waterdrop.filter

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SQLContext}


class Sql(var conf : Config) extends BaseFilter(conf) {

  def checkConfig() : (Boolean, String) = {
    // TODO
    (true, "")
  }

  def prepare(sqlContext:SQLContext) {

    println("sql")
    this.sqlContext = sqlContext
  }

  def process(dataFrame : DataFrame) : DataFrame = {

    dataFrame.createOrReplaceTempView(this.conf.getString("table_name"))
    this.sqlContext.sql(this.conf.getString("sql"))

  }
}
