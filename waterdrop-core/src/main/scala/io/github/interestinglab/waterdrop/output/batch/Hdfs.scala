package io.github.interestinglab.waterdrop.output.batch

import org.apache.spark.sql.{Dataset, Row}

class Hdfs extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("hdfs://"))
  }

  override def process(df: Dataset[Row]): Unit = {

    super.processImpl(df, "hdfs://")
  }
}
