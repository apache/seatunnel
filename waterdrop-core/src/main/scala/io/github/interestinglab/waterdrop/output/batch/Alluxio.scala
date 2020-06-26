package io.github.interestinglab.waterdrop.output.batch

import org.apache.spark.sql.{Dataset, Row}

class Alluxio extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("alluxio://"))
  }

  override def process(df: Dataset[Row]): Unit = {

    super.processImpl(df, "alluxio://")
  }
}
