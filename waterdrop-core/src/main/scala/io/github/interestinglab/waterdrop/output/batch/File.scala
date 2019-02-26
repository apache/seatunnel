package io.github.interestinglab.waterdrop.output.batch

import org.apache.spark.sql.{Dataset, Row}

class File extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("file://"))
  }

  override def process(df: Dataset[Row]): Unit = {

    super.processImpl(df, "file://")
  }
}
