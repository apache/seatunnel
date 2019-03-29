package io.github.interestinglab.waterdrop.output.batch

import org.apache.spark.sql.{Dataset, Row}

class S3 extends FileOutputBase {

  override def checkConfig(): (Boolean, String) = {
    checkConfigImpl(List("s3://", "s3a://", "s3n://"))
  }

  override def process(df: Dataset[Row]): Unit = {

    super.processImpl(df, "s3a://")
  }
}
