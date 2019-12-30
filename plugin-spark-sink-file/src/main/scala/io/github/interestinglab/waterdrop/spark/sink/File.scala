package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import org.apache.spark.sql.{Dataset, Row}


class File extends FileSinkBase {

  override def checkConfig(): CheckResult = {
    checkConfigImpl(List("file://"))
  }

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    outputImpl(data, "file://")
  }
}
