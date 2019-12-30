package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import org.apache.spark.sql.{Dataset, Row}


class Hdfs extends FileSinkBase {

  override def checkConfig(): CheckResult = {
    checkConfigImpl(List("hdfs://"))
  }

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    outputImpl(data, "hdfs://")
  }
}
