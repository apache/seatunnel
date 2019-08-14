package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.{BaseSparkSink, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

class Console extends BaseSparkSink[Unit] {

  override def output(data: Dataset[Row], environment: SparkEnvironment): Unit = {
    data.show()
  }

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}
}
