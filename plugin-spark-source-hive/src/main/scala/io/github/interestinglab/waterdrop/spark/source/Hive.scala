package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Hive extends SparkBatchSource {

  override def prepare(env: SparkEnvironment): Unit = {}

  override def checkConfig(): CheckResult = {
    config.hasPath("pre_sql") match {
      case true => new CheckResult(true, "")
      case false => new CheckResult(false, "please specify [pre_sql]")
    }
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    env.getSparkSession.sql(config.getString("pre_sql"))
  }
}
