package io.github.interestinglab.waterdrop.spark.transform

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

class Sql extends BaseSparkTransform {

  override def process(data: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    env.getSparkSession.sql(config.getString("sql"))
  }

  override def checkConfig(): CheckResult = {
    if (config.hasPath("sql")) {
      new CheckResult(true, "")
    } else {
      new CheckResult(false, "please specify [sql]")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {}
}
