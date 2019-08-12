package io.github.interestinglab.waterdrop.spark.transform

import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row}

class Sql extends BaseSparkTransform {


  override def process(data: Dataset[Row],env: SparkEnvironment): Dataset[Row] = {

    data.createOrReplaceTempView(config.getString("table.name"))
    env.getSparkSession.sql(config.getString("sql"))
  }

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}
}
