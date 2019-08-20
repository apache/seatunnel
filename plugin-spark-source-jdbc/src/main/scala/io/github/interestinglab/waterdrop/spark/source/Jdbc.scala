package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Jdbc extends SparkBatchSource {

  override def prepare(): Unit = {}

  override def getData(env: SparkEnvironment): Dataset[Row] = {
    val spark = env.getSparkSession
    val df = spark.read
      .format("jdbc")
      .option("url", config.getString("url"))
      .option("dbtable", config.getString("table"))
      .option("user", config.getString("user"))
      .option("password", config.getString("password"))
      .option("driver", config.getString("driver"))
      .load()
    df
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("url", "table", "user", "password");

    val nonExistsOptions = requiredOptions
      .map(optionName => (optionName, config.hasPath(optionName)))
      .filter { p =>
        val (optionName, exists) = p
        !exists
      }

    if (nonExistsOptions.isEmpty) {
      new CheckResult(true, "")
    } else {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string"
      )
    }
  }
}
