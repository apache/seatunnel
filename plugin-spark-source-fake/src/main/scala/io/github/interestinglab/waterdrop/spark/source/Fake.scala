package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSource
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory}

class Fake extends SparkBatchSource {

  override def prepare(): Unit = {}

  override def getData(env: SparkEnvironment): Dataset[Row] = {

    val s = Seq(RowFactory.create("Hello garyelephant"),
                RowFactory.create("Hello rickyhuo"),
                RowFactory.create("Hello kid-xiong"))

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    env.getSparkSession.createDataset(s)(RowEncoder(schema))
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
