package io.github.interestinglab.waterdrop.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Mysql extends Jdbc {

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "table", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "com.mysql.jdbc.Driver").load()
  }
}
