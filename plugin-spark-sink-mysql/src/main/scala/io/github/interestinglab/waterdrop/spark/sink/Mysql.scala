package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode
import org.apache.spark.sql.{Dataset, Row}

class Mysql extends SparkBatchSink{

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val saveMode = config.getString("save_mode")
    val customUpdateStmt = if (config.hasPath("customUpdateStmt")) config.getString("customUpdateStmt") else ""
    val duplicateIncs = if (config.hasPath("duplicateIncs")) config.getString("duplicateIncs") else ""
    if ("update".equals(saveMode)) {
      data.write.format("org.apache.spark.sql.execution.datasources.jdbc2").options(
        Map(
          "savemode" -> JDBCSaveMode.Update.toString,
          "driver" -> "com.mysql.jdbc.Driver",
          "url" -> config.getString("url"),
          "user" -> config.getString("user"),
          "password" -> config.getString("password"),
          "dbtable" -> config.getString("dbtable"),
          "useSSL" -> "false",
          "duplicateIncs" -> duplicateIncs,
          "customUpdateStmt" -> customUpdateStmt, //Custom mysql duplicate key update statement when saveMode is update
          "showSql" -> "true"
        )
      ).save()
    } else {
      val prop = new java.util.Properties()
      prop.setProperty("driver", "com.mysql.jdbc.Driver")
      prop.setProperty("user", config.getString("user"))
      prop.setProperty("password", config.getString("password"))
      data.write.mode(saveMode).jdbc(config.getString("url"), config.getString("dbtable"), prop)
    }


  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("url", "dbtable", "user", "password")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.nonEmpty) {
      new CheckResult(
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    } else {
      new CheckResult(true, "")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
  }
}
