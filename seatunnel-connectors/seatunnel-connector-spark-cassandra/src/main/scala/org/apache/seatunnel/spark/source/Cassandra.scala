package org.apache.seatunnel.spark.source

import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Cassandra extends SparkBatchSource {

  override def getData(env: SparkEnvironment): Dataset[Row] = {

    env.getSparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(Map(
      "table" -> config.getString("table.name"),
      "keyspace" -> config.getString("keyspace"),
      "cluster" -> config.getString("cluster")
    )).load()
  }

  override def checkConfig(): CheckResult = {
    val requiredOptions = List("table.name", "keyspace");

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
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "cluster" -> "default"))
    config = config.withFallback(defaultConfig)
  }
}
