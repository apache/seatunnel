package org.apache.seatunnel.spark.sink

import scala.collection.JavaConversions._

import org.apache.seatunnel.common.config.CheckConfigUtil.check
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

class Delta extends SparkBatchSink {

  override def checkConfig(): CheckResult = {
    check(config, "delta.sink.path")
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "save_mode" -> "append"))
    config = config.withFallback(defaultConfig)
  }

  override def output(df: Dataset[Row], environment: SparkEnvironment): Unit = {
    val writer = df.write.format("delta")
    for (e <- config.entrySet()) {
      writer.option(e.getKey, e.getValue.toString)
    }
    writer.mode(config.getString("save_mode"))
      .save(config.getString("delta.sink.path"))
  }
}
