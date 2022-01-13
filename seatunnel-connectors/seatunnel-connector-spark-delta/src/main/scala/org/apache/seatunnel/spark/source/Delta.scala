package org.apache.seatunnel.spark.source

import org.apache.seatunnel.common.config.CheckConfigUtil.check

import scala.collection.JavaConversions._
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSource
import org.apache.spark.sql.{Dataset, Row}

class Delta extends SparkBatchSource {

  override def prepare(env: SparkEnvironment): Unit = {}

  override def checkConfig(): CheckResult = {
    check(config, "delta.read.path")
  }

  override def getData(env: SparkEnvironment): Dataset[Row] = {

    val reader = env.getSparkSession.read.format("delta")
    for (e <- config.entrySet()) {
      reader.option(e.getKey, e.getValue.toString)
    }

    reader.load(config.getString("delta.read.path"))
  }
}
