package io.github.interestinglab.waterdrop.spark.structuredstream

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.plugin.CheckResult
import org.apache.spark.sql.{ForeachWriter, Row}

trait CustomStructuredStreamingSource extends ForeachWriter[Row] with StructuredStreamingSource{

  override def checkConfig: CheckResult = new CheckResult(true,"")

  override def prepare(): Unit = {
  }

  override def open(partitionId: Long, epochId: Long) = true


  override def close(errorOrNull: Throwable): Unit = {
  }

}
