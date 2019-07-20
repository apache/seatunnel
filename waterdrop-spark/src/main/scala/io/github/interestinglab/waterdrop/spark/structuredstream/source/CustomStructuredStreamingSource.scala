package io.github.interestinglab.waterdrop.spark.structuredstream.source

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.plugin.CheckResult
import org.apache.spark.sql.{ForeachWriter, Row}

abstract class CustomStructuredStreamingSource extends ForeachWriter[Row] with StructuredStreamingSource{

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig: Config = config

  override def checkConfig: CheckResult = new CheckResult(true,"")

  override def prepare(): Unit = {
  }

  override def open(partitionId: Long, epochId: Long) = true


  override def close(errorOrNull: Throwable): Unit = {
  }

}
