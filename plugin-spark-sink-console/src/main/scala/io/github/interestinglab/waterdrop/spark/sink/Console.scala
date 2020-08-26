package io.github.interestinglab.waterdrop.spark.sink

import io.github.interestinglab.waterdrop.config.ConfigFactory
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.batch.SparkBatchSink
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Console extends SparkBatchSink {

  override def output(df: Dataset[Row], env: SparkEnvironment): Unit = {
    val limit = config.getInt("limit")

    config.getString("serializer") match {
      case "plain" => {
        if (limit == -1) {
          df.show(Int.MaxValue, false)
        } else if (limit > 0) {
          df.show(limit, false)
        }
      }
      case "json" => {
        if (limit == -1) {
          df.toJSON.take(Int.MaxValue).foreach(s => println(s))

        } else if (limit > 0) {
          df.toJSON.take(limit).foreach(s => println(s))
        }
      }
    }
  }

  override def checkConfig(): CheckResult = {
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => new CheckResult(true, "")
      case false => new CheckResult(false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "limit" -> 100,
        "serializer" -> "plain" // plain | json
      )
    )
    config = config.withFallback(defaultConfig)
  }
}
