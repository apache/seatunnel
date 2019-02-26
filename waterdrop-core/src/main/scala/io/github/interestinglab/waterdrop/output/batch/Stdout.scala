package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Stdout extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => (true, "")
      case false => (false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "limit" -> 100,
        "serializer" -> "plain" // plain | json
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {

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
}
