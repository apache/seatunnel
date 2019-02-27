package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Kudu extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "mode" -> "insert"
      )
    )

    this.config = config.withFallback(defaultConfig)
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("kudu_master") && config.hasPath("kudu_table") match {
      case true => (true, "")
      case false => (false, "please specify [kudu_master] and [kudu_table] ")
    }
  }

  override def process(df: Dataset[Row]): Unit = {

    val kuduContext = new KuduContext(config.getString("kudu_master"), df.sparkSession.sparkContext)

    val table = config.getString("kudu_table")

    config.getString("mode") match {
      case "insert" => kuduContext.insertRows(df, table)
      case "update" => kuduContext.updateRows(df, table)
      case "upsert" => kuduContext.upsertRows(df, table)
      case "insertIgnore" => kuduContext.insertIgnoreRows(df, table)
    }
  }
}
