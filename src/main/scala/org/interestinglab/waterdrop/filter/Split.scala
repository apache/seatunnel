package org.interestinglab.waterdrop.filter

import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

class Split(var conf : Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  // TODO: check fields.length == field_types.length if field_types exists
  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("fields") && conf.getStringList("fields").size() > 0 match {
      case true => (true, "")
      case false => (false, "please specify [fields] as a non-empty string list")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "delimiter" -> " ",
        "source_field" -> "raw_message",
        "target_field" -> Json.ROOT
      )
    )

    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val srcField = conf.getString("source_field")
    val keys = conf.getStringList("fields")

    // https://stackoverflow.com/a/33345698/1145750
    conf.getString("target_field") match {
      case Json.ROOT => {
        val func = udf((s: String) => {
          split(s, conf.getString("delimiter"), keys.size())
        })
        var filterDf = df.withColumn(Json.TMP, func(col(srcField)))
        for(i <- 0 until keys.size()) {
          filterDf = filterDf.withColumn(keys.get(i), col(Json.TMP)(i))
        }
        filterDf.drop(Json.TMP)
      }
      case targetField: String => {
        val func = udf((s : String) => {
          val values = split(s, conf.getString("delimiter"), keys.size)
          val kvs = (keys zip values).toMap
          kvs
        })

        df.withColumn(targetField, func(col(srcField)))
      }
    }
  }

  /**
   * Split string by delimiter, if size of splited parts is less than fillLength,
   * empty string is filled; if greater than fillLength, parts will be truncated.
   * */
  private def split(str: String, delimiter: String, fillLength: Int): Seq[String] = {
    val parts = str.split(delimiter).map(_.trim)
    val filled = (fillLength compare parts.size) match {
      case 0 => parts
      case 1 => parts ++ Array.fill[String](fillLength - parts.size)("")
      case -1 => parts.slice(0, fillLength)
    }
    filled.toSeq
  }
}
