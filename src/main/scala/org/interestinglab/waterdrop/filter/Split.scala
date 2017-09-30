package org.interestinglab.waterdrop.filter

import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConversions._

class Split(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

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

    import spark.sqlContext.implicits

    val srcField = conf.getString("source_field")
    val keys = conf.getStringList("fields")

    // https://stackoverflow.com/a/33345698/1145750
    conf.getString("target_field") match {
      case Json.ROOT => {
        val rows = df.rdd.map { r =>
          Row.fromSeq(r.toSeq ++ udfFunc(r.getAs[String](srcField)))
        }

        val schema = StructType(df.schema.fields ++ structField())

        spark.createDataFrame(rows, schema)
      }
      case targetField : String => {
        val func = udf((s: String) => {
          val parts = s.split(conf.getString("delimiter")).map(_.trim)
          val kvs = (keys zip parts).toMap
          kvs
        })

        df.withColumn(targetField, func(col(srcField)))
      }
    }
  }

  private def udfFunc(str: String): Seq[Any] = {

    val parts = str.split(conf.getString("delimiter")).map(_.trim)
    parts.toSeq
  }

  private def structField(): Array[StructField] = {
    import org.apache.spark.sql.types._

    val keysList = conf.getStringList("fields")
    val keys = keysList.toArray

    keys.map(key => StructField(key.asInstanceOf[String], StringType))
  }
}
