package org.interestinglab.waterdrop.filter

import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

class Split(var conf: Config) extends BaseFilter(conf) {

  override def checkConfig(): (Boolean, String) = {
    // TODO
    (true, "")
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    if (!conf.hasPath("delimiter")) {
      conf = conf.withValue("delimiter", ConfigValueFactory.fromAnyRef(" "))
    }
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    import spark.sqlContext.implicits

    val srcField = conf.getString("source_field")

    val keys = conf.getStringList("keys")
    if (conf.hasPath("target_field")) {
      val targetField = conf.getString("target_field")
      val func = udf((s: String) => {
        val parts = s.split(conf.getString("delimiter")).map(_.trim)
        val kvs = (keys zip parts).toMap
        kvs
      })

      df.withColumn(targetField, func(col(srcField)))
    } else {
      val rows = df.rdd.map { r =>
        Row.fromSeq(r.toSeq ++ udfFunc(r.getAs[String](srcField)))
      }

      val schema = StructType(df.schema.fields ++ structField())

      // TODO: 为什么要createDataFrame
      spark.createDataFrame(rows, schema)
    }
  }

  private def udfFunc(str: String): Seq[Any] = {

    val parts = str.split(conf.getString("delimiter")).map(_.trim)
    parts.toSeq
  }

  private def structField(): Array[StructField] = {
    import org.apache.spark.sql.types._

    val keysList = conf.getStringList("keys")
    val keys = keysList.toArray

    keys.map(key => StructField(key.asInstanceOf[String], StringType))
  }
}
