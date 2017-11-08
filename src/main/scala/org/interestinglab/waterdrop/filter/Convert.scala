package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.types._

class Convert(var conf: Config) extends BaseFilter(conf) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    if (!conf.hasPath("source_field")) {
      (false, "please specify [source_field] as a non-empty string")
    } else if (!conf.hasPath("to")){
      (true, "please specify [to] as a non-empty string")
    } else {
      (true, "")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    super.prepare(spark, ssc)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val srcField = conf.getString("source_field")
    val to = conf.getString("to")

    to match {
      //TODO df.withColumn(srcField, df.col(srcField).cast("string"))
      case "string" => df.withColumn(srcField, df.col(srcField).cast(StringType))
      case "integer" => df.withColumn(srcField, df.col(srcField).cast(IntegerType))
      case "double" => df.withColumn(srcField, df.col(srcField).cast(DoubleType))
      case "float" => df.withColumn(srcField, df.col(srcField).cast(FloatType))
      case "long" => df.withColumn(srcField, df.col(srcField).cast(LongType))
      case "boolean" => df.withColumn(srcField, df.col(srcField).cast(BooleanType))
      case toType: String => df
    }
  }
}
