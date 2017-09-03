package org.interestinglab.waterdrop.filter


import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Split(var conf: Config) extends BaseFilter(conf) {

  def checkConfig(): (Boolean, String) = {
    // TODO
    (true, "")
  }

  override def prepare(sqlContext:SparkSession) {

    this.sqlContext = sqlContext
    if (!conf.hasPath("delimiter")) {
      conf = conf.withValue("delimiter", ConfigValueFactory.fromAnyRef(" "))
    }
  }

//  def prepare(ssc: StreamingContext) {
//
//    if (!conf.hasPath("target_field")) {
//      conf = conf.withValue("target_field", ConfigValueFactory.fromAnyRef("__root__"))
//    }
//  }

  def process(df: DataFrame): DataFrame = {

    val sqlContext = this.sqlContext

    import sqlContext.implicits._

    val srcField = conf.getString("source_field")
    val keys = conf.getStringList("keys")
    if (conf.hasPath("target_field")) {
      val targetField = conf.getString("target_field")
      val func = udf((s:String) => {
        val parts = s.split(conf.getString("delimiter")).map(_.trim)
        val kvs = (keys zip parts).toMap
        kvs
      })

      df.withColumn(targetField, func($"$srcField"))
    } else {
      val rows = df.rdd.map { r =>
        Row.fromSeq(r.toSeq ++ udfFunc(r.getAs[String](srcField)))
      }

      val schema = StructType(df.schema.fields ++ structField())

      sqlContext.createDataFrame(rows, schema)
    }
  }

  def udfFunc(str : String) : Seq[Any] = {

    val parts = str.split(conf.getString("delimiter")).map(_.trim)
    parts.toSeq
  }

  def structField() : Array[StructField] = {
    import org.apache.spark.sql.types._

    val keysList = conf.getStringList("keys")
    val keys = keysList.toArray

    keys.map(key => StructField(key.asInstanceOf[String], StringType))
  }
}
