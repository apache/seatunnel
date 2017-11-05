package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.col

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe

class Table(var config: Config) extends BaseFilter(config) {

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {

    // TODO:
    (true, "")
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        // Cache(cache=true) dataframe to avoid reloading data every time this dataframe get used.
        // This will consumes memory usage. don't do this on large dataframe
        // Also, if you want to keep data updated, set cache = false
        "cache" -> true,
        "delimiter" -> ","
      )
    )
    config = config.withFallback(defaultConfig)

    val strRDD = spark.sparkContext
      .textFile(config.getString("path"))
      .map { str =>
        str.split(config.getString("delimiter")).map(s => s.trim)
      }
      .map(s => Row.fromSeq(s))

    strRDD.collect().foreach(println(_)) // debug

    val fieldNames = config.getStringList("fields")

    val initialSchema = fieldNames.map(name => StructField(name, StringType))
    var df = spark.createDataFrame(strRDD, StructType(initialSchema))

    df = config.hasPath("field_types") match {
      case true => {
        fieldNames.zip(config.getStringList("field_types")).foldRight(df) { (field, df1) =>
          val (name, typeStr) = field
          typeStr.toLowerCase.trim match {
            case "string" => df1
            case s: String => {
              // change column type if necessary
              val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
              val module =
                runtimeMirror.staticModule("org.apache.spark.sql.types." + s.capitalize + "Type")
              val obj = runtimeMirror.reflectModule(module)
              val dataType = obj.instance.asInstanceOf[DataType]
              df1.withColumn(name, col(name).cast(dataType))
            }
          }
        }
      }
      case false => df
    }

    df.printSchema()

    config.getBoolean("cache") match {
      case true => df.cache()
      case false =>
    }

    df.createOrReplaceTempView(config.getString("table_name"))
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {
    df
  }
}

object Table {
  // TODO: maybe we can turn option map to class
  val options = List(
    Map(
      "name" -> "path",
      "type" -> "string",
      "required" -> true,
      "defaultValue" -> None,
      "allowedValues" -> None,
      "checkers" -> List()),
    Map(
      "name" -> "delimiter",
      "type" -> "string",
      "required" -> false,
      "defaultValue" -> ",",
      "allowedValues" -> None,
      "checkers" -> List()),
    Map(
      "name" -> "table_name",
      "type" -> "string",
      "required" -> true,
      "defaultValue" -> None,
      "allowedValues" -> None,
      "checkers" -> List()),
    Map(
      "name" -> "fields",
      "type" -> "string-list",
      "required" -> true,
      "defaultValue" -> None,
      "allowedValues" -> None,
      "checkers" -> List()),
    Map(
      "name" -> "field_types",
      "type" -> "string-list",
      "required" -> false,
      "defaultValue" -> None,
      "allowedValues" -> None,
      "checkers" -> List()),
    Map(
      "name" -> "cache",
      "type" -> "boolean",
      "required" -> false,
      "defaultValue" -> true,
      "allowedValues" -> None,
      "checkers" -> List())
  )
}
