package io.github.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.config.{Config, ConfigException, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

class Table extends BaseFilter {

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

    Table.options.foldRight((true, ""))((option, result) => {

      val (lastOptionPassed, msg) = result
      lastOptionPassed match {
        case true => {
          option.get("name") match {
            case Some(value) => {
              val optName = value.asInstanceOf[String]
              val required = option.getOrElse("required", false)

              if (!config.hasPath(optName) && required == true) {
                (false, "[" + optName + "] is requred")
              } else if (config.hasPath(optName)) {
                option.get("type") match {
                  case Some(v) => {
                    val optType = v.asInstanceOf[String]
                    optType match {
                      case "string" => {
                        Try(config.getString(optName)) match {
                          case Success(_) => (true, "")
                          case Failure(_: ConfigException.WrongType) =>
                            (false, "wrong type of [" + optName + "], expected: " + optType)
                          case Failure(ex) => (false, ex.getMessage)
                        }
                      }
                      case "string-list" => {
                        Try(config.getStringList(optName)) match {
                          case Success(_) => (true, "")
                          case Failure(_: ConfigException.WrongType) =>
                            (false, "wrong type of [" + optName + "], expected: " + optType)
                          case Failure(ex) => (false, ex.getMessage)
                        }
                      }
                      case "boolean" => {
                        Try(config.getBoolean(optName)) match {
                          case Success(_) => (true, "")
                          case Failure(_: ConfigException.WrongType) =>
                            (false, "wrong type of [" + optName + "], expected: " + optType)
                          case Failure(ex) => (false, ex.getMessage)
                        }
                      }
                      case "integer" => {
                        Try(config.getInt(optName)) match {
                          case Success(_) => (true, "")
                          case Failure(_: ConfigException.WrongType) =>
                            (false, "wrong type of [" + optName + "], expected: " + optType)
                          case Failure(ex) => (false, ex.getMessage)
                        }
                      }
                      case s: String => (false, "[Plugin Bug] unrecognized option type: " + s)
                    }
                  }
                  case None => (true, "")
                }
              } else {
                (true, "")
              }
            }
            case None => (true, "")
          }
        }

        case false => result
      }
    })
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
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
    val encoder = RowEncoder(StructType(initialSchema))
    var df = spark.createDataset(strRDD)(encoder)

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

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    df
  }
}

object Table {
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
