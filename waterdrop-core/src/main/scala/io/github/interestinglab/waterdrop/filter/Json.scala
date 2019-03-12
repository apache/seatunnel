package io.github.interestinglab.waterdrop.filter

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.{Common, ConfigRuntimeException}
import io.github.interestinglab.waterdrop.core.RowConstant
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.{Failure, Success, Try}

class Json extends BaseFilter {

  var conf: Config = ConfigFactory.empty()
  var customSchema: StructType = new StructType()
  var useCustomSchema: Boolean = false

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("source_field") match {
      case true => (true, "")
      case false => (false, "please specify [source_field] as a non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT,
        "schema_dir" -> Paths
          .get(Common.pluginFilesDir("json").toString, "schemas")
          .toString,
        "schema_file" -> ""
      )
    )
    conf = conf.withFallback(defaultConfig)
    val schemaFile = conf.getString("schema_file")
    if (schemaFile.trim != "") {
      parseCustomJsonSchema(spark, conf.getString("schema_dir"), schemaFile)
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val srcField = conf.getString("source_field")

    import spark.implicits._

    conf.getString("target_field") match {
      case RowConstant.ROOT => {

        val jsonRDD = df.select(srcField).as[String].rdd

        val newDF = srcField match {
          // for backward-compatibility for spark < 2.2.0, we created rdd, not Dataset[String]
          case "raw_message" => {
            val tmpDF = if (this.useCustomSchema) {
              spark.read.schema(this.customSchema).json(jsonRDD)
            } else {
              spark.read.json(jsonRDD)
            }

            tmpDF
          }
          case s: String => {
            val schema = if (this.useCustomSchema) this.customSchema else spark.read.json(jsonRDD).schema
            var tmpDf = df.withColumn(RowConstant.TMP, from_json(col(s), schema))
            schema.map { field =>
              tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
            }
            tmpDf.drop(RowConstant.TMP)
          }
        }

        newDF
      }
      case targetField: String => {
        // for backward-compatibility for spark < 2.2.0, we created rdd, not Dataset[String]
        val schema = this.useCustomSchema match {
          case true => {
            this.customSchema
          }
          case false => {
            val jsonRDD = df.select(srcField).as[String].rdd
            spark.read.json(jsonRDD).schema
          }
        }
        df.withColumn(targetField, from_json(col(srcField), schema))
      }
    }
  }

  private def parseCustomJsonSchema(spark: SparkSession, dir: String, file: String): Unit = {
    val fullPath = dir.endsWith("/") match {
      case true => dir + file
      case false => dir + "/" + file
    }
    println("[INFO] specify json schema file path: " + fullPath)
    val path = new File(fullPath)
    if (path.exists && !path.isDirectory) {
      // try to load json schema from driver node's local file system, instead of distributed file system.
      val source = Source.fromFile(path.getAbsolutePath)

      var schemaLines = ""
      Try(source.getLines().toList.mkString) match {
        case Success(schema: String) => {
          schemaLines = schema
          source.close()
        }
        case Failure(_) => {
          source.close()
          throw new ConfigRuntimeException("Loading file of " + fullPath + " failed.")
        }
      }
      val schemaRdd = spark.sparkContext.parallelize(List(schemaLines))
      val schemaJsonDF = spark.read.option("multiline", true).json(schemaRdd)
      schemaJsonDF.printSchema()
      val schemaJson = schemaJsonDF.schema.json
      this.customSchema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
      this.useCustomSchema = true
    }
  }
}
