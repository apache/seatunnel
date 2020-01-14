package io.github.interestinglab.waterdrop.spark.transform

import com.typesafe.config.waterdrop.ConfigFactory
import io.github.interestinglab.waterdrop.common.config.{CheckResult, ConfigRuntimeException, Common}
import io.github.interestinglab.waterdrop.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import java.io.File
import java.nio.file.Paths

import io.github.interestinglab.waterdrop.common.RowConstant
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.{Failure, Success, Try}

class Json extends BaseSparkTransform {

  var customSchema: StructType = new StructType()
  var useCustomSchema: Boolean = false

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val srcField = config.getString("source_field")
    val spark = env.getSparkSession

    import spark.implicits._

    config.getString("target_field") match {
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

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(env: SparkEnvironment): Unit = {
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
    config = config.withFallback(defaultConfig)
    val schemaFile = config.getString("schema_file")
    if (schemaFile.trim != "") {
      parseCustomJsonSchema(env.getSparkSession, config.getString("schema_dir"), schemaFile)
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
