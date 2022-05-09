/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.spark.transform

import org.apache.seatunnel.common.config.{Common, ConfigRuntimeException}
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.seatunnel.spark.transform.JsonConfig._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Paths

import org.apache.seatunnel.common.Constants

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.{Failure, Success, Try}

class Json extends BaseSparkTransform {

  private val LOGGER = LoggerFactory.getLogger(classOf[Json])

  var customSchema: StructType = new StructType()
  var useCustomSchema: Boolean = false

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val srcField = config.getString(SOURCE_FILED)
    val spark = env.getSparkSession

    import spark.implicits._

    config.getString(TARGET_FILED) match {
      case Constants.ROW_ROOT => {

        val jsonRDD = df.select(srcField).as[String].rdd

        val newDF = srcField match {
          // for backward-compatibility for spark < 2.2.0, we created rdd, not Dataset[String]
          case DEFAULT_SOURCE_FILED => {
            val tmpDF =
              if (this.useCustomSchema) {
                spark.read.schema(this.customSchema).json(jsonRDD)
              } else {
                spark.read.json(jsonRDD)
              }

            tmpDF
          }
          case s: String => {
            val schema =
              if (this.useCustomSchema) this.customSchema else spark.read.json(jsonRDD).schema
            var tmpDf = df.withColumn(Constants.ROW_TMP, from_json(col(s), schema))
            schema.map { field =>
              tmpDf = tmpDf.withColumn(field.name, col(Constants.ROW_TMP)(field.name))
            }
            tmpDf.drop(Constants.ROW_TMP)
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

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        SOURCE_FILED -> DEFAULT_SOURCE_FILED,
        TARGET_FILED -> Constants.ROW_ROOT,
        SCHEMA_DIR -> Paths.get(Common.pluginFilesDir("json").toString, "schemas").toString,
        SCHEMA_FILE -> DEFAULT_SCHEMA_FILE))
    config = config.withFallback(defaultConfig)
    val schemaFile = config.getString(SCHEMA_FILE)
    if (schemaFile.trim != "") {
      parseCustomJsonSchema(env.getSparkSession, config.getString(SCHEMA_DIR), schemaFile)
    }
  }

  private def parseCustomJsonSchema(spark: SparkSession, dir: String, file: String): Unit = {
    val fullPath = dir.endsWith("/") match {
      case true => dir + file
      case false => dir + "/" + file
    }
    LOGGER.info("specify json schema file path: " + fullPath)
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

  override def getPluginName: String = PLUGIN_NAME
}
