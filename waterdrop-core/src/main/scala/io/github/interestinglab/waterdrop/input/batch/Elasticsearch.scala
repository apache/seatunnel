package io.github.interestinglab.waterdrop.input.batch

import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.functions._
import org.elasticsearch.spark._
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import io.github.interestinglab.waterdrop.core.RowConstant
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class Elasticsearch extends BaseStaticInput {

  var esCfg: Map[String, String] = Map()
  val esPrefix = "es."
  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def prepare(spark: SparkSession): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "query_string" -> "*",
        "source_type" -> "nested"
      )
    )
    config = config.withFallback(defaultConfig)

    if (TypesafeConfigUtils.hasSubConfig(config, esPrefix)) {
      val esConfig = TypesafeConfigUtils.extractSubConfig(config, esPrefix, false)
      esConfig
        .entrySet()
        .foreach(entry => {
          val key = entry.getKey
          val value = String.valueOf(entry.getValue.unwrapped())
          esCfg += (esPrefix + key -> value)
        })
    }

    esCfg += ("es.nodes" -> config.getStringList("hosts").mkString(","))

    println("[INFO] Input ElasticSearch Params:")
    for (entry <- esCfg) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("hosts") && config.hasPath("index") && config.getStringList("hosts").size() > 0 match {
      case true => {
        // val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [hosts] as a non-empty string list")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {

    import spark.implicits._

    val index = config.getString("index")
    val queryString = config.getString("query_string")
    val sourceType = config.getString("source_type")
    val rdd = spark.sparkContext.esJsonRDD(index, "?q=" + queryString, esCfg)

    val df = rdd
      .toDF()
      .withColumnRenamed("_1", "_id")
      .withColumnRenamed("_2", "_source")

    if (sourceType == "string") {
      df
    } else {
      val jsonRDD = df.select("_source").as[String].rdd
      val schema = spark.read.json(jsonRDD).schema

      sourceType match {
        case "nested" =>
          df.withColumn(RowConstant.TMP, from_json(col("_source"), schema))
            .drop("_source")
            .withColumnRenamed(RowConstant.TMP, "_source")
        case "flatten" => {
          var tmpDf = df.withColumn(RowConstant.TMP, from_json(col("_source"), schema))
          schema.map { field =>
            tmpDf = tmpDf.withColumn(field.name, col(RowConstant.TMP)(field.name))
          }
          tmpDf.drop(RowConstant.TMP)
        }
      }
    }

  }
}
