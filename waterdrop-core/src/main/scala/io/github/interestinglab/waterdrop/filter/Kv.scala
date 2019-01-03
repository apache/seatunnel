package io.github.interestinglab.waterdrop.filter

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.core.RowConstant
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.JavaConversions._

class Kv extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

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
    conf.hasPath("include_fields") && conf.hasPath("exclude_fields") match {
      case true => (false, "[include_fields] and [exclude_fields] must not be specified at the same time")
      case false => (true, "")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "field_split" -> "&",
        "value_split" -> "=",
        "field_prefix" -> "",
        "include_fields" -> util.Arrays.asList(),
        "exclude_fields" -> util.Arrays.asList(),
        "default_values" -> util.Arrays.asList(),
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT
      )
    )
    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    import spark.implicits._

    val kvUDF = udf((s: String) => kv(s))

    var df2 = df.withColumn(RowConstant.TMP, kvUDF(col(conf.getString("source_field"))))

    conf.getString("target_field") match {
      case RowConstant.ROOT => {
        val schema = inferSchemaOfMapType(spark, df2, RowConstant.TMP)
        schema.fields map { field =>
          df2 = df2.withColumn(field.name, col(RowConstant.TMP)(field.name))
        }

        df2.drop(RowConstant.TMP)
      }

      case targetField: String => {
        df2.withColumnRenamed(RowConstant.TMP, targetField)
      }
    }
  }

  private def inferSchemaOfMapType(spark: SparkSession, ds: Dataset[Row], columnName: String): StructType = {

    import spark.implicits._

    // backward-compatibility for spark < 2.3,
    val mapKeys = udf[Seq[String], Map[String, Row]](_.keys.toSeq)

    // for spark >= 2.3
    // import org.apache.spark.sql.functions.map_keys

    val keys = ds
      .select(mapKeys(col(columnName)))
      .as[Seq[String]]
      .flatMap(identity)
      .distinct()
      .collect()
      .sorted

    var structFields = Seq[StructField]()
    keys map { key =>
      structFields = structFields :+ DataTypes.createStructField(key, DataTypes.StringType, true)
    }

    DataTypes.createStructType(structFields)
  }

  private def kv(str: String): Map[String, String] = {

    val includeFields = conf.getStringList("include_fields")
    val excludeFields = conf.getStringList("exclude_fields")
    val defaultValues = conf.getStringList("default_values")

    var map: Map[String, String] = Map()
    str
      .split(conf.getString("field_split"))
      .foreach(s => {
        val pair = s.split(conf.getString("value_split"))
        if (pair.size == 2) {
          val key = pair(0).trim
          val value = pair(1).trim

          if (includeFields.length == 0 && excludeFields.length == 0) {
            map += (conf.getString("field_prefix") + key -> value)
          } else if (includeFields.length > 0 && includeFields.contains(key)) {
            map += (conf.getString("field_prefix") + key -> value)
          } else if (excludeFields.length > 0 && !excludeFields.contains(key)) {
            map += (conf.getString("field_prefix") + key -> value)
          }
        }
      })

    defaultValues foreach { dv =>
      val pair = dv.split("=")
      if (pair.size == 2) {
        val key = pair(0).trim
        val value = pair(1).trim

        if (!map.contains(key)) {
          map += (key -> value)
        }
      }
    }

    map
  }
}
