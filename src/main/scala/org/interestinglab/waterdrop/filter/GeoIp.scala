package org.interestinglab.waterdrop.filter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.interestinglab.waterdrop.utils.DatabaseReader

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import java.io.File
import java.net.InetAddress
import scala.collection.JavaConversions._


class GeoIp(var config : Config) extends BaseFilter(config) {

  val fieldsArr = Array("country_name", "subdivision_name", "city_name")
  var selectedFields = Array[String]()

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("source_field", "database")

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.length == 0) {
      (true, "")
    } else {
      (false, "please specify " + nonExistsOptions.map("[" + _._1 + "]").mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> Json.ROOT,
        // "country_code" -> true,
        "country_name" -> true,
        // "country_isocode" -> false,
        "subdivision_name" -> true,
        "city_name" -> true
        // "latitude" -> false,
        // "longitude" -> false,
        // "location" -> false
      )
    )

    config = config.withFallback(defaultConfig)

    selectedFields = fieldsArr.filter(field => config.getBoolean(field))
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val database = new File(config.getString("database"))
    val source = config.getString("source_field")

    val reader = new DatabaseReader.Builder(database).build
    val broadcastReader = spark.sparkContext.broadcast(reader)

    val func = udf((s : String) => {

      var geo: Map[String, String] = Map()
      val geoIp = ipLookUp(s, broadcastReader.value)

      if (config.getBoolean("country_name")) {
        geo += ("country_name" -> geoIp("country_name"))
      }
      if (config.getBoolean("subdivision_name")) {
        geo += ("subdivision_name" -> geoIp("subdivision_name"))
      }
      if (config.getBoolean("city_name")) {
        geo += ("city_name" -> geoIp("city_name"))
      }
      geo    })
    config.getString("target_field") match {
      case Json.ROOT => {
        var tmpDf = df.withColumn(Json.TMP, func(col(source)))
        for(key <- selectedFields) {
          tmpDf = tmpDf.withColumn(key, col(Json.TMP)(key))
        }
        tmpDf.drop(Json.TMP)
      }
      case target: String => {
        df.withColumn(target, func(col(source)))
      }
    }
  }

  def ipLookUp(ip: String, reader: DatabaseReader): Map[String, String] = {
    val emptyResult = Map(
      "country_name" -> "None",
      "subdivision_name" -> "None",
      "city_name" -> "None"
    )

    InetAddress.getByName(ip) match {
      case address: InetAddress => {
        Try(reader.city(address)) match {
          case Success(response) => {
            Map(
              "country_name" -> response.getCountry.getName,
              "subdivision_name" -> response.getMostSpecificSubdivision.getName,
              "city_name" -> response.getCity.getName
            )
          }
          case Failure(exception) => {
            // TODO: log non-fatal exception.getMessage
            emptyResult
          }
        }
      }
      case NonFatal(e) => emptyResult
    }
  }
}
