package org.interestinglab.waterdrop.filter

import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import com.maxmind.geoip2.DatabaseReader
import java.io.File
import java.net.{InetAddress, UnknownHostException}
import com.maxmind.db.CHMCache
import scala.collection.JavaConversions._


class GeoIp(var conf : Config) extends BaseFilter(conf) {

  val fieldsArr = Array("country_name", "subdivision_name", "city_name")
  var selectedFields = Array[String]()

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("database") match {
      case true => (true, "")
      case false => (false, "please specify [database] as a non-empty string")
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

    conf = conf.withFallback(defaultConfig)

    selectedFields = fieldsArr.filter(field => conf.getBoolean(field))

  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val database = new File(conf.getString("database"))
    val source = conf.getString("source_field")

    val reader = new DatabaseReader.Builder(database).build
    val broadcastReader = spark.sparkContext.broadcast(reader)

    conf.getString("target_field") match {
      case Json.ROOT => df
      case target: String => {
        val func = udf((s : String) => {

          var geo: Map[String, String] = Map()
          val geoIp = ipLookUp(s, broadcastReader.value)

          if (conf.getBoolean("country_name")) {
            geo += ("country_name" -> geoIp("country_name"))
          }
          if (conf.getBoolean("subdivision_name")) {
            geo += ("subdivision_name" -> geoIp("subdivision_name"))
          }
          if (conf.getBoolean("city_name")) {
            geo += ("city_name" -> geoIp("city_name"))
          }
          geo
        })
        df.withColumn(target, func(col(source)))
      }
    }
  }

  def ipLookUp(ip: String, reader: DatabaseReader): Map[String, String] = {
    var geoIp = Map(
      "country_name" -> "None",
      "subdivision_name" -> "None",
      "city_name" -> "None"
    )

    InetAddress.getByName(ip) match {
      case address: InetAddress => {
        try {
          val response = reader.city(address)
          val country = response.getCountry()
          val subdivision = response.getMostSpecificSubdivision()
          val city = response.getCity()
          geoIp += ("country_name" -> country.getName)
          geoIp += ("subdivision_name" -> subdivision.getName)
          geoIp += ("city_name" -> city.getName)

        } catch {
          case _: UnknownHostException => {
            // TODO
          }
        }
      }
      case _ => geoIp
    }

    geoIp
  }
}
