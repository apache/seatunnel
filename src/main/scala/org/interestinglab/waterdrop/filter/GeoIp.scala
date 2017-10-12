package org.interestinglab.waterdrop.filter

import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import com.maxmind.geoip2.DatabaseReader
import java.io.File
import java.net.InetAddress
import scala.collection.JavaConversions._

class GeoIp(var conf : Config) extends BaseFilter(conf) {

  var reader:DatabaseReader = _

  def this() = {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("database") match {
      case true => (true, "")
      case false => (false, "please specify [fields] as a non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source" -> "raw_message",
        "target" -> Json.ROOT,
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

    val database = new File(conf.getString("database"))
    this.reader = new DatabaseReader.Builder(database).build

  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    import spark.sqlContext.implicits

    val source = conf.getString("source")

    conf.getString("target") match {
      case Json.ROOT => {
        val valueSchema = structField()
        val rows = df.rdd.map { r =>
          val ip = r.getAs[String](source)
          // TODO
          val values =  Seq("")
          Row.fromSeq(r.toSeq ++ values)
        }
        val schema = StructType(df.schema.fields ++ valueSchema)

        spark.createDataFrame(rows, schema)

      }
      case target: String => {
        val func = udf((s: String) => {
          var geoIp: Map[String, String] = Map()
          val ipAddress = InetAddress.getByName(s) match {
            case address: InetAddress => address
            case _ => InetAddress.getByName("localhost")
          }

          try {
            val response = reader.city(ipAddress)
            val country = response.getCountry()
            val subdivision = response.getMostSpecificSubdivision()
            val city = response.getCity()
            if (conf.getBoolean("country_name")) {
              geoIp += ("country_name" -> country.getName)
            }
            if (conf.getBoolean("subdivision_name")) {
              geoIp += ("subdivision_name" -> subdivision.getName)
            }
            if (conf.getBoolean("city_name")) {
              geoIp += ("city_name" -> city.getName)
            }
          } catch {
            case _: Throwable => {
              //TODO
            }
          }

          geoIp
        })
        df.withColumn(target, func(col(source)))
      }
    }
  }

  def structField(): Array[StructField] = {

    val fieldsArr = Array("country_name", "subdivision_name", "city_name")
    fieldsArr.filter(index => conf.getBoolean(index)).map(key =>
      StructField(key, StringType)
    )
  }
}
