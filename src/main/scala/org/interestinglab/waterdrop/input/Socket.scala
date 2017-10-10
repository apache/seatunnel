package org.interestinglab.waterdrop.input

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class Socket(config: Config) extends BaseInput(config) {

  var dstream: Option[DStream[(String, String)]] = None

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "host" -> "localhost",
        "port" -> 9999
      ))
    val mergedConfig = config.withFallback(defaultConfig)

    dstream = Some(
      ssc
        .socketTextStream(mergedConfig.getString("host"), mergedConfig.getInt("port"))
        .map(s => {
          ("", s)
        }))
  }

  override def getDStream: DStream[(String, String)] = {
    dstream.orNull
  }
}
