package io.github.interestinglab.waterdrop.input.sparkstreaming

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStreamingInput
import io.github.interestinglab.waterdrop.receiver.redis.{RedisInfo, RedisReceiver}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

class RedisStream extends BaseStreamingInput[String] {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "host" -> "127.0.0.1:6379",
        "prefKey" -> "",
        "queue" -> "",
        "password" -> "",
        "maxTotal" -> 200,
        "maxIdle" -> 200,
        "maxWaitMillis" -> 2000,
        "connectionTimeout" -> 5000,
        "soTimeout" -> 5000,
        "maxAttempts" -> 5
      ))
    config = config.withFallback(defaultConfig)
  }

  override def getDStream(ssc: StreamingContext): DStream[String] = {
    val redisInfo = new RedisInfo(config)
    ssc.receiverStream(new RedisReceiver(redisInfo))
  }

  override def rdd2dataset(spark: SparkSession, rdd: RDD[String]): Dataset[Row] = {

    val rowsRDD = rdd.map(element => {
      RowFactory.create(element)
    })

    val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
    spark.createDataFrame(rowsRDD, schema)
  }
}

