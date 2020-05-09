package io.github.interestinglab.waterdrop.spark.source

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingSource
import java.security.SecureRandom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

class FakeStream extends SparkStreamingSource[String] {

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "rate" -> 1 // rate per second, X records/sec
      ))
    config = config.withFallback(defaultConfig)
  }

  override def getData(env: SparkEnvironment): DStream[String] = {
    val receiverInputDStream = env.getStreamingContext.receiverStream(new FakeReceiver(config))
    receiverInputDStream
  }

  override def rdd2dataset(sparkSession: SparkSession,
                           rdd: RDD[String]): Dataset[Row] = {
    val rowsRDD = rdd.map(element => {
      RowFactory.create(element)
    })
    val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
    sparkSession.createDataFrame(rowsRDD, schema)
  }

  override def checkConfig(): CheckResult = {

    if (config.hasPath("content") && config.getStringList("content").nonEmpty) {
      new CheckResult(true, "")
    } else {
      new CheckResult(false, "please make sure [content] is of type string array")
    }
  }
}

private class FakeReceiver(config: Config) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  val secRandom = new SecureRandom()

  def generateData(): String = {
    val contentList = config.getStringList("content")
    val n = secRandom.nextInt(contentList.length)
    contentList.get(n)
  }

  def onStart() {
    // Start the thread that receives data over a connection

    new Thread("FakeReceiver Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while (!isStopped()) {

      store(generateData())
      Thread.sleep((1000.toDouble / config.getInt("rate")).toInt)
    }
  }
}