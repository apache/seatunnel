package org.interestinglab.waterdrop.input

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.JavaConversions._
import scala.util.Random

class Fake(var config: Config) extends BaseInput(config) {

  override def checkConfig(): (Boolean, String) = (true, "")

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "rate" -> 1000 // rate per second
      ))
    config = config.withFallback(defaultConfig)
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    val receiverInputDStream = ssc.receiverStream(new FakeReceiver(config.getInt("rate")))
    receiverInputDStream.map(s => { ("", s) })
  }
}

private class FakeReceiver(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  // TODO: 生成数据的格式，text, json, 字段个数
  def generateData(): String = {
    val fromN = 1
    val toN = 100
    val n = 100000
    (fromN to toN).map(i => "Random" + i + Random.nextInt(n)).mkString(",")
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
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
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}
