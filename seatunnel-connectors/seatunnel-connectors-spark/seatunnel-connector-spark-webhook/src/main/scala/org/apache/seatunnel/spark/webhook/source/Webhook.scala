package org.apache.seatunnel.spark.webhook.source

import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.stream.SparkStreamingSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.streaming.dstream.DStream

class Webhook extends SparkStreamingSource[String] {

  override def start(env: SparkEnvironment, handler: Dataset[Row] => Unit): Unit = {
    var spark = env.getSparkSession
    // Create HTTP Server and start streaming
    implicit val sqlContext: SQLContext = spark.sqlContext

    var port = if (config.hasPath("port")) config.getInt("port") else 9999
    var baseUrl = if (config.hasPath("path")) config.getString("path") else "/"

    val query = new JettyServerStream(port, baseUrl)
      .toDF
      .writeStream
      .foreachBatch((batch, batchId) => {
        handler(batch)
      })
      .start()

    query.awaitTermination()
  }

  override def rdd2dataset(sparkSession: SparkSession, rdd: RDD[String]): Dataset[Row] = { null }

  override def getData(env: SparkEnvironment): DStream[String] = { null }
}
