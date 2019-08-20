package io.github.interestinglab.waterdrop.spark.stream

import io.github.interestinglab.waterdrop.spark.{
  BaseSparkSource,
  SparkEnvironment
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

trait SparkStreamingSource[T] extends BaseSparkSource[DStream[T]] {

  def beforeOutput(): Unit = {}

  def afterOutput(): Unit = {}

  def rdd2dataset(sparkSession: SparkSession, rdd: RDD[T]): Dataset[Row]

  def start(env: SparkEnvironment, handler: Dataset[Row] => Unit): Unit = {
    getData(env).foreachRDD(rdd => {
      val dataset = rdd2dataset(env.getSparkSession, rdd)
      handler(dataset)
    })
  }

}
