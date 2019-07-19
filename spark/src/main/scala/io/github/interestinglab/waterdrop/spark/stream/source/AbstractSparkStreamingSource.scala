package io.github.interestinglab.waterdrop.spark.stream.source


import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseSource
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.stream.SparkStreamingEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream

abstract class AbstractSparkStreamingSource[T] extends BaseSource[DStream[T], SparkStreamingEnv] {

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}

  def beforeOutput(): Unit = {}

  def afterOutput(): Unit = {}

  def rdd2dataset(sparkSession: SparkSession, rdd: RDD[T]): Dataset[Row]

  def start(env: SparkStreamingEnv, handler: Dataset[Row] => Unit): Unit = {
    getData(env).foreachRDD(rdd =>{
      val dataset = rdd2dataset(env.sparkSession,rdd)
      handler(dataset)
    })
  }

}
