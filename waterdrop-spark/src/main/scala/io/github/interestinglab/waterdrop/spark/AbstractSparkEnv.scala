package io.github.interestinglab.waterdrop.spark

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.{
  BaseSink,
  BaseSource,
  BaseTransform
}
import io.github.interestinglab.waterdrop.env.RuntimeEnv
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.config.ConfigBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

abstract class AbstractSparkEnv[SR <: BaseSource[_, _],
                                TF <: BaseTransform[_, _, _],
                                SK <: BaseSink[_, _, _]]
    extends RuntimeEnv[SR, TF, SK] {

  var sparkSession: SparkSession = _

  protected var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {
    val sparkConf = createSparkConf()
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  private def createSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    config
      .getConfig("spark")
      .entrySet()
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }
}
