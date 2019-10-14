package io.github.interestinglab.waterdrop.spark.stream

import java.util.{List => JList}

import com.typesafe.config.waterdrop.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.common.config.CheckResult
import io.github.interestinglab.waterdrop.env.Execution
import io.github.interestinglab.waterdrop.spark.{BaseSparkSink, BaseSparkSource, BaseSparkTransform, SparkEnvironment}

import scala.collection.JavaConversions._

class SparkStreamingExecution(sparkEnvironment: SparkEnvironment) extends Execution[BaseSparkSource[_], BaseSparkTransform, BaseSparkSink[_]] {

  private var config = ConfigFactory.empty()

  override def start(sources: JList[BaseSparkSource[_]],
                     transforms: JList[BaseSparkTransform],
                     sinks: JList[BaseSparkSink[_]]): Unit = {
    val source = sources.get(0).asInstanceOf[SparkStreamingSource[_]]

    source.start(sparkEnvironment, dataset => {
      var ds = dataset
      for (tf <- transforms) {
        if (ds.take(1).length > 0) {
          ds = tf.process(ds, sparkEnvironment)
        }
      }

      source.beforeOutput

      if (ds.take(1).length > 0) {
        sinks.foreach(p => {
          p.output(ds, sparkEnvironment)
        })
      }

      source.afterOutput
    })

    val streamingContext = sparkEnvironment.getStreamingContext
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true,"")

  override def prepare(): Unit = {}
}
