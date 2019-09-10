package io.github.interestinglab.waterdrop.spark.batch

import java.util.{List => JList}

import com.typesafe.config.waterdrop.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.env.Execution
import io.github.interestinglab.waterdrop.plugin.CheckResult
import io.github.interestinglab.waterdrop.spark.SparkEnvironment
import scala.collection.JavaConversions._

class SparkBatchExecution(environment: SparkEnvironment)
    extends Execution[SparkBatchSource, SparkBatchTransform, SparkBatchSink] {

  private var config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = this.config = config

  override def getConfig: Config = config

  override def checkConfig(): CheckResult = new CheckResult(true, "")

  override def prepare(): Unit = {}
  override def start(sources: JList[SparkBatchSource],
                     transforms: JList[SparkBatchTransform],
                     sinks: JList[SparkBatchSink]): Unit = {
    if (!sources.isEmpty) {
      var ds = sources.get(0).getData(environment)
      for (tf <- transforms) {

        if (ds.take(1).length > 0) {
          ds = tf.process(ds, environment)
        }
      }

//      if (ds.take(1).length > 0) {
      sinks.foreach(p => {
        p.output(ds, environment)
      })
//      }
    }
  }
}
