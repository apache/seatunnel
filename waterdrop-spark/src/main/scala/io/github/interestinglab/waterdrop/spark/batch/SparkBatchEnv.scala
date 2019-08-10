package io.github.interestinglab.waterdrop.spark.batch

import java.util.{List => JList}

import io.github.interestinglab.waterdrop.spark.AbstractSparkEnv
import io.github.interestinglab.waterdrop.spark.batch.sink.AbstractSparkBatchSink
import io.github.interestinglab.waterdrop.spark.batch.source.AbstractSparkBatchSource
import io.github.interestinglab.waterdrop.spark.batch.transform.AbstractSparkBatchTransform
import scala.collection.JavaConversions._

class SparkBatchEnv
    extends AbstractSparkEnv[AbstractSparkBatchSource,
                             AbstractSparkBatchTransform,
                             AbstractSparkBatchSink] {

  override def start(sources: JList[AbstractSparkBatchSource],
                     transforms: JList[AbstractSparkBatchTransform],
                     sinks: JList[AbstractSparkBatchSink]): Unit = {
    if (!sources.isEmpty) {
      var ds = sources.get(0).getData(getSparkBatchEnv)
      for (tf <- transforms) {
        if (ds.take(1).length > 0) {
          ds = tf.process(ds, getSparkBatchEnv)
        }
      }

      if (ds.take(1).length > 0) {
        sinks.foreach(p => {
          p.output(ds, getSparkBatchEnv)
        })
      }
    }
  }

  private def getSparkBatchEnv = this

  override def prepare(): Unit = {
    super.prepare()
  }
}
