package io.github.interestinglab.waterdrop.spark.batch

import java.util.{List => JList}

import io.github.interestinglab.waterdrop.spark.AbstractSparkEnv
import io.github.interestinglab.waterdrop.spark.batch.sink.AbstractSparkBatchSink
import io.github.interestinglab.waterdrop.spark.batch.source.AbstractSparkBatchSource
import io.github.interestinglab.waterdrop.spark.batch.transform.AbstractSparkBatchTransform

class SparkBatchEnv extends AbstractSparkEnv[AbstractSparkBatchSource, AbstractSparkBatchTransform, AbstractSparkBatchSink]{
  override def start(sources: JList[AbstractSparkBatchSource],
                     transforms: JList[AbstractSparkBatchTransform],
                     sinks: JList[AbstractSparkBatchSink]): Unit = {

  }
}
