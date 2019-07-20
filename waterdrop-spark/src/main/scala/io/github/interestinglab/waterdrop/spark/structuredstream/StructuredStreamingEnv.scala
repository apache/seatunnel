package io.github.interestinglab.waterdrop.spark.structuredstream

import java.util.{List => JList}

import io.github.interestinglab.waterdrop.spark.AbstractSparkEnv
import io.github.interestinglab.waterdrop.spark.structuredstream.sink.AbstractStructuredStreamingSink
import io.github.interestinglab.waterdrop.spark.structuredstream.source.StructuredStreamingSource
import io.github.interestinglab.waterdrop.spark.structuredstream.transform.AbstractStructuredStreamingTransform

class StructuredStreamingEnv extends AbstractSparkEnv[StructuredStreamingSource, AbstractStructuredStreamingTransform, AbstractStructuredStreamingSink]{
  override def start(sources: JList[StructuredStreamingSource],
                     transforms: JList[AbstractStructuredStreamingTransform],
                     sinks: JList[AbstractStructuredStreamingSink]): Unit = {

  }
}
