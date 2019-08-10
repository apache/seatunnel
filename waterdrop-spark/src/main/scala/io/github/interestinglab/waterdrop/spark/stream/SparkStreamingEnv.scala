package io.github.interestinglab.waterdrop.spark.stream

import java.util.{List => JList}

import io.github.interestinglab.waterdrop.spark.AbstractSparkEnv
import io.github.interestinglab.waterdrop.spark.stream.sink.AbstractSparkStreamingSink
import io.github.interestinglab.waterdrop.spark.stream.source.AbstractSparkStreamingSource
import io.github.interestinglab.waterdrop.spark.stream.transform.AbstractSparkStreamingTransform
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

class SparkStreamingEnv
    extends AbstractSparkEnv[AbstractSparkStreamingSource[_],
                             AbstractSparkStreamingTransform,
                             AbstractSparkStreamingSink] {

  var streamingContext: StreamingContext = _

  override def start(sources: JList[AbstractSparkStreamingSource[_]],
                     transforms: JList[AbstractSparkStreamingTransform],
                     sinks: JList[AbstractSparkStreamingSink]): Unit = {
    val source = sources.get(0)

    source.start(
      this,
      dataset => {
        var ds = dataset
        for (tf <- transforms) {
          if (ds.take(1).length > 0) {
            ds = tf.process(ds, getSparkStreamingEnv)
          }
        }

        source.beforeOutput

        if (ds.take(1).length > 0) {
          sinks.foreach(p => {
            p.output(ds, getSparkStreamingEnv)
          })
        }

        source.afterOutput
      }
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def getSparkStreamingEnv = this

  def createStreamingContext: StreamingContext = {
    val conf = sparkSession.sparkContext.getConf
    val duration = conf.getLong("spark.stream.batchDuration", 5)
    if (streamingContext == null) {
      streamingContext =
        new StreamingContext(sparkSession.sparkContext, Seconds(duration))
    }
    streamingContext
  }

  override def prepare(): Unit = {
    super.prepare()
    streamingContext = createStreamingContext
  }
}
