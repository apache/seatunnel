package io.github.interestinglab.waterdrop.input.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class HdfsStream extends FileStream {

  override def getDStream(ssc: StreamingContext): DStream[String] = {

    streamFileReader(ssc, buildPathWithDefaultSchema(config.getString("path"), "hdfs://"))
  }
}
