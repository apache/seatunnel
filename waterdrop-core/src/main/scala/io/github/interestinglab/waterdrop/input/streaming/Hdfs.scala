package io.github.interestinglab.waterdrop.input.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class Hdfs extends File {

  override def getDStream(ssc: StreamingContext): DStream[String] = {

    streamFileReader(ssc, buildPathWithDefaultSchema(config.getString("path"), "hdfs://"))
  }
}
