package org.interestinglab.waterdrop.input

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class Textfile(config: Config) extends BaseInput(config) {

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("dir") match {
      case true => (true, "")
      case false => (false, "please specify [dir] as non-empty string")
    }
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    ssc.textFileStream(config.getString("dir")).map(s => { ("", s) })
  }
}
