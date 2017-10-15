package org.interestinglab.waterdrop.input

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class Textfile(config: Config) extends BaseInput(config) {

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("dir") match {
      case true => {
        val allowedURISchema = List("file://", "hdfs://", "s3://", "s3a://", "s3n://")
        val dir = config.getString("dir")
        val unSupportedSchema = allowedURISchema.forall(schema => {
          !dir.startsWith(schema)
        })

        unSupportedSchema match {
          case true =>
            (false, "unsupported schema, please set the following allowed schemas: " + allowedURISchema.mkString(", "))
          case false => (true, "")
        }
      }
      case false => (false, "please specify [dir] as non-empty string")
    }
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    ssc.textFileStream(config.getString("dir")).map(s => { ("", s) })
  }
}
