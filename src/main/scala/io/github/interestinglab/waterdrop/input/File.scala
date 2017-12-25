package io.github.interestinglab.waterdrop.input

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class File(config: Config) extends BaseInput(config) {

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("path") match {
      case true => {

        val dir = config.getString("path")
        val path = new org.apache.hadoop.fs.Path(dir)
        Option(path.toUri.getScheme) match {
          case None => (true, "")
          case Some(schema) => (true, "")
          case _ =>
            (
              false,
              "unsupported schema, please set the following allowed schemas: file://, for example: file:///var/log")
        }
      }
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    val dir = config.getString("path")
    val path = new org.apache.hadoop.fs.Path(dir)
    val fullPath = Option(path.toUri.getScheme) match {
      case None => ("file://" + dir)
      case Some(schema) => dir
    }

    ssc.textFileStream(fullPath).map(s => { ("", s) })
  }
}
