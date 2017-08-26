package org.interestinglab.waterdrop.input

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.Plugin

abstract class BaseInput(config: Config) extends Plugin {

  def prepare(ssc: StreamingContext)

  def getDstream(): DStream[(String, String)]

  def beforeOutput

  def afterOutput

}
