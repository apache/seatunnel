package org.interestinglab.waterdrop.input

import org.apache.spark.streaming.dstream.DStream
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.Plugin

abstract class BaseInput(config: Config) extends Plugin {

  def getDStream: DStream[(String, String)]

  /**
   * Things to do after filter and before output
   * */
  def beforeOutput: Unit = {}

  /**
   * Things to do after output, such as update offset
   * */
  def afterOutput: Unit = {}

}
