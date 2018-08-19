package io.github.interestinglab.waterdrop.apis

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

abstract class BaseInput extends Plugin {

  /**
   * No matter what kind of Input it is, all you have to do is create a DStream to be used latter
   * */
  def getDStream(ssc: StreamingContext): DStream[(String, String)]

  /**
   * Things to do after filter and before output
   * */
  def beforeOutput: Unit = {}

  /**
   * Things to do after output, such as update offset
   * */
  def afterOutput: Unit = {}

}
