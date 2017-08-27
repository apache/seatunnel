package org.interestinglab.waterdrop.output

import org.apache.spark.streaming.StreamingContext
import com.typesafe.config.Config
import org.interestinglab.waterdrop.core.{Event, Plugin}

abstract class BaseOutput(config: Config) extends Plugin {

  def prepare(ssc: StreamingContext)

  def process(iter: Iterator[Event])
}
